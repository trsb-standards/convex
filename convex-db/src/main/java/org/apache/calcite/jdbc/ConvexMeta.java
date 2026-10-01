package org.apache.calcite.jdbc;

import java.util.Collections;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.CalciteServerStatement;

import convex.db.ConvexDB;
import convex.db.calcite.BatchSyncDeferral;
import convex.db.calcite.ConvexDdlExecutor;
import convex.db.calcite.ConvexSchema;
import convex.db.calcite.QueryLog;
import convex.db.lattice.SQLDatabase;
import convex.node.NodeServer;

/**
 * Calcite Meta implementation for Convex SQL databases.
 *
 * <p>Adds JDBC transaction support by overriding CalciteMetaImpl (which throws
 * UnsupportedOperationException for commit/rollback). Transaction isolation
 * uses Convex's lattice cursor fork/sync model:
 * <ul>
 *   <li><b>BEGIN</b> (setAutoCommit(false)): forks the SQLDatabase cursor</li>
 *   <li><b>COMMIT</b>: syncs the fork back to the parent</li>
 *   <li><b>ROLLBACK</b>: discards the fork</li>
 * </ul>
 *
 * <p>During a transaction, the connection's ConvexSchema is swapped to one
 * backed by the forked cursor, so SELECT queries read from the fork. Other
 * connections continue to read from the original cursor.
 *
 * <p>Must live in {@code org.apache.calcite.jdbc} to access the package-private
 * CalciteConnectionImpl.
 */
public class ConvexMeta extends CalciteMetaImpl {

	private final CalciteConnectionImpl calciteConnection;

	/** The forked database for the current transaction, or null if not in a transaction. */
	private SQLDatabase txDatabase;

	/** The original ConvexSchema to restore on commit/rollback. */
	private ConvexSchema originalSchema;

	/** JDBC-level autoCommit state. Initialised to true (JDBC default).
	 *  Transitions detected from connProps (not result) since Calcite's
	 *  internal autoCommit default differs from the JDBC standard. */
	private boolean autoCommit = true;

	/** Reentrancy guard: getSchema() triggers connectionSync() internally. */
	private boolean inSync = false;

	/**
	 * Per-connection override for how many peer acknowledgements every
	 * subsequent autocommit statement's sync should wait for, set via
	 * {@code SET WRITE_ACKS = <n>}. {@code -1} (the default) means
	 * unconfigured: {@link #syncIfAutoCommit} leaves NodeServer's own
	 * per-call ack target unset, so publication uses its ordinary fully
	 * synchronous behavior, unchanged from before this feature existed.
	 */
	private int writeAcks = -1;

	/** Fixed wait bound for a configured {@link #writeAcks} target. Not yet
	 *  independently configurable -- see SET_WRITE_ACKS's own doc. */
	private static final long WRITE_ACKS_TIMEOUT_MS = 5000;

	protected ConvexMeta(CalciteConnectionImpl connection) {
		super(connection,
			CalciteMetaTableFactoryImpl.INSTANCE,
			CalciteMetaColumnFactoryImpl.INSTANCE);
		this.calciteConnection = connection;
	}

	/** Factory method called by {@link convex.db.jdbc.ConvexDriver#createMeta}. */
	public static ConvexMeta create(AvaticaConnection connection) {
		return new ConvexMeta((CalciteConnectionImpl) connection);
	}

	/**
	 * Resolves the original SQL text for an already-prepared statement --
	 * needed by {@link #execute(StatementHandle, java.util.List, int)} (the
	 * bound-{@code PreparedStatement} path), which receives neither the SQL
	 * text as a parameter nor a populated {@code h.signature} (checked live
	 * 2026-08-07: null at that call site for a local, non-remote
	 * connection). Uses the exact same lookup {@code CalciteMetaImpl}'s own
	 * {@code execute}/{@code fetch} use internally
	 * ({@code calciteConnection.server.getStatement(h).getSignature()}) --
	 * this is live, authoritative state the framework already tracks, not
	 * a duplicate of it (a first attempt tried caching sql in a local map
	 * populated from a {@code prepare} override, but {@code prepare} turned
	 * out to never be called at all for this driver's local
	 * {@code PreparedStatement} path -- checked live, confirmed empty).
	 */
	private String signatureSql(StatementHandle h) {
		try {
			CalciteServerStatement stmt = calciteConnection.server.getStatement(h);
			Signature signature = stmt.getSignature();
			return (signature != null) ? signature.sql : null;
		} catch (Exception e) {
			return null;
		}
	}

	// ── Secondary index DDL interception ─────────────────────────────────────

	// Table name accepts an optional "schema." qualifier (e.g. "meta.otcol")
	// — without it, a qualified reference just failed to match at all and
	// fell through to Calcite's real parser, which has no CREATE/DROP INDEX
	// grammar, producing a confusing parse error instead of doing the right
	// thing. Unqualified still falls back to the connection's own default
	// schema, same as before.
	private static final Pattern CREATE_INDEX = Pattern.compile(
		"(?i)CREATE\\s+INDEX\\s+(IF\\s+NOT\\s+EXISTS\\s+)?(\\w+)\\s+ON\\s+(?:(\\w+)\\.)?(\\w+)\\s*\\(\\s*(\\w+)[^)]*\\)\\s*");

	private static final Pattern DROP_INDEX = Pattern.compile(
		"(?i)DROP\\s+INDEX\\s+(IF\\s+EXISTS\\s+)?(\\w+)(?:\\s+ON\\s+(?:(\\w+)\\.)?(\\w+))?\\s*");

	// REPLICATE DB "name" (or unquoted) — same regex-interception trick as
	// CREATE/DROP INDEX, since Calcite has no native grammar for this either.
	// Must be issued on the connection of the node you want to become a
	// replica; see ConvexDdlExecutor.onReplicateDb for why this can't reach
	// out and command some OTHER named node instead.
	private static final Pattern REPLICATE_DB = Pattern.compile(
		"(?i)REPLICATE\\s+DB\\s+['\"]?(\\w+)['\"]?\\s*");

	// REGISTER PEER 'host' port 'keyHex' — asks THIS connection's node to
	// push its own future writes back to the requester; see
	// ConvexDdlExecutor.onRegisterPeer for why this exists (closing the
	// one-directional-gossip gap: syncing FROM a peer never used to tell
	// that peer to sync back).
	private static final Pattern REGISTER_PEER = Pattern.compile(
		"(?i)REGISTER\\s+PEER\\s+['\"]?([\\w.\\-]+)['\"]?\\s+(\\d+)\\s+['\"]?([0-9a-fA-F]+)['\"]?\\s*");

	// REPLICATE SCHEMA "<db>.<schema>" (or unquoted) — the schema-granularity
	// counterpart to REPLICATE DB: replicates one named schema within a db,
	// not every schema that db holds. See ConvexDdlExecutor.onReplicateSchema
	// for why this only has any effect when REPLICATE DB was never issued
	// for the containing db (a whole-db link already covers every schema in it).
	private static final Pattern REPLICATE_SCHEMA = Pattern.compile(
		"(?i)REPLICATE\\s+SCHEMA\\s+['\"]?(\\w+)\\.(\\w+)['\"]?\\s*");

	// CREATE TABLE ... (...) VERSIONED -- unlike the fully-intercepted
	// statements above, CREATE TABLE genuinely needs Calcite's real
	// column/type parsing (hand-rolling that would be a real capability
	// regression), and SqlCreateTable has no properties/WITH-clause bag to
	// piggyback a flag on. So this only strips the trailing VERSIONED
	// keyword (captured group excludes it) and hands the rest, unchanged,
	// to Calcite's real parser via the normal super.prepareAndExecute
	// fallthrough below -- see ConvexDdlExecutor.PENDING_VERSIONED for how
	// the flag actually crosses to the CREATE TABLE handler.
	private static final Pattern CREATE_TABLE_VERSIONED_SUFFIX = Pattern.compile(
		"(?i)^(.*\\))\\s*VERSIONED\\s*(;\\s*)?$", Pattern.DOTALL);

	// ALTER TABLE t VERSIONED -- converts an existing plain table to
	// versioned in place. Unlike CREATE TABLE, there's no real underlying
	// Calcite ALTER TABLE semantics being reused here (this isn't a column/
	// constraint change), and the CREATE-TABLE-style "strip the keyword,
	// let Calcite parse the rest" trick doesn't work: stripping VERSIONED
	// from "ALTER TABLE t VERSIONED" would leave "ALTER TABLE t", which
	// isn't valid SQL on its own. So this is a fully intercepted statement,
	// same pattern as REPLICATE DB/REGISTER PEER above.
	private static final Pattern ALTER_TABLE_VERSIONED = Pattern.compile(
		"(?i)ALTER\\s+TABLE\\s+(?:(\\w+)\\.)?(\\w+)\\s+VERSIONED\\s*");

	// CREATE TABLE ... (...) AUTOINCREMENT / ALTER TABLE t AUTOINCREMENT --
	// same regex-interception shapes as their VERSIONED counterparts above,
	// for the exact same reasons (real column/type parsing needed for
	// CREATE TABLE, no properties bag to piggyback a flag on; ALTER TABLE
	// has no real underlying Calcite semantics being reused, and stripping
	// the keyword would leave invalid SQL). A statement combining both
	// VERSIONED and AUTOINCREMENT is out of scope for now — these are
	// checked independently, not together.
	private static final Pattern CREATE_TABLE_AUTOINCREMENT_SUFFIX = Pattern.compile(
		"(?i)^(.*\\))\\s*AUTOINCREMENT\\s*(;\\s*)?$", Pattern.DOTALL);

	private static final Pattern ALTER_TABLE_AUTOINCREMENT = Pattern.compile(
		"(?i)ALTER\\s+TABLE\\s+(?:(\\w+)\\.)?(\\w+)\\s+AUTOINCREMENT\\s*");

	// SET WRITE_ACKS = <n> -- per-connection durability/latency knob for
	// every subsequent autocommit statement on this connection. See
	// syncIfAutoCommit's own doc for what this actually changes; the short
	// version: by default, every autocommit write's response waits for a
	// full synchronous broadcast attempt to every connected peer (no acks
	// tracked). Setting this to 0 makes autocommit writes return as soon as
	// they're durably persisted on THIS node, firing the broadcast without
	// waiting on it at all. Setting it to N>0 waits for at least N peers to
	// respond (capped at however many are actually connected) before
	// returning, trading some of that latency back for a stronger, but
	// still bounded (WRITE_ACKS_TIMEOUT_MS), delivery guarantee.
	private static final Pattern SET_WRITE_ACKS = Pattern.compile(
		"(?i)SET\\s+WRITE_ACKS\\s*=\\s*(\\d+)\\s*");

	// TRANSFER <DB|SCHEMA|OT> <target> TO <entityid> -- the ownership-
	// transfer verb (see ConvexDdlExecutor.onTransfer's own doc for the full
	// design). Same regex-interception shape as REPLICATE DB, and the same
	// "quotes optional around a bare word/id token" convention -- a target
	// name with characters outside \w (spaces, dots) needs the numeric id
	// form instead, same limitation REPLICATE DB/SCHEMA already have.
	private static final Pattern TRANSFER_DB = Pattern.compile(
		"(?i)TRANSFER\\s+DB\\s+['\"]?(\\w+)['\"]?\\s+TO\\s+(\\d+)\\s*");
	private static final Pattern TRANSFER_SCHEMA = Pattern.compile(
		"(?i)TRANSFER\\s+SCHEMA\\s+['\"]?(\\w+)['\"]?\\s+TO\\s+(\\d+)\\s*");
	private static final Pattern TRANSFER_OT = Pattern.compile(
		"(?i)TRANSFER\\s+OT\\s+['\"]?(\\w+)['\"]?\\s+TO\\s+(\\d+)\\s*");
	// TRANSFER OTROWS FROM <ot> WHERE <predicate> TO <entityid> -- the
	// predicate itself is opaque here (captured raw, handed to the handler,
	// which runs it as a real SELECT ... WHERE <predicate>), so this pattern
	// only needs to find where the predicate ends: the LAST " TO <digits>"
	// in the statement (reluctant match up to that, DOTALL so a predicate
	// spanning a literal newline still works).
	private static final Pattern TRANSFER_OTROWS = Pattern.compile(
		"(?i)TRANSFER\\s+OTROWS\\s+FROM\\s+['\"]?(\\w+)['\"]?\\s+WHERE\\s+(.+)\\s+TO\\s+(\\d+)\\s*", Pattern.DOTALL);

	/**
	 * {@inheritDoc}
	 *
	 * <p>The whole method body is timed and reported to {@link QueryLog},
	 * not just the {@code super.prepareAndExecute(...)} fallthrough path --
	 * found live 2026-08-07: an earlier attempt at this hooked {@code
	 * PgProtocolHandler} instead, which (a) only sees pgwire clients, since
	 * {@code PgServer}'s own connection supplier is itself an ordinary
	 * {@code jdbc:convex:} caller and every OTHER direct Java caller of this
	 * driver was invisible to it, and (b) missed every regex-intercepted
	 * admin statement below ({@code CREATE INDEX}/{@code DROP INDEX}/{@code
	 * REPLICATE DB}/{@code REGISTER PEER}/{@code REPLICATE SCHEMA}), since
	 * those return before ever reaching the Calcite fallthrough. Hooking
	 * here instead covers every caller and every statement shape in one
	 * place, with no double-counting.
	 */
	@Override
	public ExecuteResult prepareAndExecute(StatementHandle h, String sql,
			long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback)
			throws NoSuchStatementException {
		long startNanos = System.nanoTime();
		Long rowCount = null;
		String errorMessage = null;
		try {
			// Checked FIRST, before any of convex-db's own regex-intercepted
			// statements below -- lets an embedder (dbase) recognize and fully
			// handle its own SQL extensions in its own code. See
			// ConvexDdlExecutor.rawSqlInterceptor's own doc.
			Function<ConvexDdlExecutor.RawSqlRequest, ConvexDdlExecutor.RawSqlAction> interceptor
				= ConvexDdlExecutor.rawSqlInterceptor;
			if (interceptor != null) {
				ConvexDdlExecutor.RawSqlAction action = interceptor.apply(
					new ConvexDdlExecutor.RawSqlRequest(h, sql, getSchemaName()));
				if (action instanceof ConvexDdlExecutor.RawSqlAction.Done done) {
					rowCount = rowCountOf(done.result());
					return done.result();
				}
				if (action instanceof ConvexDdlExecutor.RawSqlAction.Rewrite rewrite) {
					sql = rewrite.sql();
				}
			}

			Matcher m = CREATE_INDEX.matcher(sql.trim());
			if (m.matches()) {
				boolean ifNotExists = m.group(1) != null;
				String indexName  = m.group(2);
				String schemaName = (m.group(3) != null) ? m.group(3) : getSchemaName();
				String tableName  = m.group(4);
				String columnName = m.group(5);
				ConvexSchema schema = findConvexSchema(schemaName);
				if (schema == null) {
					throw new IllegalStateException("Schema \"" + schemaName + "\" not found");
				}
				schema.createIndex(indexName, tableName, columnName, ifNotExists);
				fireDdlExecuted(schema);
				rowCount = 0L;
				return new ExecuteResult(Collections.singletonList(
						MetaResultSet.count(h.connectionId, h.id, 0L)));
			}

			m = DROP_INDEX.matcher(sql.trim());
			if (m.matches()) {
				boolean ifExists = m.group(1) != null;
				String indexName = m.group(2);
				String schemaName = (m.group(3) != null) ? m.group(3) : getSchemaName();
				ConvexSchema schema = findConvexSchema(schemaName);
				if (schema == null) {
					throw new IllegalStateException("Schema \"" + schemaName + "\" not found");
				}
				schema.dropIndex(indexName, ifExists);
				fireDdlExecuted(schema);
				rowCount = 0L;
				return new ExecuteResult(Collections.singletonList(
						MetaResultSet.count(h.connectionId, h.id, 0L)));
			}

			m = REPLICATE_DB.matcher(sql.trim());
			if (m.matches()) {
				String dbName = m.group(1);
				ConvexDdlExecutor.fireReplicateDb(dbName);
				rowCount = 0L;
				return new ExecuteResult(Collections.singletonList(
						MetaResultSet.count(h.connectionId, h.id, 0L)));
			}

			m = REGISTER_PEER.matcher(sql.trim());
			if (m.matches()) {
				String host = m.group(1);
				int port = Integer.parseInt(m.group(2));
				String keyHex = m.group(3);
				ConvexDdlExecutor.fireRegisterPeer(host, port, keyHex);
				rowCount = 0L;
				return new ExecuteResult(Collections.singletonList(
						MetaResultSet.count(h.connectionId, h.id, 0L)));
			}

			m = REPLICATE_SCHEMA.matcher(sql.trim());
			if (m.matches()) {
				String dbName = m.group(1);
				String schemaName = m.group(2);
				ConvexDdlExecutor.fireReplicateSchema(dbName, schemaName);
				rowCount = 0L;
				return new ExecuteResult(Collections.singletonList(
						MetaResultSet.count(h.connectionId, h.id, 0L)));
			}

			m = ALTER_TABLE_VERSIONED.matcher(sql.trim());
			if (m.matches()) {
				String schemaName = (m.group(1) != null) ? m.group(1) : getSchemaName();
				ConvexSchema schema = findConvexSchema(schemaName);
				if (schema == null) {
					throw new IllegalStateException("Schema \"" + schemaName + "\" not found");
				}
				// Resolved case-insensitively against what's actually stored
				// -- found live 2026-09-30: unlike CREATE TABLE (which goes
				// through Calcite's real parser and gets its identifiers
				// normalized), this whole statement is regex-intercepted, so
				// the captured table name is exactly as the caller typed it.
				// A table created via ordinary "CREATE TABLE t (...)" is
				// stored uppercase ("T") by Calcite's own normalization, but
				// one created via a direct Java API call (e.g. dbase-meta's
				// own catalog tables) is stored exactly as given, often
				// lowercase -- blindly uppercasing the requested name (the
				// previous fix here) only covers the first case, and fails
				// outright ("Table 'OT' not found") for the second.
				String tableName = resolveActualTableName(schema, m.group(2));
				// convertToVersioned returns false (no-op) if the table
				// doesn't exist in this schema -- found live 2026-08-09: a
				// caller connected to the wrong schema (e.g. "meta" instead
				// of "ose") got a silent "OK" for a statement that did
				// nothing, since this return value used to be discarded.
				if (!schema.getTables().convertToVersioned(tableName)) {
					throw new IllegalStateException(
						"Table \"" + tableName + "\" not found in schema \"" + schemaName + "\"");
				}
				fireDdlExecuted(schema);
				rowCount = 0L;
				return new ExecuteResult(Collections.singletonList(
						MetaResultSet.count(h.connectionId, h.id, 0L)));
			}

			m = ALTER_TABLE_AUTOINCREMENT.matcher(sql.trim());
			if (m.matches()) {
				String schemaName = (m.group(1) != null) ? m.group(1) : getSchemaName();
				ConvexSchema schema = findConvexSchema(schemaName);
				if (schema == null) {
					throw new IllegalStateException("Schema \"" + schemaName + "\" not found");
				}
				// Same case-insensitive resolution as ALTER_TABLE_VERSIONED above.
				String tableName = resolveActualTableName(schema, m.group(2));
				if (!schema.getTables().convertToAutoIncrement(tableName)) {
					throw new IllegalStateException(
						"Table \"" + tableName + "\" not found in schema \"" + schemaName + "\"");
				}
				fireDdlExecuted(schema);
				rowCount = 0L;
				return new ExecuteResult(Collections.singletonList(
						MetaResultSet.count(h.connectionId, h.id, 0L)));
			}

			m = SET_WRITE_ACKS.matcher(sql.trim());
			if (m.matches()) {
				writeAcks = Integer.parseInt(m.group(1));
				rowCount = 0L;
				return new ExecuteResult(Collections.singletonList(
						MetaResultSet.count(h.connectionId, h.id, 0L)));
			}

			m = TRANSFER_DB.matcher(sql.trim());
			if (m.matches()) {
				ConvexDdlExecutor.fireTransfer(new ConvexDdlExecutor.TransferRequest(
					ConvexDdlExecutor.TransferKind.DB, m.group(1), null,
					Integer.parseInt(m.group(2)), getSchemaName()));
				rowCount = 0L;
				return new ExecuteResult(Collections.singletonList(
						MetaResultSet.count(h.connectionId, h.id, 0L)));
			}

			m = TRANSFER_SCHEMA.matcher(sql.trim());
			if (m.matches()) {
				ConvexDdlExecutor.fireTransfer(new ConvexDdlExecutor.TransferRequest(
					ConvexDdlExecutor.TransferKind.SCHEMA, m.group(1), null,
					Integer.parseInt(m.group(2)), getSchemaName()));
				rowCount = 0L;
				return new ExecuteResult(Collections.singletonList(
						MetaResultSet.count(h.connectionId, h.id, 0L)));
			}

			// TRANSFER_OT must be checked before it could ever wrongly match
			// text meant for TRANSFER_OTROWS -- it can't: "OTROWS" right
			// after "OT" fails TRANSFER_OT's own \s+ requirement, so the two
			// patterns never collide regardless of check order.
			m = TRANSFER_OT.matcher(sql.trim());
			if (m.matches()) {
				ConvexDdlExecutor.fireTransfer(new ConvexDdlExecutor.TransferRequest(
					ConvexDdlExecutor.TransferKind.OT, m.group(1), null,
					Integer.parseInt(m.group(2)), getSchemaName()));
				rowCount = 0L;
				return new ExecuteResult(Collections.singletonList(
						MetaResultSet.count(h.connectionId, h.id, 0L)));
			}

			m = TRANSFER_OTROWS.matcher(sql.trim());
			if (m.matches()) {
				ConvexDdlExecutor.fireTransfer(new ConvexDdlExecutor.TransferRequest(
					ConvexDdlExecutor.TransferKind.OTROWS, m.group(1), m.group(2),
					Integer.parseInt(m.group(3)), getSchemaName()));
				rowCount = 0L;
				return new ExecuteResult(Collections.singletonList(
						MetaResultSet.count(h.connectionId, h.id, 0L)));
			}

			String effectiveSql = sql;
			String trimmedUpper = sql.trim().toUpperCase();
			if (trimmedUpper.startsWith("CREATE TABLE") || trimmedUpper.startsWith("CREATE OR REPLACE TABLE")) {
				Matcher vm = CREATE_TABLE_VERSIONED_SUFFIX.matcher(sql.trim());
				if (vm.matches()) {
					effectiveSql = vm.group(1);
					ConvexDdlExecutor.PENDING_VERSIONED.set(true);
				} else {
					Matcher am = CREATE_TABLE_AUTOINCREMENT_SUFFIX.matcher(sql.trim());
					if (am.matches()) {
						effectiveSql = am.group(1);
						ConvexDdlExecutor.PENDING_AUTOINCREMENT.set(true);
					}
				}
			}

			ExecuteResult result = super.prepareAndExecute(h, effectiveSql, maxRowCount, maxRowsInFirstFrame, callback);
			syncIfAutoCommit(trimmedUpper.startsWith("SELECT"), h.connectionId);
			rowCount = rowCountOf(result);
			return result;
		} catch (NoSuchStatementException e) {
			errorMessage = e.getMessage();
			throw e;
		} catch (RuntimeException e) {
			errorMessage = e.getMessage();
			throw e;
		} finally {
			QueryLog.fire(sql, elapsedMs(startNanos), rowCount, errorMessage, h.connectionId);
		}
	}

	/**
	 * {@inheritDoc}
	 *
	 * <p>Overridden for the same reason as {@link #prepareAndExecute} above --
	 * a bound {@code PreparedStatement} (unlike a fresh simple-query string)
	 * goes through this entry point instead. The original SQL text isn't a
	 * parameter here, and {@code h.signature} is null at this call site for
	 * a local connection (checked live 2026-08-07) -- {@link #signatureSql}
	 * resolves it the same way {@code CalciteMetaImpl}'s own {@code execute}
	 * does internally.
	 */
	@Override
	public ExecuteResult execute(StatementHandle h, java.util.List<org.apache.calcite.avatica.remote.TypedValue> parameterValues,
			int maxRowsInFirstFrame) throws NoSuchStatementException {
		long startNanos = System.nanoTime();
		Long rowCount = null;
		String errorMessage = null;
		String sql = signatureSql(h);
		try {
			// Same seam as prepareAndExecute's own, but only the Done action
			// makes sense here -- this statement was already prepared against
			// its original text, so there's no fresh-parse step left to
			// rewrite into. A Rewrite action is silently ignored (falls
			// through to the ordinary super.execute below) rather than
			// treated as an error, since an embedder's handler may
			// legitimately not distinguish the two call sites.
			if (sql != null) {
				Function<ConvexDdlExecutor.RawSqlRequest, ConvexDdlExecutor.RawSqlAction> interceptor
					= ConvexDdlExecutor.rawSqlInterceptor;
				if (interceptor != null) {
					ConvexDdlExecutor.RawSqlAction action = interceptor.apply(
						new ConvexDdlExecutor.RawSqlRequest(h, sql, getSchemaName()));
					if (action instanceof ConvexDdlExecutor.RawSqlAction.Done done) {
						rowCount = rowCountOf(done.result());
						return done.result();
					}
				}
			}

			ExecuteResult result = super.execute(h, parameterValues, maxRowsInFirstFrame);
			syncIfAutoCommit(sql != null && sql.trim().toUpperCase().startsWith("SELECT"), h.connectionId);
			rowCount = rowCountOf(result);
			return result;
		} catch (NoSuchStatementException e) {
			errorMessage = e.getMessage();
			throw e;
		} catch (RuntimeException e) {
			errorMessage = e.getMessage();
			throw e;
		} finally {
			if (sql != null) {
				QueryLog.fire(sql, elapsedMs(startNanos), rowCount, errorMessage, h.connectionId);
			}
		}
	}

	private static long elapsedMs(long startNanos) {
		return (System.nanoTime() - startNanos) / 1_000_000;
	}

	/**
	 * Row count from an ExecuteResult, when actually available: exact for a
	 * real {@code updateCount} (regex-intercepted admin statements set this
	 * explicitly to 0 at their own call sites above; an ordinary INSERT/
	 * UPDATE/DELETE would use this too, if this execution path ever
	 * populated it -- see below).
	 *
	 * <p>Checked live 2026-08-07 (logged actual {@code MetaResultSet} shapes
	 * for a real SELECT/INSERT/bound-PreparedStatement against this
	 * implementation): {@code updateCount} was {@code -1} and {@code
	 * firstFrame} was either {@code null} or an empty, not-yet-{@code done}
	 * frame in every ordinary-SQL case observed. Row data for this
	 * implementation is fetched lazily, via separate {@code Meta.fetch(...)}
	 * calls issued by {@code AvaticaResultSet} *after* {@code
	 * prepareAndExecute}/{@code execute} return -- it genuinely does not
	 * exist yet at the point this hook fires, not just "isn't exposed
	 * conveniently." A prior attempt at counting an available-but-not-yet-
	 * {@code done} first frame was removed: it would have silently reported
	 * "0 rows" for ordinary SELECTs rather than the honest "unknown," which
	 * is worse. {@code Querylog.ROWCOUNT} is null for ordinary SELECT/
	 * INSERT/UPDATE/DELETE through this path as a result -- duration, type,
	 * and error message remain reliable regardless.
	 */
	private static Long rowCountOf(ExecuteResult result) {
		if (result == null || result.resultSets == null || result.resultSets.isEmpty()) return null;
		MetaResultSet rs = result.resultSets.get(0);
		return (rs.updateCount >= 0) ? rs.updateCount : null;
	}

	/**
	 * Syncs the current schema's database cursor after every autocommit
	 * statement -- publishing it to {@code LatticePropagator} for ongoing
	 * gossip to peers.
	 *
	 * <p>Found live 2026-08-06 diagnosing "dev"/"meta" dbase nodes never
	 * catching up on each other's writes without a restart: {@link #commit}
	 * below only ever synced {@link #txDatabase}, which is only non-null
	 * during an explicit manual transaction ({@code setAutoCommit(false)} ...
	 * {@code COMMIT}). Autocommit -- the JDBC default, and what {@code psql}'s
	 * simple-query protocol and virtually every ordinary client actually
	 * use -- forked nothing and therefore synced nothing: every INSERT/UPDATE/
	 * DELETE wrote directly into the live, unforked database (so local reads
	 * on the SAME node saw it immediately) but never told the node's own
	 * propagator anything had changed, so it never got announced, persisted
	 * for restart-durability, or broadcast to peers at all -- not "broadcast
	 * and failed", genuinely never attempted. A restart happened to "fix" this
	 * because a fresh boot's {@code pullPath} reads the target's current state
	 * directly, a completely different, always-synchronous mechanism.
	 *
	 * <p>Called unconditionally after every statement (including SELECTs) for
	 * simplicity and safety -- {@code sync()} on an unchanged cursor is cheap
	 * (novelty-tracking finds nothing new to announce or broadcast). Skipped
	 * entirely while {@link #autoCommit} is false: an in-progress manual
	 * transaction's writes belong to {@link #txDatabase} and must not become
	 * visible to peers (or even to this node's own non-transactional readers)
	 * until {@link #commit} explicitly syncs it.
	 *
	 * <p>If {@link #writeAcks} has been configured (via {@code SET
	 * WRITE_ACKS = <n>}), sets NodeServer's per-thread ack target
	 * (NodeServer.setNextSyncAckTarget) before calling {@code db.sync()},
	 * so the publish this sync triggers bounds how many peer
	 * acknowledgements it waits for instead of always doing a full
	 * synchronous broadcast to every connected peer. See {@link
	 * convex.node.LatticePropagator#publishWithAckTarget}'s own doc for
	 * exact semantics -- the short version: {@code writeAcks == 0} returns
	 * as soon as the write is durably persisted on this node (broadcast
	 * still fires, just not waited on); {@code writeAcks > 0} waits for
	 * that many peer responses (capped at however many are connected) or
	 * {@link #WRITE_ACKS_TIMEOUT_MS}, whichever comes first, but never
	 * fails the statement on a timeout. Left unconfigured (the default,
	 * {@code writeAcks == -1}), this method touches nothing extra and
	 * publication behaves exactly as it did before this feature existed.
	 *
	 * <p><b>Found live 2026-08-25</b>: the ack-target logic used to apply
	 * unconditionally, the same as the {@code sync()} call itself -- but
	 * unlike a no-op {@code sync()} (genuinely cheap on an unchanged
	 * cursor, per this method's own original doc above), a configured
	 * ack-target is NOT cheap for a no-op: {@code LatticePropagator.
	 * publishWithAckTarget} decides whether to wait purely from {@code
	 * minAcks > 0} and {@code snapshot.hasPeers()} -- neither checks
	 * whether the value actually changed -- so a plain SELECT on a
	 * connection with {@code WRITE_ACKS} configured still built a real
	 * ack-tracked broadcast and blocked on a genuine peer round-trip,
	 * exactly as if it had written something. Measured live: SELECT
	 * latency on an {@code acks=1} connection jumped from ~400µs to
	 * ~7ms -- the same order of magnitude as an actual acked write,
	 * for a statement that never touched any data. {@code isQuery} scopes
	 * the ack-target to genuine writes only; {@code sync()} itself still
	 * runs unconditionally either way (still cheap for a no-op, and worth
	 * keeping for the reasons in this method's own original doc).
	 *
	 * @param isQuery true if the statement that just ran was a read (e.g.
	 *        a {@code SELECT}) -- skips configuring an ack target
	 *        regardless of {@link #writeAcks}, since a read never has
	 *        anything for a peer to acknowledge.
	 * @param connectionId this statement's Avatica connection id (see
	 *        {@link BatchSyncDeferral}) -- null-safe, since some call
	 *        shapes (e.g. a bare local test not going through a real
	 *        {@code StatementHandle}) may not have one.
	 */
	private void syncIfAutoCommit(boolean isQuery, String connectionId) {
		if (!autoCommit) return;
		// See BatchSyncDeferral's own doc: PgProtocolHandler.executeWithParameters
		// opens a deferred-sync window around every bound-PreparedStatement
		// Execute in a pg-wire extended-protocol pipeline (including every
		// row of a JDBC executeBatch()) -- while one is open for this
		// connection, just record whether a write happened and skip the
		// real sync; PgProtocolHandler#handleSync (the wire protocol's own
		// pipeline-flush point) triggers exactly one real sync afterward via
		// flushDeferredSync, instead of one per row.
		if (BatchSyncDeferral.isDeferring(connectionId)) {
			if (!isQuery) BatchSyncDeferral.markDirty(connectionId);
			return;
		}
		doSync(isQuery);
	}

	/**
	 * Flushes a deferred batch sync for {@code connectionId} if one is owed
	 * -- called from {@code PgProtocolHandler#handleSync} via {@code
	 * ConvexMetaAccess}. A no-op if nothing was dirty (a read-only pipeline,
	 * or no deferred window was ever open for this connection).
	 */
	public void flushDeferredSync(String connectionId) {
		if (BatchSyncDeferral.end(connectionId)) {
			doSync(false);
		}
	}

	/** The actual sync-performing logic, shared by the immediate and deferred-flush paths. */
	private void doSync(boolean isQuery) {
		boolean ackTargetSet = !isQuery && writeAcks >= 0;
		if (ackTargetSet) {
			NodeServer.setNextSyncAckTarget(writeAcks, WRITE_ACKS_TIMEOUT_MS);
		}
		try {
			SQLDatabase db = findDatabase();
			if (db != null) db.sync();
		} catch (Exception e) {
			// Best-effort: the statement itself already succeeded and its
			// result is already on its way back to the client: a
			// publish/broadcast failure here must not turn into a client-
			// visible statement failure. Matches the tolerance shown
			// elsewhere in this class and in DbaseServer's own peer-sync
			// helpers for the same class of best-effort operation.
		} finally {
			// publishApplicationRoot already consumes+clears this the
			// instant it reads it, but db.sync() can throw before ever
			// reaching that point (e.g. findDatabase() itself failing) --
			// clear defensively so a never-consumed target can't leak into
			// a later, unrelated sync() on this same connection's thread.
			if (ackTargetSet) {
				NodeServer.clearNextSyncAckTarget();
			}
		}
	}

	/** Syncs fork to parent, then starts a new fork if still in manual-commit mode. */
	@Override
	public void commit(ConnectionHandle ch) {
		if (txDatabase != null) {
			txDatabase.sync();
			endTransaction();
		}
		if (!autoCommit) {
			beginTransaction();
		}
	}

	/** Discards fork, then starts a new fork if still in manual-commit mode. */
	@Override
	public void rollback(ConnectionHandle ch) {
		endTransaction();
		if (!autoCommit) {
			beginTransaction();
		}
	}

	@Override
	public ConnectionProperties connectionSync(ConnectionHandle ch, ConnectionProperties connProps) {
		if (inSync) return super.connectionSync(ch, connProps);
		inSync = true;
		try {
			return doConnectionSync(ch, connProps);
		} finally {
			inSync = false;
		}
	}

	/** Detects autoCommit transitions from the requested connProps. */
	private ConnectionProperties doConnectionSync(ConnectionHandle ch, ConnectionProperties connProps) {
		ConnectionProperties result = super.connectionSync(ch, connProps);

		try {
			// Read from connProps (the request), not result (Calcite internal state)
			boolean newAutoCommit = connProps.isAutoCommit();
			if (newAutoCommit != autoCommit) {
				if (!newAutoCommit) {
					beginTransaction();
					if (txDatabase != null) {
						autoCommit = false;
					}
				} else {
					if (txDatabase != null) {
						txDatabase.sync();
						endTransaction();
					}
					autoCommit = true;
				}
			}
		} catch (Exception e) {
			// connProps.isAutoCommit() throws if not explicitly set (e.g. during init)
		}

		return result;
	}

	/** Forks the SQLDatabase and swaps the connection's schema to the fork. */
	private void beginTransaction() {
		SQLDatabase db = findDatabase();
		if (db == null) return;

		String schemaName = getSchemaName();
		if (schemaName == null) return;

		ConvexSchema currentSchema = findConvexSchema(schemaName);
		if (currentSchema == null) return;

		originalSchema = currentSchema;
		txDatabase = db.fork();

		ConvexSchema txSchema = new ConvexSchema(txDatabase, schemaName);
		// See ConvexDriver.connect's own comment on setCacheEnabled(false) --
		// same reasoning applies to every schema mount, not just the initial
		// connection-time ones.
		calciteConnection.getRootSchema().add(schemaName, txSchema).setCacheEnabled(false);
	}

	/** Restores the original schema and discards the fork. */
	private void endTransaction() {
		if (txDatabase == null) return;

		String schemaName = getSchemaName();
		if (schemaName != null && originalSchema != null) {
			calciteConnection.getRootSchema().add(schemaName, originalSchema).setCacheEnabled(false);
		}
		txDatabase = null;
		originalSchema = null;
	}

	/**
	 * Fires {@link ConvexDdlExecutor#onDdlExecuted} for a CREATE INDEX/DROP
	 * INDEX statement — these never go through {@code ConvexDdlExecutor} at
	 * all (intercepted earlier, via regex, since Calcite's DDL parser has no
	 * native CREATE INDEX support here), so without this a caller relying on
	 * that hook (e.g. to refresh ot/otindex and re-announce to peers) would
	 * never see an index change take effect until a restart.
	 */
	private static void fireDdlExecuted(ConvexSchema schema) {
		ConvexDdlExecutor.fireDdlExecuted(ConvexDB.lookup(schema.getName()));
	}

	private String getSchemaName() {
		try {
			return calciteConnection.getSchema();
		} catch (Exception e) {
			return null;
		}
	}

	private SQLDatabase findDatabase() {
		String schemaName = getSchemaName();
		if (schemaName == null) return null;
		ConvexSchema schema = findConvexSchema(schemaName);
		return (schema != null) ? schema.getDatabase() : null;
	}

	private ConvexSchema findConvexSchema(String schemaName) {
		try {
			SchemaPlus sub = calciteConnection.getRootSchema().getSubSchema(schemaName);
			if (sub != null) {
				return sub.unwrap(ConvexSchema.class);
			}
		} catch (Exception e) {
			// ignore
		}
		return null;
	}

	/**
	 * Resolves a regex-captured (never Calcite-normalized) table name
	 * against what's actually stored, case-insensitively — a table created
	 * via ordinary SQL DDL is stored uppercase, but one created via a direct
	 * Java API call (e.g. dbase-meta's own catalog tables) is stored exactly
	 * as given. Falls back to the requested name verbatim if nothing
	 * matches, so the caller's own "table not found" error still names what
	 * was actually typed.
	 */
	private static String resolveActualTableName(ConvexSchema schema, String requested) {
		for (String actual : schema.getTables().getTableNames()) {
			if (actual.equalsIgnoreCase(requested)) return actual;
		}
		return requested.toUpperCase();
	}
}
