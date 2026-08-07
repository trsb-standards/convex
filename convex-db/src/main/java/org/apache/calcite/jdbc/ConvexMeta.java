package org.apache.calcite.jdbc;

import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.schema.SchemaPlus;

import convex.db.ConvexDB;
import convex.db.calcite.ConvexDdlExecutor;
import convex.db.calcite.ConvexSchema;
import convex.db.lattice.SQLDatabase;

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

	@Override
	public ExecuteResult prepareAndExecute(StatementHandle h, String sql,
			long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback)
			throws NoSuchStatementException {
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
			return new ExecuteResult(Collections.singletonList(
					MetaResultSet.count(h.connectionId, h.id, 0L)));
		}

		m = REPLICATE_DB.matcher(sql.trim());
		if (m.matches()) {
			String dbName = m.group(1);
			ConvexDdlExecutor.fireReplicateDb(dbName);
			return new ExecuteResult(Collections.singletonList(
					MetaResultSet.count(h.connectionId, h.id, 0L)));
		}

		m = REGISTER_PEER.matcher(sql.trim());
		if (m.matches()) {
			String host = m.group(1);
			int port = Integer.parseInt(m.group(2));
			String keyHex = m.group(3);
			ConvexDdlExecutor.fireRegisterPeer(host, port, keyHex);
			return new ExecuteResult(Collections.singletonList(
					MetaResultSet.count(h.connectionId, h.id, 0L)));
		}

		m = REPLICATE_SCHEMA.matcher(sql.trim());
		if (m.matches()) {
			String dbName = m.group(1);
			String schemaName = m.group(2);
			ConvexDdlExecutor.fireReplicateSchema(dbName, schemaName);
			return new ExecuteResult(Collections.singletonList(
					MetaResultSet.count(h.connectionId, h.id, 0L)));
		}

		ExecuteResult result = super.prepareAndExecute(h, sql, maxRowCount, maxRowsInFirstFrame, callback);
		syncIfAutoCommit();
		return result;
	}

	/**
	 * {@inheritDoc}
	 *
	 * <p>Overridden for the same reason as {@link #prepareAndExecute} below --
	 * a bound {@code PreparedStatement} (unlike a fresh simple-query string)
	 * goes through this entry point instead.
	 */
	@Override
	public ExecuteResult execute(StatementHandle h, java.util.List<org.apache.calcite.avatica.remote.TypedValue> parameterValues,
			int maxRowsInFirstFrame) throws NoSuchStatementException {
		ExecuteResult result = super.execute(h, parameterValues, maxRowsInFirstFrame);
		syncIfAutoCommit();
		return result;
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
	 */
	private void syncIfAutoCommit() {
		if (!autoCommit) return;
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
}
