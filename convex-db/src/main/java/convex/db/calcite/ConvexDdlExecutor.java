package convex.db.calcite;

import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.server.DdlExecutorImpl;
import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateSchema;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDropObject;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.jdbc.ContextSqlValidator;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import convex.db.ConvexDB;
import convex.db.lattice.SQLDatabase;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.util.Static.RESOURCE;

/**
 * DDL executor that creates Convex lattice-backed tables instead of
 * Calcite's default in-memory MutableArrayTable.
 *
 * <p>Supports CREATE SCHEMA, CREATE TABLE and DROP TABLE via SQL. Schemas map
 * 1:1 to named Convex databases (see {@link ConvexDB#database(String)}); tables
 * and schemas alike are persisted to the lattice cursor tree and participate
 * in lattice replication.
 */
public class ConvexDdlExecutor extends DdlExecutorImpl {

	private static final Logger LOG = LoggerFactory.getLogger(ConvexDdlExecutor.class);

	public static final ConvexDdlExecutor INSTANCE = new ConvexDdlExecutor();

	/**
	 * Fired after a CREATE TABLE / CREATE SCHEMA / DROP TABLE statement has
	 * successfully executed against a given {@link ConvexDB} — e.g. so a
	 * caller can refresh its own metadata catalog (ot/otcol/otindex) and
	 * re-announce the updated lattice state to peers, since neither happens
	 * automatically. There's exactly one callback slot (not a list) since
	 * this executor is a process-wide singleton with a single embedded
	 * caller in practice; set to null to clear it.
	 */
	public static volatile Consumer<ConvexDB> onDdlExecuted;

	/**
	 * Invokes {@link #onDdlExecuted} if set, swallowing (and logging) any
	 * exception it throws — the DDL statement itself already succeeded by
	 * the time this runs, so a failing callback must not roll it back or
	 * break the client's connection. Public so other DDL entry points outside
	 * this class (e.g. {@code ConvexMeta}'s regex-intercepted CREATE
	 * INDEX/DROP INDEX, which never goes through this executor at all) can
	 * fire the same hook.
	 */
	public static void fireDdlExecuted(ConvexDB cdb) {
		Consumer<ConvexDB> callback = onDdlExecuted;
		if (callback == null || cdb == null) return;
		try {
			callback.accept(cdb);
		} catch (Exception e) {
			LOG.warn("onDdlExecuted callback failed", e);
		}
	}

	/**
	 * Handler for the {@code REPLICATE DB "name"} statement (regex-intercepted
	 * in {@code ConvexMeta}, same as CREATE/DROP INDEX, since Calcite's parser
	 * has no grammar for it) — given just the db name, since a real handler
	 * (registered by e.g. dbase's DbaseServer) already has everything else
	 * (which node it is, its NodeServer, its own meta schema) closed over from
	 * its own startup. Unlike {@link #onDdlExecuted}, a REPLICATE DB statement
	 * IS the operation (there's no prior "already succeeded" step) — so, on
	 * purpose, {@link #fireReplicateDb} does NOT swallow exceptions the way
	 * {@link #fireDdlExecuted} does; they propagate back to the client as a
	 * real query error instead.
	 */
	public static volatile Consumer<String> onReplicateDb;

	/**
	 * Invokes {@link #onReplicateDb}, letting any exception it throws
	 * propagate to the caller (unlike {@link #fireDdlExecuted}) — a failed
	 * replication attempt must be visible to the client as a query error, not
	 * silently swallowed.
	 *
	 * @throws IllegalStateException if no handler is registered
	 */
	public static void fireReplicateDb(String dbName) {
		Consumer<String> callback = onReplicateDb;
		if (callback == null) {
			throw new IllegalStateException("REPLICATE DB is not supported in this environment (no handler registered)");
		}
		callback.accept(dbName);
	}

	/**
	 * A request from another node asking this node to push its own future
	 * writes back to the requester, made via the {@code REGISTER PEER}
	 * statement (regex-intercepted in {@code ConvexMeta}, same pattern as
	 * {@code REPLICATE DB}).
	 *
	 * @param host requester's own reachable hostname
	 * @param port requester's own native lattice/peer wire-protocol port (not its SQL/PG port)
	 * @param accountKeyHex requester's own AccountKey, hex-encoded
	 */
	public record PeerRegistration(String host, int port, String accountKeyHex) {}

	/**
	 * Handler for the {@code REGISTER PEER '<host>' <port> '<keyHex>'}
	 * statement. Exists to close the "ongoing gossip is one-directional" gap:
	 * a node that pulls from a peer registers that peer as an outbound target
	 * on its own propagator (so its own future writes push out), but nothing
	 * previously told the SOURCE peer to do the same back — so the source's
	 * own subsequent writes never reached the puller without an explicit
	 * re-pull. A node now calls REGISTER PEER against whatever it just synced
	 * from, right after a successful pull, so the source starts pushing back
	 * too. Same non-swallowing behaviour as {@link #onReplicateDb} — this IS
	 * the operation, not a post-hoc refresh.
	 */
	public static volatile Consumer<PeerRegistration> onRegisterPeer;

	/**
	 * Invokes {@link #onRegisterPeer}, letting any exception it throws
	 * propagate to the caller.
	 *
	 * @throws IllegalStateException if no handler is registered
	 */
	public static void fireRegisterPeer(String host, int port, String accountKeyHex) {
		Consumer<PeerRegistration> callback = onRegisterPeer;
		if (callback == null) {
			throw new IllegalStateException("REGISTER PEER is not supported in this environment (no handler registered)");
		}
		callback.accept(new PeerRegistration(host, port, accountKeyHex));
	}

	/**
	 * A request to replicate a single named schema within a db, made via the
	 * {@code REPLICATE SCHEMA "<db>.<schema>"} statement (regex-intercepted
	 * in {@code ConvexMeta}, same pattern as {@code REPLICATE DB}).
	 *
	 * @param dbName db the schema belongs to
	 * @param schemaName the specific schema to replicate
	 */
	public record SchemaReplication(String dbName, String schemaName) {}

	/**
	 * Handler for the {@code REPLICATE SCHEMA "<db>.<schema>"} statement —
	 * the schema-granularity counterpart to {@link #onReplicateDb}, for
	 * replicating just one schema within a db rather than every schema that
	 * db happens to hold. Same non-swallowing behaviour as
	 * {@link #onReplicateDb} — this IS the operation, not a post-hoc
	 * refresh, so a failure must be visible to the client as a query error.
	 */
	public static volatile Consumer<SchemaReplication> onReplicateSchema;

	/**
	 * Invokes {@link #onReplicateSchema}, letting any exception it throws
	 * propagate to the caller.
	 *
	 * @throws IllegalStateException if no handler is registered
	 */
	public static void fireReplicateSchema(String dbName, String schemaName) {
		Consumer<SchemaReplication> callback = onReplicateSchema;
		if (callback == null) {
			throw new IllegalStateException("REPLICATE SCHEMA is not supported in this environment (no handler registered)");
		}
		callback.accept(new SchemaReplication(dbName, schemaName));
	}

	public static final SqlParserImplFactory PARSER_FACTORY =
		new SqlParserImplFactory() {
			@Override public SqlAbstractParserImpl getParser(Reader stream) {
				return SqlDdlParserImpl.FACTORY.getParser(stream);
			}

			@Override public DdlExecutor getDdlExecutor() {
				return ConvexDdlExecutor.INSTANCE;
			}
		};

	protected ConvexDdlExecutor() {}

	/**
	 * Executes CREATE SCHEMA by creating (and registering) a new named Convex
	 * database, sharing the same underlying ConvexDB/lattice as the connection
	 * this statement runs on — not a separate, disconnected instance. Mounted
	 * into the current session's root schema immediately so it's usable by
	 * subsequent statements on the same connection without reconnecting.
	 *
	 * <p>Requires that at least one of the root schema's current children is
	 * itself backed by a registered ConvexDB (true for any normal
	 * jdbc:convex:database=X or PgServer connection) — CREATE SCHEMA has no
	 * other way to discover which lattice/ConvexDB instance to create the new
	 * database in.
	 */
	public void execute(SqlCreateSchema create, CalcitePrepare.Context context) {
		String name = create.name.getSimple();
		CalciteSchema rootSchema = context.getMutableRootSchema();

		// Calcite's default unquoted-identifier casing upper-cases "name" (e.g.
		// "test" -> "TEST"), but the PG wire protocol's -d <name> / legacy
		// jdbc:convex:database=<name> selector is a raw string with no case
		// folding of its own — and real Postgres clients expect lower-case
		// folding for unquoted identifiers. Register/store/mount the database
		// under the lower-cased name so it's reachable the way a client
		// actually typed it, both across a fresh reconnect AND for DML on
		// this same connection (DML codegen resolves a table's schema by
		// looking the mounted ConvexSchema's own name back up in the
		// registry, so that name must match the registered one exactly).
		// In-session qualified DDL/DML referencing the Calcite-parsed
		// (upper-cased) name still resolves fine against this lower-case
		// mount via schema()'s case-insensitive lookup below.
		String registeredName = name.toLowerCase();

		if (rootSchema.getSubSchema(registeredName, false) != null) {
			if (create.ifNotExists) return;
			if (!create.getReplace()) {
				throw SqlUtil.newContextException(create.name.getParserPosition(),
						RESOURCE.schemaExists(name));
			}
		}

		ConvexDB cdb = findRegisteredConvexDB(rootSchema);
		if (cdb == null) {
			throw new IllegalStateException(
					"CREATE SCHEMA requires an existing connection to a registered Convex database "
					+ "(none of this connection's current schemas are backed by one)");
		}

		SQLDatabase newDb = cdb.database(registeredName);
		cdb.register(registeredName);
		// See ConvexDriver.connect's own comment on disabling schema caching.
		rootSchema.add(registeredName, new ConvexSchema(newDb, registeredName)).setCache(false);
		fireDdlExecuted(cdb);
	}

	/**
	 * Finds the ConvexDB backing whichever of the root schema's current
	 * children is Convex-backed and registered, so a newly created sibling
	 * database ends up in the same lattice instead of an unrelated one.
	 */
	private static ConvexDB findRegisteredConvexDB(CalciteSchema rootSchema) {
		for (String subName : rootSchema.getSubSchemaMap().keySet()) {
			ConvexDB cdb = ConvexDB.lookup(subName);
			if (cdb != null) return cdb;
		}
		return null;
	}

	/**
	 * Executes CREATE TABLE by creating a Convex lattice-backed table.
	 */
	public void execute(SqlCreateTable create, CalcitePrepare.Context context) {
		final Pair<CalciteSchema, String> pair = schema(context, create.name);
		CalciteSchema schema = requireNonNull(pair.left, "schema");
		String tableName = pair.right;

		// Check if table already exists
		if (schema.plus().tables().get(tableName) != null) {
			if (create.ifNotExists) return;
			if (!create.getReplace()) {
				throw SqlUtil.newContextException(create.name.getParserPosition(),
						RESOURCE.tableExists(tableName));
			}
		}

		// Extract column names and types from the DDL
		if (create.columnList == null) {
			throw SqlUtil.newContextException(create.name.getParserPosition(),
					RESOURCE.createTableRequiresColumnList());
		}

		List<String> columnNames = new ArrayList<>();
		List<ConvexColumnType> columnTypes = new ArrayList<>();
		SqlValidator validator = new ContextSqlValidator(context, true);

		for (SqlNode node : create.columnList) {
			if (node instanceof SqlColumnDeclaration col) {
				columnNames.add(col.name.getSimple());
				RelDataType relType = col.dataType.deriveType(validator, true);
				columnTypes.add(ConvexColumnType.fromRelDataType(relType));
			} else if (node instanceof SqlIdentifier id) {
				columnNames.add(id.getSimple());
				columnTypes.add(ConvexColumnType.of(ConvexType.ANY));
			}
		}

		// Find the ConvexSchema and create the table in the lattice
		Schema unwrapped = schema.plus().unwrap(ConvexSchema.class);
		if (unwrapped instanceof ConvexSchema convexSchema) {
			convexSchema.getTables().createTable(tableName,
					columnNames.toArray(new String[0]),
					columnTypes.toArray(new ConvexColumnType[0]));
			// Add to Calcite's schema so it's immediately visible
			schema.plus().add(tableName, new ConvexTable(convexSchema, tableName));
			fireDdlExecuted(ConvexDB.lookup(convexSchema.getName()));
		} else {
			throw new IllegalStateException(
					"CREATE TABLE requires a ConvexSchema, got: " + schema.plus().getClass());
		}
	}

	/**
	 * Executes DROP TABLE / DROP VIEW / DROP SCHEMA etc.
	 */
	public void execute(SqlDropObject drop, CalcitePrepare.Context context) {
		final Pair<CalciteSchema, String> pair = schema(context, drop.name);
		CalciteSchema schema = pair.left;
		String name = pair.right;

		if (schema == null) {
			if (drop.ifExists) return;
			throw SqlUtil.newContextException(drop.name.getParserPosition(),
					RESOURCE.objectNotFoundWithin(name, "schema"));
		}

		switch (drop.getKind()) {
		case DROP_TABLE:
			// Drop from lattice
			Schema unwrapped = schema.plus().unwrap(ConvexSchema.class);
			if (unwrapped instanceof ConvexSchema convexSchema) {
				// "name" is Calcite's parsed, upper-cased SQL identifier, but
				// Convex's native table storage is case-preserving (whatever
				// name the table was actually created with, e.g. lower-case
				// via the native API) and dropTable() does an exact,
				// case-sensitive lookup. Resolve the real stored name
				// case-insensitively first, same as table/column resolution
				// already does elsewhere — otherwise dropping a table created
				// via the native API (any of dbase-meta's tables, e.g.) fails
				// with "not found" even though it clearly exists.
				CalciteSchema.TableEntry entry = schema.getTable(name, false);
				String realName = (entry != null) ? entry.name : name;
				if (!convexSchema.dropTable(realName)) {
					if (!drop.ifExists) {
						throw SqlUtil.newContextException(drop.name.getParserPosition(),
								RESOURCE.objectNotFound(name));
					}
				}
				fireDdlExecuted(ConvexDB.lookup(convexSchema.getName()));
			}
			// Remove from Calcite
			schema.removeTable(name);
			break;
		default:
			throw new UnsupportedOperationException("DROP " + drop.getKind() + " not supported");
		}
	}

	private static Pair<CalciteSchema, String> schema(
			CalcitePrepare.Context context, SqlIdentifier id) {
		final String name;
		final List<String> path;
		if (id.isSimple()) {
			path = context.getDefaultSchemaPath();
			name = id.getSimple();
		} else {
			path = Util.skipLast(id.names);
			name = Util.last(id.names);
		}
		CalciteSchema schema = context.getMutableRootSchema();
		for (String p : path) {
			// Case-insensitive: schema/database names are mounted at connect
			// time using the raw string a client passed (e.g. "-d test",
			// never SQL-parsed), while a qualified reference like "test.t" in
			// SQL text goes through Calcite's own unquoted-identifier casing
			// (upper-cased by default) before it ever reaches this method —
			// so a case-sensitive match here would silently fail to find a
			// schema the connection is otherwise perfectly able to reach
			// (including a database qualifying itself, e.g. from a connection
			// whose own default schema is "test", writing "test.t" instead of
			// just "t"). Matches the caseSensitive=false already set on every
			// Convex JDBC connection (see ConvexDriver.connect()) — this was
			// simply not honoured here.
			CalciteSchema sub = schema.getSubSchema(p, false);
			if (sub == null) return Pair.of(null, name);
			schema = sub;
		}
		return Pair.of(schema, name);
	}
}
