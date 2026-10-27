package convex.db.calcite;

import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.server.DdlExecutorImpl;
import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
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

import convex.core.data.Strings;
import convex.db.ConvexDB;
import convex.db.lattice.SQLDatabase;
import convex.db.lattice.SQLSchema;

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
	 * Carries the {@code VERSIONED} flag from {@code ConvexMeta.prepareAndExecute}
	 * (which strips a trailing {@code VERSIONED} keyword off a {@code CREATE
	 * TABLE} statement via regex before handing the rest to Calcite's real
	 * parser — there's no grammar for it, and no properties/{@code WITH}-clause
	 * bag on {@code SqlCreateTable} to piggyback on) through to {@link
	 * #execute(SqlCreateTable, CalcitePrepare.Context)} below. A 3rd method
	 * parameter isn't possible: Calcite's {@code ReflectUtil.MethodDispatcher}
	 * reflectively dispatches on the exact 2-arg signature. {@code ThreadLocal}
	 * rather than a plain field since this executor is a process-wide
	 * singleton — set immediately before the {@code super.prepareAndExecute}
	 * call that will (synchronously, same thread) reach {@code execute} below,
	 * and always cleared there whether or not it was actually a CREATE TABLE
	 * (so a stale true never leaks into some later, unrelated statement on the
	 * same thread).
	 */
	public static final ThreadLocal<Boolean> PENDING_VERSIONED = new ThreadLocal<>();

	/**
	 * Carries the {@code AUTOINCREMENT} key mode from {@code ConvexMeta.prepareAndExecute}
	 * to {@link #execute(SqlCreateTable, CalcitePrepare.Context)} below —
	 * exact same mechanism/reasoning as {@link #PENDING_VERSIONED}, kept as
	 * a separate ThreadLocal (not reused) since the two are orthogonal
	 * flags a statement can set independently. Null: keys are not generated.
	 */
	public static final ThreadLocal<convex.db.lattice.AutoIncrementMode> PENDING_AUTOINCREMENT = new ThreadLocal<>();

	/**
	 * Carries the {@code CREATEDT} table option (a creation-time column, see
	 * {@link SQLSchema#CREATEDT_COLUMN}) the same way as {@link #PENDING_VERSIONED}.
	 */
	public static final ThreadLocal<Boolean> PENDING_CREATEDT = new ThreadLocal<>();

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
	 * One argument bundle passed to {@link #rawSqlInterceptor} — everything
	 * an embedder needs to recognize and fully handle its own SQL extensions
	 * without needing access to any Calcite/Avatica connection internals.
	 * {@code defaultSchemaName} is {@code ConvexMeta}'s own {@code
	 * getSchemaName()} (the connection's current default schema) — genuine
	 * per-connection state an embedder can't otherwise recover, since
	 * {@link convex.db.ConvexDB#lookup} is a plain, connection-independent
	 * process-wide registry lookup by name.
	 */
	public record RawSqlRequest(StatementHandle statementHandle, String sql, String defaultSchemaName) {}

	/**
	 * What a {@link #rawSqlInterceptor} decided to do with one {@link
	 * RawSqlRequest}. An embedder that recognizes the statement returns
	 * exactly one of these; returning {@code null} from the interceptor
	 * itself (not from this type) means "not mine" — see the field's own
	 * doc.
	 */
	public sealed interface RawSqlAction {
		/**
		 * Hand this SQL (typically a rewritten, literal query — e.g. a
		 * generated {@code SELECT '...' AS "..."} — never the original
		 * extension syntax, which Calcite's real parser can't read) to
		 * convex-db's own engine instead of the original statement text.
		 * Still passes through the existing {@code VERSIONED}/{@code
		 * AUTOINCREMENT} suffix checks and {@code super.prepareAndExecute}
		 * fallthrough unchanged.
		 */
		record Rewrite(String sql) implements RawSqlAction {}

		/** Fully handled already -- use this result as-is, run nothing else. */
		record Done(ExecuteResult result) implements RawSqlAction {}
	}

	/**
	 * A generic pre-interception hook, checked FIRST in {@link
	 * org.apache.calcite.jdbc.ConvexMeta#prepareAndExecute}/{@code execute}
	 * -- before any of convex-db's own regex-intercepted statements below.
	 * Lets an embedder (e.g. dbase) recognize and fully handle its own SQL
	 * extensions (its own {@code REPLICATE}/{@code TRANSFER}/{@code
	 * GRANT}/{@code SHARE}/{@code SHOW CREATE TABLE}, ...) using its own
	 * vocabulary and catalog classes entirely in its own code, with
	 * convex-db knowing nothing about their names or semantics -- see
	 * dbase's own {@code PLAN_convexdb_dbase_split.md} for the reasoning.
	 * Convex-db itself never registers anything here; this is purely a seam
	 * for an embedder to use. Returning {@code null} means "not mine" --
	 * the request falls through to convex-db's own handling completely
	 * unchanged. There's exactly one callback slot (not a list), matching
	 * {@link #onDdlExecuted}'s own single-embedded-caller-in-practice
	 * reasoning; an embedder wanting several independent statement handlers
	 * composes them into one {@code Function} itself before assigning it
	 * here.
	 */
	public static volatile Function<RawSqlRequest, RawSqlAction> rawSqlInterceptor;

	// REPLICATE DB/REPLICATE SCHEMA/REGISTER PEER/TRANSFER used to each have
	// their own dedicated hook (onReplicateDb/onReplicateSchema/
	// onRegisterPeer/onTransfer) here, mirroring onDdlExecuted's shape.
	// Migrated onto rawSqlInterceptor above instead (see dbase.sqlext's
	// ReplicateDbHandler/ReplicateSchemaHandler/RegisterPeerHandler/
	// TransferHandler, and PLAN_convexdb_dbase_split.md) -- they're entirely
	// an embedder's own management-layer vocabulary (dbase's db/schema/
	// replication/ownership catalog), not something convex-db itself has
	// any notion of, so there's no reason for convex-db to carry their
	// request-record types or dispatch methods at all.

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
		// A connection into an owner tree creates schemas in its own db, whatever else it can see.
		convex.db.SchemaView view = convex.db.SchemaView.current();
		if (view != null) return view.takeCreateSchemaTarget();
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
		boolean versioned = Boolean.TRUE.equals(PENDING_VERSIONED.get());
		PENDING_VERSIONED.remove();
		convex.db.lattice.AutoIncrementMode autoIncrement = PENDING_AUTOINCREMENT.get();
		PENDING_AUTOINCREMENT.remove();
		boolean createdDt = Boolean.TRUE.equals(PENDING_CREATEDT.get());
		PENDING_CREATEDT.remove();

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
		List<String> primaryKey = null;
		SqlValidator validator = new ContextSqlValidator(context, true);

		for (SqlNode node : create.columnList) {
			String declaredName;
			if (node.getKind() == SqlKind.PRIMARY_KEY && node instanceof SqlCall key) {
				// A table-level "PRIMARY KEY (a, b)" constraint: operands are
				// [constraint name, column list] -- see SqlKeyConstraint.
				primaryKey = new ArrayList<>();
				for (SqlNode keyColumn : (SqlNodeList) key.getOperandList().get(1)) {
					primaryKey.add(((SqlIdentifier) keyColumn).getSimple());
				}
				continue;
			}
			if (node instanceof SqlColumnDeclaration col) {
				declaredName = col.name.getSimple();
				RelDataType relType = col.dataType.deriveType(validator, true);
				columnTypes.add(ConvexColumnType.fromRelDataType(relType));
			} else if (node instanceof SqlIdentifier id) {
				declaredName = id.getSimple();
				columnTypes.add(ConvexColumnType.of(ConvexType.ANY));
			} else {
				continue;
			}
			// OWNERID is a reserved, auto-appended ownership column (see
			// SQLSchema#OWNERID_COLUMN / PLAN_access_rights.md) -- a client
			// declaring its own would either collide with or be silently
			// shadowed by the real one, so reject outright rather than let
			// either happen quietly.
			if (SQLSchema.OWNERID_COLUMN.equalsIgnoreCase(declaredName)) {
				throw new IllegalArgumentException(
					"Column name '" + declaredName + "' is reserved (auto-appended ownership column) "
					+ "and cannot be declared explicitly");
			}
			if (SQLSchema.CREATEDT_COLUMN.equalsIgnoreCase(declaredName)) {
				throw new IllegalArgumentException(
					"Column name '" + declaredName + "' is reserved (creation time, added with the CREATEDT "
					+ "table option) and cannot be declared explicitly");
			}
			columnNames.add(declaredName);
		}

		int pkCount = primaryKeyCount(tableName, columnNames, primaryKey);

		// Find the ConvexSchema and create the table in the lattice
		Schema unwrapped = schema.plus().unwrap(ConvexSchema.class);
		if (unwrapped instanceof ConvexSchema convexSchema) {
			convexSchema.getTables().createTable(Strings.create(tableName),
					columnNames.toArray(new String[0]),
					columnTypes.toArray(new ConvexColumnType[0]),
					pkCount, versioned, autoIncrement);
			if (createdDt) convexSchema.getTables().addCreatedDt(Strings.create(tableName));
			// Add to Calcite's schema so it's immediately visible
			schema.plus().add(tableName, new ConvexTable(convexSchema, tableName));
			fireDdlExecuted(ConvexDB.lookup(convexSchema.getName()));
		} else {
			throw new IllegalStateException(
					"CREATE TABLE requires a ConvexSchema, got: " + schema.plus().getClass());
		}
	}

	/**
	 * How many leading columns form the primary key. A table is keyed on its
	 * first {@code pkCount} columns (see {@link SQLSchema#createTable}), so a
	 * declared {@code PRIMARY KEY (...)} must name exactly the leading
	 * columns, in declaration order -- anything else is rejected rather than
	 * silently keyed on something the statement didn't ask for. Without a
	 * PRIMARY KEY clause the first column is the key, as before.
	 */
	private static int primaryKeyCount(String tableName, List<String> columnNames, List<String> primaryKey) {
		if (primaryKey == null) return 1;
		boolean leading = !primaryKey.isEmpty() && primaryKey.size() <= columnNames.size();
		for (int i = 0; leading && i < primaryKey.size(); i++) {
			leading = primaryKey.get(i).equalsIgnoreCase(columnNames.get(i));
		}
		if (!leading) {
			throw new IllegalArgumentException(
				"PRIMARY KEY (" + String.join(", ", primaryKey) + ") on table '" + tableName
				+ "' must list the table's leading columns in declaration order (the first "
				+ primaryKey.size() + " declared: "
				+ String.join(", ", columnNames.subList(0, Math.min(primaryKey.size(), columnNames.size()))) + ")");
		}
		return primaryKey.size();
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
