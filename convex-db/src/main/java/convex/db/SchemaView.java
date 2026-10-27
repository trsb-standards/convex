package convex.db;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import convex.db.lattice.SQLDatabase;

/**
 * The schemas one connection can see, by name. In an owner tree two entities
 * can each have a schema called "app", so "which schema is app" depends on who
 * is asking: a connection gets a view holding the schemas of its own db plus
 * the public ones, and every lookup by schema name made while that connection
 * runs a statement goes through the view.
 *
 * <p>While a view is current on a thread, {@link ConvexDB#lookup},
 * {@link ConvexDB#lookupDatabase}, {@link ConvexDB#getRegisteredNames} and
 * {@link ConvexDB#register} use it in place of the process-wide registry. The
 * JDBC driver makes a connection's view current around every statement.
 */
public final class SchemaView {

	private static final ThreadLocal<SchemaView> CURRENT = new ThreadLocal<>();

	private final ConvexDB home;
	private final String defaultSchema;
	private final Map<String, SQLDatabase> schemas = new LinkedHashMap<>();

	/**
	 * @param home The connection's own db: where CREATE SCHEMA puts a new schema
	 * @param defaultSchema Schema that unqualified table names resolve in, or null
	 */
	public SchemaView(ConvexDB home, String defaultSchema) {
		this.home = home;
		this.defaultSchema = defaultSchema;
	}

	/** Adds every schema that already has state in the given db; an earlier entry of the same name wins. */
	public SchemaView addAll(ConvexDB cdb) {
		for (String name : cdb.getDatabaseNames()) add(name, cdb);
		return this;
	}

	/** Adds one schema of the given db; an earlier entry of the same name wins. */
	public synchronized SchemaView add(String name, ConvexDB cdb) {
		if (!schemas.containsKey(name)) schemas.put(name, cdb.database(name));
		return this;
	}

	public ConvexDB home() {
		return home;
	}

	private ConvexDB createSchemaTarget;

	/**
	 * Names the db the next CREATE SCHEMA on this connection goes to, for an
	 * embedder that lets a statement say which db it means ({@code CREATE
	 * SCHEMA <db>.<schema>}). Applies to one statement.
	 */
	public synchronized void setCreateSchemaTarget(ConvexDB target) {
		this.createSchemaTarget = target;
	}

	/** The db a CREATE SCHEMA running now should use: the one-off target if set, else {@link #home()}. */
	public synchronized ConvexDB takeCreateSchemaTarget() {
		ConvexDB target = (createSchemaTarget != null) ? createSchemaTarget : home;
		createSchemaTarget = null;
		return target;
	}

	public String defaultSchema() {
		return defaultSchema;
	}

	public synchronized SQLDatabase database(String name) {
		return schemas.get(name);
	}

	public synchronized Set<String> names() {
		return Set.copyOf(schemas.keySet());
	}

	/** A copy of the name-to-schema map, in the order the schemas were added. */
	public synchronized Map<String, SQLDatabase> snapshot() {
		return new LinkedHashMap<>(schemas);
	}

	// ========== Current view of the running thread ==========

	/** The view of the connection running a statement on this thread, or null. */
	public static SchemaView current() {
		return CURRENT.get();
	}

	/** Restores the previously current view when closed. */
	public interface Scope extends AutoCloseable {
		@Override
		void close();
	}

	/** Makes {@code view} current on this thread until the returned scope is closed; null leaves things as they are. */
	public static Scope enter(SchemaView view) {
		if (view == null) return () -> {};
		SchemaView previous = CURRENT.get();
		CURRENT.set(view);
		return () -> {
			if (previous == null) CURRENT.remove();
			else CURRENT.set(previous);
		};
	}
}
