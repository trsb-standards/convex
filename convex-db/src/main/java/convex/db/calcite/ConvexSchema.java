package convex.db.calcite;

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import convex.db.calcite.pgcatalog.PgCatalogSchema;
import convex.db.lattice.SQLDatabase;
import convex.db.lattice.SQLSchema;

/**
 * Calcite Schema backed by a Convex {@link SQLDatabase}.
 *
 * <p>Provides the bridge between Calcite's SQL query engine and Convex's
 * lattice-based table storage. Tables created via DDL are persisted to
 * the underlying lattice cursor tree.
 *
 * <p>This schema is mutable - tables can be added and removed via SQL DDL
 * statements when used with Calcite's DDL executor.
 */
public class ConvexSchema extends AbstractSchema {

	/** The database a schema reads and writes now, with its tables. */
	private record Binding(SQLDatabase database, SQLSchema tables) {
		Binding(SQLDatabase database) {
			this(database, database.tables());
		}
	}

	private final SQLDatabase base;
	private volatile Binding binding;
	private final String name;

	/**
	 * Creates a new ConvexSchema backed by the given database.
	 *
	 * @param database The SQLDatabase instance
	 * @param name Schema name
	 */
	public ConvexSchema(SQLDatabase database, String name) {
		this.base = database;
		this.binding = new Binding(database);
		this.name = name;
	}

	/**
	 * Gets the SQLDatabase this schema reads and writes now: a transaction's
	 * fork while one is open on it ({@link #bindTransaction}), else the
	 * database it was made with.
	 *
	 * @return The SQLDatabase backing this schema
	 */
	public SQLDatabase getDatabase() {
		return binding.database();
	}

	/**
	 * Binds this schema to a transaction's fork of its database, or back to
	 * the database itself. A plan prepared on this schema reaches its tables
	 * when it runs, not when it is prepared, so a statement prepared before a
	 * transaction, or in an earlier one, runs in the current transaction.
	 *
	 * @param fork Fork of this schema's database, or null to end the transaction
	 */
	public void bindTransaction(SQLDatabase fork) {
		this.binding = new Binding((fork == null) ? base : fork);
	}

	/**
	 * Gets the database this schema was made with, whichever transaction it
	 * is bound to.
	 *
	 * @return The SQLDatabase this schema was created with
	 */
	public SQLDatabase getBaseDatabase() {
		return base;
	}

	@Override
	public boolean isMutable() {
		return true; // Allow DDL modifications
	}

	@Override
	protected Map<String, Table> getTableMap() {
		Map<String, Table> tableMap = new HashMap<>();
		String[] tableNames = getTables().getTableNames();
		for (String tableName : tableNames) {
			tableMap.put(tableName, new ConvexTable(this, tableName));
		}
		return tableMap;
	}

	@Override
	protected Map<String, Schema> getSubSchemaMap() {
		Map<String, Schema> subSchemas = new HashMap<>();
		subSchemas.put("pg_catalog", new PgCatalogSchema(this));
		return subSchemas;
	}

	/**
	 * Gets the underlying SQLSchema instance.
	 *
	 * @return SQLSchema backing store
	 */
	public SQLSchema getTables() {
		return binding.tables();
	}

	/**
	 * Gets the schema name.
	 *
	 * @return Schema name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Creates a table in the schema.
	 *
	 * @param tableName Table name
	 * @param columnNames Column names
	 * @return true if table was created, false if it already exists
	 */
	public boolean createTable(String tableName, String... columnNames) {
		return getTables().createTable(tableName, columnNames);
	}

	/**
	 * Drops a table from the schema.
	 *
	 * <p>Called by the DDL executor when processing DROP TABLE statements.
	 *
	 * @param tableName Table name
	 * @return true if table was dropped, false if it didn't exist
	 */
	public boolean dropTable(String tableName) {
		return getTables().dropTable(tableName);
	}

	/**
	 * Checks if a table exists in the schema.
	 *
	 * @param tableName Table name
	 * @return true if table exists
	 */
	public boolean tableExists(String tableName) {
		return getTables().tableExists(tableName);
	}

	/**
	 * Gets a ConvexTable by name.
	 *
	 * @param tableName Table name
	 * @return The ConvexTable
	 */
	public ConvexTable getConvexTable(String tableName) {
		return new ConvexTable(this, tableName);
	}
}
