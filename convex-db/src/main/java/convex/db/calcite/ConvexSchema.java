package convex.db.calcite;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import convex.db.calcite.pgcatalog.PgCatalogSchema;
import convex.db.lattice.SQLDatabase;
import convex.db.lattice.SQLSchema;
import convex.db.lattice.VersionedSQLTable;

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

	private final SQLDatabase database;
	private final SQLSchema tables;
	private final String name;

	/**
	 * Registry of SQL index names to (tableName, columnName) pairs.
	 * Populated by CREATE INDEX DDL; used to resolve DROP INDEX by name.
	 */
	final ConcurrentHashMap<String, String[]> indexRegistry = new ConcurrentHashMap<>();

	/**
	 * Creates a new ConvexSchema backed by the given database.
	 *
	 * @param database The SQLDatabase instance
	 * @param name Schema name
	 */
	public ConvexSchema(SQLDatabase database, String name) {
		this.database = database;
		this.tables = database.tables();
		this.name = name;
	}

	/**
	 * Gets the underlying SQLDatabase.
	 *
	 * @return The SQLDatabase backing this schema
	 */
	public SQLDatabase getDatabase() {
		return database;
	}

	@Override
	public boolean isMutable() {
		return true; // Allow DDL modifications
	}

	@Override
	protected Map<String, Table> getTableMap() {
		Map<String, Table> tableMap = new HashMap<>();
		String[] tableNames = tables.getTableNames();
		for (String tableName : tableNames) {
			tableMap.put(tableName, new ConvexTable(this, tableName));
			// A "<table>_HISTORY" virtual table is only meaningful (and only
			// registered) for versioned tables -- a plain table has no
			// history to show. See ConvexHistoryTable's own class doc.
			if (tables.getTable(tableName) instanceof VersionedSQLTable) {
				tableMap.put(tableName + ConvexHistoryTable.SUFFIX, new ConvexHistoryTable(this, tableName));
			}
		}
		// FILES is schema-wide (backed by the node's single shared BlobCAS
		// store, not any one table) -- registered once, unconditionally, not
		// per base table. See ConvexFilesTable's own class doc.
		tableMap.put("FILES", new ConvexFilesTable());
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
		return tables;
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
		return tables.createTable(tableName, columnNames);
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
		return tables.dropTable(tableName);
	}

	/**
	 * Checks if a table exists in the schema.
	 *
	 * @param tableName Table name
	 * @return true if table exists
	 */
	public boolean tableExists(String tableName) {
		return tables.tableExists(tableName);
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

	/**
	 * Creates a secondary index via DDL.
	 *
	 * <p>Table/column existence is checked explicitly rather than relying on
	 * {@link convex.db.lattice.SQLSchema#createIndex}'s return value — that
	 * method returns {@code false} both when the table doesn't exist in
	 * *this* schema (e.g. the client's connection default schema isn't the
	 * one actually holding the table — an easy mistake, since CREATE INDEX
	 * here has no qualified-name support, see {@code ConvexMeta}) and,
	 * ambiguously, would otherwise be indistinguishable from a genuine no-op.
	 * Silently reporting success for the former case previously left a
	 * client believing an index existed when nothing had actually happened.
	 *
	 * @param indexName  SQL index name (for later DROP INDEX lookup)
	 * @param tableName  Table to index
	 * @param columnName Column to index
	 * @param ifNotExists if true, silently succeed if already exists
	 * @return true if index was created
	 */
	public boolean createIndex(String indexName, String tableName, String columnName,
			boolean ifNotExists) {
		if (!tables.tableExists(tableName)) {
			throw new IllegalStateException(
					"Table \"" + tableName + "\" does not exist in schema \"" + name + "\"");
		}
		String[] columnNames = tables.getColumnNames(tableName);
		boolean columnFound = false;
		for (String c : columnNames) {
			if (c.equalsIgnoreCase(columnName)) {
				columnFound = true;
				break;
			}
		}
		if (!columnFound) {
			throw new IllegalStateException(
					"Column \"" + columnName + "\" does not exist on table \"" + tableName + "\"");
		}

		if (tables.hasIndex(tableName, columnName)) {
			if (ifNotExists) return false;
			throw new IllegalStateException(
					"Index on " + tableName + "(" + columnName + ") already exists");
		}
		boolean created = tables.createIndex(tableName, columnName);
		if (created) {
			indexRegistry.put(indexName.toUpperCase(), new String[]{tableName, columnName});
		}
		return created;
	}

	/**
	 * Drops a secondary index by its SQL name.
	 *
	 * @param indexName SQL index name
	 * @param ifExists  if true, silently succeed if not found
	 * @return true if index was dropped
	 */
	public boolean dropIndex(String indexName, boolean ifExists) {
		String[] pair = indexRegistry.remove(indexName.toUpperCase());
		if (pair == null) {
			if (ifExists) return false;
			throw new IllegalStateException("Index not found: " + indexName);
		}
		return tables.dropIndex(pair[0], pair[1]);
	}
}
