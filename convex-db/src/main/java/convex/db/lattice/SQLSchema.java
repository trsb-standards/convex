package convex.db.lattice;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import convex.core.data.ABlob;
import convex.core.data.ACell;
import convex.core.data.AString;
import convex.core.data.AVector;
import convex.core.data.Blob;
import convex.core.data.Index;
import convex.core.data.Strings;
import convex.core.data.Vectors;
import convex.core.data.prim.CVMLong;
import convex.db.calcite.ConvexColumnType;
import convex.db.calcite.ConvexType;
import convex.lattice.ALatticeComponent;
import convex.lattice.cursor.ALatticeCursor;
import convex.lattice.cursor.Cursors;

/**
 * A SQL-like table store built on Convex lattice technology.
 *
 * <p>Provides basic table operations (CREATE TABLE, DROP TABLE, INSERT, SELECT, DELETE)
 * with lattice merge replication for conflict-free distributed operation.
 *
 * <p>Primary keys must be blob-like types (Blob, AString, AccountKey, etc.) as
 * required by the Index data structure for ordering.
 *
 * <p>Usage:
 * <pre>
 * SQLSchema schema = SQLSchema.create();
 *
 * // Create a table
 * schema.createTable("users", new String[]{"id", "name", "email"});
 *
 * // Insert rows (first column is primary key)
 * schema.insert("users", 1, "Alice", "alice@example.com");
 *
 * // Query rows
 * AVector&lt;ACell&gt; row = schema.selectByKey("users", CVMLong.create(1));
 *
 * // Delete rows
 * schema.deleteByKey("users", CVMLong.create(1));
 *
 * // Drop table
 * schema.dropTable("users");
 * </pre>
 */
public class SQLSchema extends ALatticeComponent<Index<AString, AVector<ACell>>> {

	/**
	 * JVM-global monotonic write-sequence counter. Initialised from
	 * {@code System.currentTimeMillis()} so that v4 sequence numbers always sort
	 * after v3 compact-timestamp rows (~1.7e12) in LWW comparisons.
	 * Being static means every write in any schema instance within this process gets
	 * a unique version — no ties are possible even when multiple forks write
	 * concurrently within the same millisecond.
	 *
	 * <p><b>Found + fixed 2026-08-07</b>: {@link #now()} used to be a plain
	 * {@code incrementAndGet()} on this field, advancing by exactly 1 per
	 * write regardless of how much real time passed between writes. That
	 * makes values incomparable across different processes/nodes once
	 * they've been running for different lengths of time with different
	 * write volumes: a low-activity node's value stays pinned near its own
	 * boot-time seed indefinitely, while a node that simply booted a few
	 * seconds *later* starts with a higher seed than the first node will
	 * accumulate through ordinary write-count increments alone — inverting
	 * the intended chronological LWW ordering. Found live diagnosing a
	 * DELETE that appeared to succeed locally but silently reverted within
	 * seconds via ordinary background gossip: the deleting node's tombstone
	 * (a low-activity node, timestamp still near its own boot seed) lost
	 * every LWW comparison against a peer's live row written at THAT peer's
	 * own, later boot-time seed — even though the peer's write chronologically
	 * predated the delete by several minutes of real wall-clock time. Not
	 * specific to any one table; every row in every table uses this same
	 * mechanism for conflict resolution. Fixed in {@link #now()} below by
	 * tracking real wall-clock time instead of a pure per-write increment.
	 */
	private static final AtomicLong WRITE_SEQ = new AtomicLong(System.currentTimeMillis());

	/**
	 * This schema's own name, used as the {@code TableVersionRegistry} lookup
	 * key for every table this instance touches — table names are unique
	 * within a schema, not across a whole node, so registry lookups need both.
	 * A bare {@link #SQLSchema(ALatticeCursor)} (no name given) gets a unique
	 * generated placeholder, safe because such an instance is never sharing a
	 * registry namespace with anything else (it's its own isolated cursor
	 * tree) — {@link SQLDatabase#tables()} passes the real schema name.
	 */
	private final AString schemaName;

	public SQLSchema(ALatticeCursor<Index<AString, AVector<ACell>>> cursor) {
		this(null, cursor, anonymousSchemaName());
	}

	public SQLSchema(ALatticeCursor<Index<AString, AVector<ACell>>> cursor, AString schemaName) {
		this(null, cursor, schemaName);
	}

	/**
	 * Full constructor, threading through both upstream's new parent-component
	 * link (unused within this class itself, but part of {@link
	 * ALatticeComponent}'s own general contract now) and this fork's own
	 * {@code schemaName} (needed for {@link TableVersionRegistry} lookups --
	 * see that field's own javadoc).
	 */
	SQLSchema(ALatticeComponent<?> parent,
			ALatticeCursor<Index<AString, AVector<ACell>>> cursor, AString schemaName) {
		super(parent, cursor);
		this.schemaName = schemaName;
	}

	static AString anonymousSchemaName() {
		return Strings.create("anon-" + java.util.UUID.randomUUID());
	}

	/** This schema's own name (see {@link #schemaName}'s javadoc for what it's used for). */
	public AString getSchemaName() {
		return schemaName;
	}

	/**
	 * Creates a new empty table store.
	 *
	 * @return New SQLSchema instance
	 */
	public static SQLSchema create() {
		AString name = anonymousSchemaName();
		ALatticeCursor<Index<AString, AVector<ACell>>> cursor =
			Cursors.createLattice(new HybridTableStoreLattice(name));
		return new SQLSchema(cursor, name);
	}

	/**
	 * Connects to an existing cursor for cursor chain integration.
	 *
	 * <p>No current caller passes a name-bearing cursor chain here — prefer
	 * {@link SQLDatabase#tables()} for a schema that needs correct per-table
	 * versioned dispatch tied to a real, stable schema name.
	 *
	 * @param cursor Lattice cursor (e.g. from a SignedCursor path)
	 * @return New SQLSchema instance connected to the cursor
	 */
	public static SQLSchema connect(ALatticeCursor<Index<AString, AVector<ACell>>> cursor) {
		return new SQLSchema(cursor, anonymousSchemaName());
	}

	/**
	 * Creates a forked copy of this store for independent operation.
	 *
	 * @return Forked SQLSchema instance
	 */
	public SQLSchema fork() {
		return new SQLSchema(parent(), cursor.fork(), schemaName);
	}

	// ========== Internal Helpers ==========

	/**
	 * Returns the next unique write-sequence number for LWW ordering.
	 *
	 * <p>Always at least {@code System.currentTimeMillis()} -- so a value
	 * genuinely reflects real elapsed time, staying comparable against
	 * writes from other processes/nodes no matter how long this process has
	 * been idle -- while still guaranteeing strict per-process monotonicity
	 * (advances by at least 1 even if the wall clock hasn't ticked forward,
	 * e.g. multiple writes within the same millisecond).
	 */
	private CVMLong now() {
		long prev;
		long next;
		do {
			prev = WRITE_SEQ.get();
			next = Math.max(prev + 1, System.currentTimeMillis());
		} while (!WRITE_SEQ.compareAndSet(prev, next));
		return CVMLong.create(next);
	}

	/**
	 * Plain {@code System.currentTimeMillis()}, deliberately not {@link #now()}'s
	 * tie-broken monotonic sequence — preserves {@code VersionedSQLSchema}'s
	 * existing behavior for versioned-table timestamps exactly, since {@link
	 * VersionedSQLTable}'s own LWW/merge comparisons were only ever exercised
	 * against this simpler form.
	 */
	private static CVMLong millis() {
		return CVMLong.create(System.currentTimeMillis());
	}

	/**
	 * Gets a cursor-backed table by name, returning null if not found —
	 * returns a {@link VersionedSQLTable} if {@code name} is marked versioned
	 * in {@link TableVersionRegistry}, a plain {@link SQLTable} otherwise, so
	 * a single schema can hold both kinds side by side.
	 */
	public SQLTable getTable(AString name) {
		ALatticeCursor<AVector<ACell>> tableCursor = cursor.path(name);
		if (tableCursor.get() == null) return null;
		if (TableVersionRegistry.isVersioned(schemaName, name)) {
			return new VersionedSQLTable(tableCursor);
		}
		return new SQLTable(this, tableCursor);
	}

	/** Convenience overload. */
	public SQLTable getTable(String name) {
		return getTable(Strings.create(name));
	}

	/**
	 * Gets a live (non-tombstone) table, returning null if not found or dropped.
	 */
	public SQLTable getLiveTable(AString name) {
		SQLTable table = getTable(name);
		if (table == null || !table.isLive()) return null;
		return table;
	}

	/** Convenience overload. */
	public SQLTable getLiveTable(String name) {
		return getLiveTable(Strings.create(name));
	}

	/**
	 * Converts an ACell to ABlob for use as primary key.
	 * Supports: ABlob (direct), CVMLong (8-byte encoding), AString (UTF-8 bytes).
	 *
	 * @param key The key to convert
	 * @return ABlob representation
	 * @throws IllegalArgumentException if key type not supported
	 */
	protected ABlob toKey(ACell key) {
		if (key instanceof ABlob blob) return blob;
		if (key instanceof CVMLong n) {
			// Encode as 8-byte big-endian
			long v = n.longValue();
			byte[] bytes = new byte[8];
			for (int i = 7; i >= 0; i--) {
				bytes[i] = (byte) (v & 0xFF);
				v >>= 8;
			}
			return Blob.wrap(bytes);
		}
		if (key instanceof AString s) {
			return Blob.wrap(s.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
		}
		throw new IllegalArgumentException("Unsupported primary key type: " + key.getClass().getSimpleName());
	}

	/**
	 * Converts the first {@code pkCount} columns of a row into a single composite
	 * key blob by concatenating their individual byte encodings.
	 *
	 * <p>Each component is encoded as: [4-byte big-endian length][raw bytes].
	 * This is unambiguous for all component types and lengths.
	 *
	 * <p>Falls back to {@link #toKey(ACell)} when pkCount == 1.
	 *
	 * @param row     Full row values (PK columns must be first)
	 * @param pkCount Number of leading columns that form the key
	 * @return Composite ABlob key
	 */
	protected ABlob toCompositeKey(AVector<ACell> row, int pkCount) {
		if (pkCount == 1) return toKey(row.get(0));
		// Collect byte encodings of each PK component
		byte[][] parts = new byte[pkCount][];
		int totalSize = 0;
		for (int i = 0; i < pkCount; i++) {
			parts[i] = toKey(row.get(i)).getBytes();
			totalSize += 4 + parts[i].length;
		}
		// Concatenate with 4-byte big-endian length prefix per component
		byte[] result = new byte[totalSize];
		int pos = 0;
		for (byte[] part : parts) {
			int len = part.length;
			result[pos++] = (byte) ((len >> 24) & 0xFF);
			result[pos++] = (byte) ((len >> 16) & 0xFF);
			result[pos++] = (byte) ((len >>  8) & 0xFF);
			result[pos++] = (byte) (len & 0xFF);
			System.arraycopy(part, 0, result, pos, len);
			pos += len;
		}
		return Blob.wrap(result);
	}

	// ========== Table Operations ==========

	/**
	 * Creates a new table with the given column names.
	 * All columns use ANY type (dynamic typing).
	 */
	public boolean createTable(String name, String[] columns) {
		return createTable(Strings.create(name), columns);
	}

	/** Creates a new table with explicitly typed columns (no precision/scale). */
	public boolean createTable(String name, String[] columns, ConvexType[] types) {
		return createTable(Strings.create(name), columns, types, 1);
	}

	/** Creates a new table with explicitly typed columns and a composite PK spanning pkCount leading columns. */
	public boolean createTable(String name, String[] columns, ConvexType[] types, int pkCount) {
		return createTable(Strings.create(name), columns, types, pkCount);
	}

	/** Creates a new table with fully typed columns (including precision/scale). */
	public boolean createTable(String name, String[] columns, ConvexColumnType[] types) {
		return createTable(Strings.create(name), columns, types, 1);
	}

	/** Creates a new table with fully typed columns and a composite PK spanning pkCount leading columns. */
	public boolean createTable(String name, String[] columns, ConvexColumnType[] types, int pkCount) {
		return createTable(Strings.create(name), columns, types, pkCount);
	}

	/** Creates a new table with ANY-typed columns, optionally versioned (row-history-tracked). */
	public boolean createTable(String name, String[] columns, boolean versioned) {
		ConvexColumnType[] types = new ConvexColumnType[columns.length];
		for (int i = 0; i < types.length; i++) {
			types[i] = ConvexColumnType.of(ConvexType.ANY);
		}
		return createTable(Strings.create(name), columns, types, 1, versioned);
	}

	public boolean createTable(AString name, String[] columns) {
		ConvexColumnType[] types = new ConvexColumnType[columns.length];
		for (int i = 0; i < types.length; i++) {
			types[i] = ConvexColumnType.of(ConvexType.ANY);
		}
		return createTable(name, columns, types);
	}

	public boolean createTable(AString name, String[] columns, ConvexType[] types) {
		return createTable(name, columns, types, 1);
	}

	public boolean createTable(AString name, String[] columns, ConvexType[] types, int pkCount) {
		ConvexColumnType[] columnTypes = new ConvexColumnType[types.length];
		for (int i = 0; i < types.length; i++) {
			columnTypes[i] = ConvexColumnType.of(types[i]);
		}
		return createTable(name, columns, columnTypes, pkCount);
	}

	public boolean createTable(AString name, String[] columns, ConvexColumnType[] types) {
		return createTable(name, columns, types, 1);
	}

	public boolean createTable(AString name, String[] columns, ConvexColumnType[] types, int pkCount) {
		return createTable(name, columns, types, pkCount, false);
	}

	/** Creates a new table, plain or versioned, with no auto-increment — see the 6-arg overload for the real terminus. */
	public boolean createTable(AString name, String[] columns, ConvexColumnType[] types, int pkCount, boolean versioned) {
		return createTable(name, columns, types, pkCount, versioned, false);
	}

	/**
	 * Creates a new table, plain/versioned/auto-increment per the flags. The
	 * real terminus — every other {@code createTable} overload funnels down
	 * to this one.
	 *
	 * <p>Versioned tables don't support composite primary keys yet — {@link
	 * VersionedSQLTable}'s history model treats the pk as an opaque single
	 * blob (see {@link HistoryKey}) — so {@code versioned=true} with {@code
	 * pkCount != 1} rejects outright rather than silently building a table
	 * that can't be queried by its individual key components.
	 *
	 * <p>Auto-increment tables are likewise restricted to a single-column PK
	 * (an auto-generating composite key has no sensible definition in any
	 * SQL database), and that column must be {@link ConvexType#INTEGER} —
	 * {@code AutoIncrementCounters} generates plain 64-bit sequential
	 * values, not arbitrary-precision or string keys. Combining {@code
	 * versioned} and {@code autoIncrement} on the same table is out of
	 * scope for now (not needed by anything using this yet) — not rejected
	 * outright, but untested; callers wanting both should treat it as
	 * unsupported.
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public boolean createTable(AString name, String[] columns, ConvexColumnType[] types, int pkCount,
			boolean versioned, boolean autoIncrement) {
		if (columns.length != types.length) {
			throw new IllegalArgumentException("Columns and types must have same length");
		}
		if (versioned && pkCount != 1) {
			throw new UnsupportedOperationException(
				"Versioned tables don't support composite primary keys (pkCount=" + pkCount + ")");
		}
		if (autoIncrement) {
			if (pkCount != 1) {
				throw new UnsupportedOperationException(
					"Auto-increment tables don't support composite primary keys (pkCount=" + pkCount + ")");
			}
			if (types[0].getBaseType() != ConvexType.INTEGER) {
				throw new UnsupportedOperationException(
					"Auto-increment primary key must be INTEGER, not " + types[0].getBaseType());
			}
		}

		// Build full desired schema: [[name, typeName, precision, scale], ...]
		AVector newSchema = Vectors.empty();
		for (int i = 0; i < columns.length; i++) {
			ConvexColumnType ct = types[i];
			AString typeName = (ct.getBaseType() == ConvexType.ANY) ? null : Strings.create(ct.getBaseType().name());
			CVMLong precision = ct.hasPrecision() ? CVMLong.create(ct.getPrecision()) : null;
			CVMLong scale = ct.hasScale() ? CVMLong.create(ct.getScale()) : null;
			newSchema = newSchema.append(Vectors.of(Strings.create(columns[i]), typeName, precision, scale));
		}

		SQLTable existing = getLiveTable(name);
		if (existing == null) {
			// Table does not exist yet: create fresh
			ALatticeCursor<AVector<ACell>> tableCursor = cursor.path(name);
			if (versioned) {
				tableCursor.set(VersionedSQLTable.createState((AVector<AVector<ACell>>) newSchema, millis()));
				TableVersionRegistry.markVersioned(schemaName, name);
			} else {
				tableCursor.set(SQLTable.createState((AVector<AVector<ACell>>) newSchema, now(), pkCount));
			}
			if (autoIncrement) {
				AutoIncrementRegistry.markAutoIncrement(schemaName, name);
			}
			return true;
		}

		// Table already exists: append any new columns not present in stored
		// schema. Matched by NAME, not by position/count — a naive "append
		// everything past the existing count" would corrupt the schema (e.g.
		// duplicate a column) if a new column is inserted in the middle of
		// the desired column list rather than strictly appended at the end.
		// Same logic regardless of versioned-ness -- POS_SCHEMA (slot 0) is
		// identical between SQLTable and VersionedSQLTable state shapes.
		AVector<AVector<ACell>> existingSchema = existing.getSchema();
		long existingCount = existingSchema != null ? existingSchema.count() : 0;

		java.util.Set<String> existingNames = new java.util.HashSet<>();
		for (long i = 0; i < existingCount; i++) {
			existingNames.add(existingSchema.get(i).get(0).toString());
		}

		AVector combinedSchema = existingSchema;
		boolean anyNew = false;
		for (long i = 0; i < newSchema.count(); i++) {
			AVector<ACell> col = (AVector<ACell>) newSchema.get(i);
			if (!existingNames.contains(col.get(0).toString())) {
				combinedSchema = combinedSchema.append(col);
				anyNew = true;
			}
		}
		if (!anyNew) return false; // no new columns to add

		final AVector<AVector<ACell>> finalSchema = (AVector<AVector<ACell>>) combinedSchema;
		ALatticeCursor<AVector<ACell>> tableCursor = cursor.path(name);
		tableCursor.updateAndGet(state -> state.assoc(SQLTable.POS_SCHEMA, finalSchema));
		return true;
	}

	/** Drops a table by creating a tombstone. */
	public boolean dropTable(String name) {
		return dropTable(Strings.create(name));
	}

	public boolean dropTable(AString name) {
		if (getLiveTable(name) == null) return false;
		ALatticeCursor<AVector<ACell>> tableCursor = cursor.path(name);
		tableCursor.set(SQLTable.createTombstoneState(now()));
		return true;
	}

	/** Checks if a table exists and is live. */
	public boolean tableExists(String name) {
		return tableExists(Strings.create(name));
	}

	public boolean tableExists(AString name) {
		return getLiveTable(name) != null;
	}

	/** Gets the schema (column definitions) for a table. */
	public AVector<AVector<ACell>> getSchema(String name) {
		return getSchema(Strings.create(name));
	}

	public AVector<AVector<ACell>> getSchema(AString name) {
		SQLTable table = getLiveTable(name);
		if (table == null) return null;
		return table.getSchema();
	}

	/** Gets the column names for a table. */
	public String[] getColumnNames(String name) {
		return getColumnNames(Strings.create(name));
	}

	public String[] getColumnNames(AString name) {
		AVector<AVector<ACell>> schema = getSchema(name);
		if (schema == null) return null;
		String[] result = new String[(int) schema.count()];
		for (int i = 0; i < result.length; i++) {
			result[i] = schema.get(i).get(0).toString();
		}
		return result;
	}

	/** Gets the column types for a table (with precision/scale). */
	public ConvexColumnType[] getColumnTypes(String name) {
		return getColumnTypes(Strings.create(name));
	}

	public ConvexColumnType[] getColumnTypes(AString name) {
		AVector<AVector<ACell>> schema = getSchema(name);
		if (schema == null) return null;
		ConvexColumnType[] result = new ConvexColumnType[(int) schema.count()];
		for (int i = 0; i < result.length; i++) {
			AVector<ACell> colDef = schema.get(i);
			ACell typeCell = colDef.get(1);

			ConvexType baseType;
			if (typeCell == null) {
				baseType = ConvexType.ANY;
			} else {
				baseType = ConvexType.fromName(typeCell.toString());
			}

			// Read precision and scale (may be null or missing for old schemas)
			int precision = -1;
			int scale = -1;
			if (colDef.count() > 2 && colDef.get(2) instanceof CVMLong p) {
				precision = (int) p.longValue();
			}
			if (colDef.count() > 3 && colDef.get(3) instanceof CVMLong s) {
				scale = (int) s.longValue();
			}

			if (scale >= 0) {
				result[i] = ConvexColumnType.withScale(baseType, precision, scale);
			} else if (precision >= 0) {
				result[i] = ConvexColumnType.withPrecision(baseType, precision);
			} else {
				result[i] = ConvexColumnType.of(baseType);
			}
		}
		return result;
	}

	/** Gets the row count for a table. */
	public long getRowCount(String name) {
		return getRowCount(Strings.create(name));
	}

	public long getRowCount(AString name) {
		SQLTable table = getLiveTable(name);
		if (table == null) return 0;
		return table.getRowCount();
	}

	// ========== Row Operations ==========

	/** Inserts a row into a table. First column is used as primary key. */
	public boolean insert(String tableName, AVector<ACell> row) {
		return insert(Strings.create(tableName), row);
	}

	public boolean insert(AString tableName, AVector<ACell> row) {
		SQLTable table = getLiveTable(tableName);
		if (table == null) return false;
		if (!(table instanceof VersionedSQLTable) && AutoIncrementRegistry.isAutoIncrement(schemaName, tableName)) {
			return insertAutoIncrement(table, tableName, row);
		}
		if (table instanceof VersionedSQLTable vt) {
			// Versioned tables are always single-column PK (enforced at
			// createTable time), so a plain toKey suffices -- no composite
			// key support needed here.
			ABlob pk = toKey(row.get(0));
			return vt.insertRowVersioned(pk, row, millis());
		}
		int pkCount = SQLTable.getPkCount(table.getState());
		ABlob pk = toCompositeKey(row, pkCount);
		return table.insertRow(pk, row, now());
	}

	/**
	 * Handles insert into an auto-increment table (always single-column PK,
	 * enforced at {@code CREATE TABLE}/{@code ALTER TABLE} time — see
	 * {@link #createTable}/{@link #convertToAutoIncrement}). If the PK
	 * (column 0) is omitted/{@code NULL}, generates a value and atomically
	 * claims it via {@link SQLTable#insertRowIfAbsent}, retrying with the
	 * next candidate on the rare cross-node collision that method can
	 * report (see {@link AutoIncrementCounters}'s own doc for why this is
	 * safe against clobbering — the value returned to the caller is always
	 * the one that was actually, atomically written, never just checked
	 * ahead of a separate later write). If the caller supplied an explicit
	 * value instead, records it so future generated values skip past it,
	 * mirroring MySQL's own auto_increment behaviour, then inserts normally
	 * (an explicit value collision is a genuine caller error, not something
	 * to silently retry past).
	 */
	private boolean insertAutoIncrement(SQLTable table, AString tableName, AVector<ACell> row) {
		if (row.get(0) == null) {
			while (true) {
				long candidate = AutoIncrementCounters.nextCandidate(this, tableName);
				AVector<ACell> candidateRow = row.assoc(0, CVMLong.create(candidate));
				ABlob pk = toKey(CVMLong.create(candidate));
				if (table.insertRowIfAbsent(pk, candidateRow, now())) return true;
				// Slot claimed by something else (e.g. a peer's write merged
				// in since we last observed this table) -- retry with a fresh candidate.
			}
		}
		if (row.get(0) instanceof CVMLong explicit) {
			AutoIncrementCounters.recordValue(this, tableName, explicit.longValue());
		}
		ABlob pk = toKey(row.get(0));
		return table.insertRow(pk, row, now());
	}

	/** Inserts a row with auto-conversion from Java types. First value is primary key. */
	public boolean insert(String tableName, Object... values) {
		return insert(Strings.create(tableName), Vectors.of(values));
	}

	/**
	 * Batch-inserts rows into a table in a single atomic lattice update.
	 *
	 * <p>Rows are sorted by primary key, grouped by block-key prefix, and written
	 * with one {@code Index.assoc()} per block rather than one per row. This
	 * reduces intermediate allocation from O(N × trie-depth) to O(blocks × trie-depth).
	 *
	 * @param tableName Target table (must exist and be live)
	 * @param rows      Rows to insert; first element of each row is the primary key
	 * @return number of newly-live rows added
	 */
	public int insertAll(String tableName, List<AVector<ACell>> rows) {
		return insertAll(Strings.create(tableName), rows);
	}

	public int insertAll(AString tableName, List<AVector<ACell>> rows) {
		if (rows == null || rows.isEmpty()) return 0;
		SQLTable table = getLiveTable(tableName);
		if (table == null) return 0;
		if (!(table instanceof VersionedSQLTable) && AutoIncrementRegistry.isAutoIncrement(schemaName, tableName)) {
			// Auto-increment generation needs per-row atomic claim-and-retry
			// (see insertAutoIncrement's own doc) -- doesn't fit this
			// method's single-batch-write optimization, so an auto-increment
			// table falls back to plain per-row inserts instead. Correct,
			// just without the block-grouping speedup for this specific case.
			int count = 0;
			for (AVector<ACell> row : rows) {
				if (insertAutoIncrement(table, tableName, row)) count++;
			}
			return count;
		}
		if (table instanceof VersionedSQLTable vt) {
			CVMLong ts = millis();
			List<Map.Entry<ABlob, AVector<ACell>>> sorted = new ArrayList<>(rows.size());
			for (AVector<ACell> row : rows) {
				sorted.add(Map.entry(toKey(row.get(0)), row));
			}
			sorted.sort(Map.Entry.comparingByKey());
			return vt.insertRowsVersioned(sorted, ts);
		}
		int pkCount = SQLTable.getPkCount(table.getState());
		CVMLong ts = now();
		List<Map.Entry<ABlob, AVector<ACell>>> sorted = new ArrayList<>(rows.size());
		for (AVector<ACell> row : rows) {
			sorted.add(Map.entry(toCompositeKey(row, pkCount), row));
		}
		sorted.sort(Map.Entry.comparingByKey());
		return table.insertRows(sorted, ts);
	}

	/** Selects a row by primary key. Single-column PK only — use the List overload for composite keys. */
	public AVector<ACell> selectByKey(String tableName, ACell primaryKey) {
		return selectByKey(Strings.create(tableName), primaryKey);
	}

	public AVector<ACell> selectByKey(AString tableName, ACell primaryKey) {
		return selectByKey(tableName, List.of(primaryKey));
	}

	/**
	 * Selects a row by a (possibly composite) primary key — one ACell per PK
	 * column, in column order. For a single-column PK, pass a singleton list.
	 */
	public AVector<ACell> selectByKey(String tableName, List<ACell> keyParts) {
		return selectByKey(Strings.create(tableName), keyParts);
	}

	public AVector<ACell> selectByKey(AString tableName, List<ACell> keyParts) {
		SQLTable table = getLiveTable(tableName);
		if (table == null) return null;

		Index<ABlob, ACell> rows = table.getRows();
		if (rows == null) return null;

		ABlob pk = toCompositeKey(Vectors.create(keyParts), keyParts.size());
		ABlob bk = RowBlock.blockKey(pk);
		ACell block = rows.get(bk);
		AVector<ACell> row = RowBlock.get(block, pk);
		if (row == null || !SQLRow.isLive(row)) return null;
		return SQLRow.getValues(row);
	}

	/** Deletes a row by primary key. Single-column PK only — use the List overload for composite keys. */
	public boolean deleteByKey(String tableName, ACell primaryKey) {
		return deleteByKey(Strings.create(tableName), primaryKey);
	}

	public boolean deleteByKey(AString tableName, ACell primaryKey) {
		return deleteByKey(tableName, List.of(primaryKey));
	}

	/**
	 * Deletes a row by a (possibly composite) primary key — one ACell per PK
	 * column, in column order. For a single-column PK, pass a singleton list.
	 */
	public boolean deleteByKey(String tableName, List<ACell> keyParts) {
		return deleteByKey(Strings.create(tableName), keyParts);
	}

	public boolean deleteByKey(AString tableName, List<ACell> keyParts) {
		SQLTable table = getLiveTable(tableName);
		if (table == null) return false;
		if (table instanceof VersionedSQLTable vt) {
			// Versioned tables are always single-column PK (enforced at
			// createTable time) -- keyParts.get(0) is the whole key.
			ABlob key = toKey(keyParts.get(0));
			return vt.deleteRowVersioned(key, millis());
		}
		ABlob key = toCompositeKey(Vectors.create(keyParts), keyParts.size());
		return table.deleteRow(key, now());
	}

	// ========== Versioned Table Operations ==========
	// These only do something meaningful for a table currently marked
	// versioned in TableVersionRegistry -- graceful empty/null/no-op
	// otherwise, since a plain SQLSchema instance may hold a mix of both
	// kinds of table side by side.

	/**
	 * Returns all recorded change events for a primary key, oldest first --
	 * empty if the table isn't versioned or doesn't exist. Each entry is
	 * {@code [values|null, CVMLong(writeSeq), CVMLong(changeType)]} — see
	 * {@link VersionedSQLTable#getHistoryWriteSeq}.
	 */
	public List<AVector<ACell>> getHistory(String tableName, ACell primaryKey) {
		return getHistory(Strings.create(tableName), primaryKey);
	}

	public List<AVector<ACell>> getHistory(AString tableName, ACell primaryKey) {
		SQLTable table = getLiveTable(tableName);
		if (!(table instanceof VersionedSQLTable vt)) return List.of();
		return vt.getHistory(toKey(primaryKey));
	}

	/**
	 * Returns the row as it existed at or before the given writeSeq
	 * (equivalent to {@code SELECT ... AS OF SYSTEM TIME}) — null if the
	 * table isn't versioned or doesn't exist.
	 *
	 * @param writeSeq Upper-bound value, comparable to {@code
	 *                 System.currentTimeMillis()} (see {@link
	 *                 VersionedSQLTable#nextHistorySeq()})
	 */
	public AVector<ACell> getAsOf(String tableName, ACell primaryKey, long writeSeq) {
		return getAsOf(Strings.create(tableName), primaryKey, writeSeq);
	}

	public AVector<ACell> getAsOf(AString tableName, ACell primaryKey, long writeSeq) {
		SQLTable table = getLiveTable(tableName);
		if (!(table instanceof VersionedSQLTable vt)) return null;
		return vt.getAsOf(toKey(primaryKey), writeSeq);
	}

	/**
	 * Converts an existing plain table to versioned (row-history-tracked) in
	 * place. Prospective-only: existing live rows are preserved unchanged,
	 * but no history is backfilled for writes that happened before this call
	 * — only writes from this point onward are tracked.
	 *
	 * @return true if converted (or already versioned); false if the table doesn't exist
	 */
	public boolean convertToVersioned(String tableName) {
		return convertToVersioned(Strings.create(tableName));
	}

	public boolean convertToVersioned(AString tableName) {
		SQLTable table = getLiveTable(tableName);
		if (table == null) return false;
		if (table instanceof VersionedSQLTable) return true; // already versioned, no-op

		int pkCount = SQLTable.getPkCount(table.getState());
		if (pkCount != 1) {
			throw new UnsupportedOperationException(
				"Cannot convert a composite-PK table to versioned (pkCount=" + pkCount + ")");
		}

		// Positions 0-3 (schema, rows, utime, liveCount) are identical
		// between SQLTable and VersionedSQLTable -- carry them over
		// directly, so existing live rows survive the conversion unchanged.
		// Position 4 differs (blockVec for plain, history for versioned) --
		// start it empty, per the locked-in prospective-only semantics.
		AVector<ACell> state = table.getState();
		AVector<ACell> versionedState = Vectors.of(
			state.get(SQLTable.POS_SCHEMA),
			state.get(SQLTable.POS_ROWS),
			state.get(SQLTable.POS_UTIME),
			state.get(SQLTable.POS_LIVE_COUNT),
			Index.EMPTY);

		ALatticeCursor<AVector<ACell>> tableCursor = cursor.path(tableName);
		tableCursor.set(versionedState);
		TableVersionRegistry.markVersioned(schemaName, tableName);
		return true;
	}

	/**
	 * Marks an existing plain table's (single-column) primary key as
	 * auto-incrementing, in place. Unlike {@link #convertToVersioned}, this
	 * doesn't change the table's underlying state shape at all — it's a
	 * pure {@link AutoIncrementRegistry} marker; the table stays an
	 * ordinary {@link SQLTable}, with existing live rows completely
	 * untouched. The counter that generates future values (see {@code
	 * AutoIncrementCounters}) seeds itself from this table's own current
	 * {@code MAX(pk)} the first time it's needed, so already-present rows
	 * (e.g. migrated historical data) are never collided with.
	 *
	 * @return true if converted (or already auto-increment); false if the table doesn't exist
	 */
	public boolean convertToAutoIncrement(String tableName) {
		return convertToAutoIncrement(Strings.create(tableName));
	}

	public boolean convertToAutoIncrement(AString tableName) {
		SQLTable table = getLiveTable(tableName);
		if (table == null) return false;
		if (table instanceof VersionedSQLTable) {
			throw new UnsupportedOperationException(
				"Cannot make a versioned table auto-increment — not supported together in this pass");
		}
		if (AutoIncrementRegistry.isAutoIncrement(schemaName, tableName)) return true; // already, no-op

		int pkCount = SQLTable.getPkCount(table.getState());
		if (pkCount != 1) {
			throw new UnsupportedOperationException(
				"Cannot convert a composite-PK table to auto-increment (pkCount=" + pkCount + ")");
		}
		AVector<AVector<ACell>> schema = table.getSchema();
		ACell pkTypeName = (schema != null && schema.count() > 0) ? schema.get(0).get(1) : null;
		if (pkTypeName == null || !"INTEGER".equals(pkTypeName.toString())) {
			throw new UnsupportedOperationException(
				"Auto-increment primary key must be INTEGER, not " + (pkTypeName == null ? "ANY" : pkTypeName));
		}

		AutoIncrementRegistry.markAutoIncrement(schemaName, tableName);
		return true;
	}

	/** Returns all live rows in a table. */
	public Index<ABlob, AVector<ACell>> selectAll(String tableName) {
		return selectAll(Strings.create(tableName));
	}

	@SuppressWarnings("unchecked")
	public Index<ABlob, AVector<ACell>> selectAll(AString tableName) {
		SQLTable table = getLiveTable(tableName);
		if (table == null) return Index.none();

		Index<ABlob, ACell> rows = table.getRows();
		if (rows == null) return Index.none();

		// Iterate blocks, expand live rows keyed by full pk
		Index<ABlob, AVector<ACell>>[] result = new Index[] { Index.none() };
		rows.forEach((bk, block) -> {
			if (RowBlock.isBlock(block)) {
				RowBlock.forEach(block, (pk, row) -> {
					if (SQLRow.isLive(row)) result[0] = result[0].assoc(pk, SQLRow.getValues(row));
				});
			} else if (block instanceof AVector && SQLRow.isLive((AVector<ACell>)block)) {
				// Legacy single-row entry (backward compat)
				result[0] = result[0].assoc(bk, SQLRow.getValues((AVector<ACell>)block));
			}
		});
		return result[0];
	}

	/**
	 * Returns all table names.
	 *
	 * @return Array of table names
	 */
	public String[] getTableNames() {
		Index<AString, AVector<ACell>> store = cursor.get();
		if (store == null) return new String[0];

		java.util.List<String> names = new java.util.ArrayList<>();
		for (var entry : store.entrySet()) {
			if (SQLTable.isLiveState(entry.getValue())) {
				names.add(entry.getKey().toString());
			}
		}
		return names.toArray(new String[0]);
	}

	/** Gets the column count for a table. */
	public int getColumnCount(String name) {
		return getColumnCount(Strings.create(name));
	}

	public int getColumnCount(AString name) {
		SQLTable table = getLiveTable(name);
		if (table == null) return 0;
		return (int) table.getColumnCount();
	}

	// ========== Secondary Index Operations ==========

	/**
	 * Creates a secondary index on a column.
	 * Scans existing rows to build the initial index, then maintains it on
	 * subsequent inserts and deletes.
	 *
	 * @param tableName  Target table (must exist and be live)
	 * @param columnName Column to index (must exist in the table schema)
	 * @return true if the index was created; false if it already exists,
	 *         the table doesn't exist, or the column doesn't exist
	 */
	public boolean createIndex(String tableName, String columnName) {
		return createIndex(Strings.create(tableName), Strings.create(columnName));
	}

	public boolean createIndex(AString tableName, AString columnName) {
		SQLTable table = getLiveTable(tableName);
		if (table == null) return false;
		return table.createColumnIndex(columnName);
	}

	/**
	 * Returns true if a secondary index exists on the named column.
	 */
	public boolean hasIndex(String tableName, String columnName) {
		return hasIndex(Strings.create(tableName), Strings.create(columnName));
	}

	public boolean hasIndex(AString tableName, AString columnName) {
		SQLTable table = getLiveTable(tableName);
		if (table == null) return false;
		return table.hasColumnIndex(columnName);
	}

	/**
	 * Drops a secondary index on a column.
	 *
	 * @return true if the index was dropped; false if it didn't exist
	 */
	public boolean dropIndex(String tableName, String columnName) {
		return dropIndex(Strings.create(tableName), Strings.create(columnName));
	}

	public boolean dropIndex(AString tableName, AString columnName) {
		SQLTable table = getLiveTable(tableName);
		if (table == null) return false;
		return table.dropColumnIndex(columnName);
	}

	/**
	 * Finds all live rows whose {@code columnName} column equals {@code value}
	 * using a secondary index (O(log n + k)) instead of a full table scan.
	 *
	 * @return matching rows as a list, or {@code null} if the table doesn't exist
	 *         or has no index on {@code columnName} (caller should fall back to
	 *         {@link #selectByColumn}/a scan in that case)
	 */
	public List<AVector<ACell>> selectByIndex(String tableName, String columnName, ACell value) {
		return selectByIndex(Strings.create(tableName), Strings.create(columnName), value);
	}

	public List<AVector<ACell>> selectByIndex(AString tableName, AString columnName, ACell value) {
		SQLTable table = getLiveTable(tableName);
		if (table == null) return null;
		return table.selectByIndex(columnName, value);
	}

	/**
	 * Returns all live rows where the named column equals {@code value}.
	 *
	 * <p>If a secondary index exists on the column, uses the index to avoid a
	 * full-table scan.  Otherwise falls back to scanning all rows.
	 *
	 * <p>Results are always re-validated against the row store, so stale index
	 * entries (possible after distributed merge) are silently filtered out.
	 *
	 * @param tableName  Target table
	 * @param columnName Column to filter on
	 * @param value      Value to match (must be the same ACell type as stored)
	 * @return Index of matching rows keyed by primary-key blob, or empty if none
	 */
	public Index<ABlob, AVector<ACell>> selectByColumn(
			String tableName, String columnName, ACell value) {
		return selectByColumn(Strings.create(tableName), Strings.create(columnName), value);
	}

	@SuppressWarnings("unchecked")
	public Index<ABlob, AVector<ACell>> selectByColumn(
			AString tableName, AString columnName, ACell value) {
		SQLTable table = getLiveTable(tableName);
		if (table == null) return Index.none();

		// Try index-backed lookup first
		Index<AString, Index<ABlob, AVector<ABlob>>> allIndices =
			SQLTable.getIndicesFromState(table.getState());
		if (allIndices != null) {
			ACell rawColIdx = allIndices.get(columnName);
			if (rawColIdx instanceof Index) {
				Index<ABlob, AVector<ABlob>> colIdx = (Index<ABlob, AVector<ABlob>>) rawColIdx;
				Index<ABlob, AVector<ACell>>[] result = new Index[]{Index.none()};
				AVector<AVector<ACell>> schema = table.getSchema();
				int ci = (schema != null)
					? SQLTable.findColIdxInSchema(schema, columnName) : -1;
				colIdx.forEach((indexKey, bucket) -> {
					if (!ColumnIndex.matchesValue(indexKey, value)) return;
					for (long i = 0; i < bucket.count(); i++) {
						ABlob pkBlob = bucket.get((int) i);
						AVector<ACell> row = table.selectByKeyBlob(pkBlob);
						if (row == null) continue; // tombstoned or missing
						// Re-validate column value (guards against stale index entries
						// and the residual chance of a pk-hash collision)
						if (ci >= 0 && ci < (int) row.count() && value.equals(row.get(ci))) {
							result[0] = result[0].assoc(pkBlob, row);
						}
					}
				});
				return result[0];
			}
		}

		// Fallback: full-scan with in-memory filter
		AVector<AVector<ACell>> schema = table.getSchema();
		int colIdx = (schema != null)
			? SQLTable.findColIdxInSchema(schema, columnName) : -1;
		if (colIdx < 0) return Index.none();
		Index<ABlob, AVector<ACell>> all = selectAll(tableName);
		Index<ABlob, AVector<ACell>>[] result = new Index[]{Index.none()};
		all.forEach((pk, row) -> {
			if (colIdx < (int) row.count() && value.equals(row.get(colIdx))) {
				result[0] = result[0].assoc(pk, row);
			}
		});
		return result[0];
	}

	/**
	 * Returns all live rows where the named column is in the inclusive range [from, to].
	 *
	 * <p>Uses the secondary index if present (iterates index entries filtering by range).
	 * Falls back to a full-table scan otherwise.
	 *
	 * <p>Range comparison uses sortable encoding: CVMLong values compare numerically;
	 * AString values compare lexicographically by UTF-8 bytes.
	 *
	 * @param tableName  Target table
	 * @param columnName Column to filter on
	 * @param from       Lower bound (inclusive)
	 * @param to         Upper bound (inclusive)
	 * @return Index of matching rows keyed by primary-key blob, or empty if none
	 */
	public Index<ABlob, AVector<ACell>> selectByColumnRange(
			String tableName, String columnName, ACell from, ACell to) {
		return selectByColumnRange(
			Strings.create(tableName), Strings.create(columnName), from, to);
	}

	@SuppressWarnings("unchecked")
	public Index<ABlob, AVector<ACell>> selectByColumnRange(
			AString tableName, AString columnName, ACell from, ACell to) {
		SQLTable table = getLiveTable(tableName);
		if (table == null) return Index.none();

		// Try index-backed lookup
		Index<AString, Index<ABlob, AVector<ABlob>>> allIndices =
			SQLTable.getIndicesFromState(table.getState());
		if (allIndices != null) {
			ACell rawColIdx = allIndices.get(columnName);
			if (rawColIdx instanceof Index) {
				Index<ABlob, AVector<ABlob>> colIdx = (Index<ABlob, AVector<ABlob>>) rawColIdx;
				Index<ABlob, AVector<ACell>>[] result = new Index[]{Index.none()};
				AVector<AVector<ACell>> schema = table.getSchema();
				int ci = (schema != null)
					? SQLTable.findColIdxInSchema(schema, columnName) : -1;
				colIdx.forEach((indexKey, bucket) -> {
					if (!ColumnIndex.matchesRange(indexKey, from, to)) return;
					for (long i = 0; i < bucket.count(); i++) {
						ABlob pkBlob = bucket.get((int) i);
						AVector<ACell> row = table.selectByKeyBlob(pkBlob);
						if (row == null) continue;
						// Re-validate (guards against stale index entries and pk-hash collisions)
						if (ci >= 0 && ci < (int) row.count()) {
							ACell colVal = row.get(ci);
							if (ColumnIndex.matchesRange(
									ColumnIndex.indexKey(colVal, pkBlob), from, to)) {
								result[0] = result[0].assoc(pkBlob, row);
							}
						}
					}
				});
				return result[0];
			}
		}

		// Fallback: full-scan with in-memory filter
		AVector<AVector<ACell>> schema = table.getSchema();
		int colIdx = (schema != null)
			? SQLTable.findColIdxInSchema(schema, columnName) : -1;
		if (colIdx < 0) return Index.none();
		byte[] fromBytes = ColumnIndex.encodeValue(from);
		byte[] toBytes   = ColumnIndex.encodeValue(to);
		Index<ABlob, AVector<ACell>> all = selectAll(tableName);
		Index<ABlob, AVector<ACell>>[] result = new Index[]{Index.none()};
		all.forEach((pk, row) -> {
			if (colIdx >= (int) row.count()) return;
			ACell colVal = row.get(colIdx);
			if (colVal == null) return;
			byte[] valBytes = ColumnIndex.encodeValue(colVal);
			if (valBytes.length != fromBytes.length) return;
			// Lexicographic comparison
			int cmpFrom = 0, cmpTo = 0;
			for (int i = 0; i < valBytes.length; i++) {
				int b = valBytes[i] & 0xFF;
				if (cmpFrom == 0) {
					if      (b > (fromBytes[i] & 0xFF)) cmpFrom = 1;
					else if (b < (fromBytes[i] & 0xFF)) cmpFrom = -1;
				}
				if (cmpTo == 0) {
					if      (b < (toBytes[i] & 0xFF)) cmpTo = -1;
					else if (b > (toBytes[i] & 0xFF)) cmpTo = 1;
				}
			}
			if (cmpFrom >= 0 && cmpTo <= 0) result[0] = result[0].assoc(pk, row);
		});
		return result[0];
	}
}
