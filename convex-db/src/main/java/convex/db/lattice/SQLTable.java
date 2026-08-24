package convex.db.lattice;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import convex.core.data.ABlob;
import convex.core.data.ACell;
import convex.core.data.AString;
import convex.core.data.AVector;
import convex.core.data.Blob;
import convex.core.data.Hash;
import convex.core.data.Index;
import convex.core.data.Strings;
import convex.core.data.Vectors;
import convex.core.data.prim.CVMLong;
import convex.lattice.ALatticeComponent;
import convex.lattice.cursor.ALatticeCursor;

/**
 * A single SQL table within the lattice table store.
 *
 * <p>Wraps a lattice cursor pointing at the table's state vector:
 * [schema, rows, utime, liveCount, blockVec, indices]
 * <ul>
 *   <li>schema (AVector) - Column definitions: [[name, type, precision, scale], ...]</li>
 *   <li>rows (Index) - Row data: primary-key prefix (ABlob) → RowBlock (flat Blob)</li>
 *   <li>utime (CVMLong) - Schema update timestamp for LWW</li>
 *   <li>liveCount (CVMLong) - Number of live (non-tombstone) rows</li>
 *   <li>blockVec (AVector|null) - Sequential block list for O(n)-total full scans</li>
 *   <li>indices (Index) - Column indices: colName (AString) → Index(indexKey → pk)</li>
 * </ul>
 *
 * <p>Schema is immutable after creation (for now). Row data merges independently.
 *
 * <p>Obtained from {@link SQLSchema#getTable(String)} as a cursor-backed component
 * in the hierarchy: ConvexDB → SQLDatabase → SQLSchema → SQLTable.
 */
public class SQLTable extends ALatticeComponent<AVector<ACell>> {

	/** Position of schema in table vector */
	static final int POS_SCHEMA    = 0;
	/** Position of rows in table vector */
	static final int POS_ROWS      = 1;
	/** Position of update timestamp */
	static final int POS_UTIME     = 2;
	/** Position of live row count */
	static final int POS_LIVE_COUNT = 3;
	/**
	 * Position of the sequential block vector (for O(1)-heap full scans).
	 *
	 * <p>Value is {@code AVector<ACell>} (the ordered list of live block blobs) or
	 * {@code null} when invalidated by a single-row write. Building it costs one
	 * Index traversal; reading it requires no Index traversal at all.
	 *
	 * <p>VersionedSQLTable stores its history {@code Index} at this same slot, so
	 * {@link #getBlockVec()} guards with {@code instanceof AVector} to distinguish.
	 */
	static final int POS_BLOCK_VEC = 4;
	/**
	 * Position of secondary column indices.
	 *
	 * <p>Value is {@code Index<AString, Index<ABlob, AVector<ABlob>>>} mapping column
	 * name to a column index. The column index maps
	 * {@code encode(value) ++ hash(pk)} → bucket of pks (see {@link ColumnIndex}).
	 * May be {@code null} when no indices are defined.
	 */
	static final int POS_INDICES   = 5;
	/**
	 * Position of composite primary key column count.
	 *
	 * <p>Value is {@code CVMLong} storing how many leading columns (after
	 * pkFirst ordering) form the composite primary key. Absent or 1 means
	 * single-column PK (backward compatible). When > 1, SQLSchema.insert()
	 * concatenates the first N column blobs into a single composite key.
	 */
	static final int POS_PK_COUNT  = 6;

	SQLTable(ALatticeCursor<AVector<ACell>> cursor) {
		super(cursor);
	}

	// ========== Static State Factories ==========

	/**
	 * Creates the initial state vector for a new live table.
	 *
	 * @param schema Column definitions
	 * @param timestamp Creation timestamp
	 * @return State vector [schema, empty-rows, timestamp, liveCount=0, emptyBlockVec, emptyIndices]
	 */
	@SuppressWarnings("unchecked")
	static AVector<ACell> createState(AVector<AVector<ACell>> schema, CVMLong timestamp) {
		return createState(schema, timestamp, 1);
	}

	/**
	 * Creates the initial state vector for a new live table with a composite primary key.
	 *
	 * @param schema   Column definitions (PK columns must be first)
	 * @param timestamp Creation timestamp
	 * @param pkCount  Number of leading columns that form the composite PK (>= 1)
	 * @return State vector [schema, empty-rows, timestamp, liveCount=0, emptyBlockVec, emptyIndices, pkCount]
	 */
	@SuppressWarnings("unchecked")
	static AVector<ACell> createState(AVector<AVector<ACell>> schema, CVMLong timestamp, int pkCount) {
		return Vectors.of(schema, (Index<ABlob, ACell>) Index.EMPTY, timestamp, CVMLong.ZERO,
			Vectors.empty(), Index.EMPTY, CVMLong.create(pkCount));
	}

	/**
	 * Extracts the composite PK column count from a state vector.
	 * Returns 1 if absent (backward compatible with single-column PK tables).
	 */
	static int getPkCount(AVector<ACell> state) {
		if (state == null || state.count() <= POS_PK_COUNT) return 1;
		ACell slot = state.get(POS_PK_COUNT);
		if (slot instanceof CVMLong c) return (int) c.longValue();
		return 1;
	}

	/**
	 * Creates a tombstone state vector for a dropped table.
	 *
	 * @param timestamp Deletion timestamp
	 * @return Tombstone state vector [null, null, timestamp, liveCount=0]
	 */
	static AVector<ACell> createTombstoneState(CVMLong timestamp) {
		return Vectors.of(null, null, timestamp, CVMLong.ZERO);
	}

	// ========== Static Helpers (for raw state access) ==========

	/**
	 * Checks if a raw table state represents a live (non-tombstone) table.
	 */
	static boolean isLiveState(AVector<ACell> state) {
		return state != null && state.get(POS_SCHEMA) != null;
	}

	// ========== Static Helpers (block vector) ==========

	/**
	 * Builds a sequential block vector from a rows Index.
	 * Used to populate slot 4 for O(1)-heap full scans.
	 */
	@SuppressWarnings("unchecked")
	static AVector<ACell> buildBlockVec(Index<ABlob, ACell> rows) {
		if (rows == null || rows.count() == 0) return Vectors.empty();
		List<ACell> blocks = new ArrayList<>((int) Math.min(rows.count(), Integer.MAX_VALUE));
		rows.forEach((bk, block) -> {
			if (RowBlock.isBlock(block)) blocks.add(block);
		});
		return Vectors.create(blocks);
	}

	// ========== Static Helpers (column indices) ==========

	/**
	 * Extracts the secondary column indices map from a raw state vector.
	 * Returns null if not present or empty.
	 *
	 * <p>Each column index maps an {@link ColumnIndex} key (value + hashed pk) to a
	 * <b>bucket</b> ({@code AVector<ABlob>}) of pks — see {@link ColumnIndex} class
	 * javadoc for why the pk is hashed and why a bucket (not a single pk) is needed.
	 */
	@SuppressWarnings("unchecked")
	static Index<AString, Index<ABlob, AVector<ABlob>>> getIndicesFromState(AVector<ACell> state) {
		if (state == null || state.count() <= POS_INDICES) return null;
		ACell slot = state.get(POS_INDICES);
		if (!(slot instanceof Index)) return null;
		Index<AString, Index<ABlob, AVector<ABlob>>> idx = (Index<AString, Index<ABlob, AVector<ABlob>>>) slot;
		return (idx.count() == 0) ? null : idx;
	}

	/** Returns a bucket with {@code pk} added, creating a new single-entry bucket if none exists. */
	static AVector<ABlob> addToBucket(AVector<ABlob> bucket, ABlob pk) {
		if (bucket == null) return Vectors.of(pk);
		for (long i = 0; i < bucket.count(); i++) {
			if (bucket.get((int) i).equals(pk)) return bucket; // already present
		}
		return bucket.append(pk);
	}

	/** Returns a bucket with {@code pk} removed, or {@code null} if that empties the bucket. */
	static AVector<ABlob> removeFromBucket(AVector<ABlob> bucket, ABlob pk) {
		if (bucket == null) return null;
		List<ABlob> remaining = new ArrayList<>();
		for (long i = 0; i < bucket.count(); i++) {
			ABlob b = bucket.get((int) i);
			if (!b.equals(pk)) remaining.add(b);
		}
		return remaining.isEmpty() ? null : Vectors.create(remaining.toArray(new ABlob[0]));
	}

	/** Unions two buckets (used when merging column indices from two lattice replicas). */
	static AVector<ABlob> mergeBuckets(AVector<ABlob> a, AVector<ABlob> b) {
		if (a == null) return b;
		if (b == null) return a;
		AVector<ABlob> merged = a;
		for (long i = 0; i < b.count(); i++) {
			merged = addToBucket(merged, b.get((int) i));
		}
		return merged;
	}

	/**
	 * Finds the column position index within a schema vector by column name.
	 * Returns -1 if not found.
	 */
	static int findColIdxInSchema(AVector<AVector<ACell>> schema, AString colName) {
		if (schema == null) return -1;
		for (int i = 0; i < (int) schema.count(); i++) {
			if (colName.equals(schema.get(i).get(0))) return i;
		}
		return -1;
	}

	/**
	 * Builds a fresh column index from all live rows in the given state.
	 * Used when {@link #createColumnIndex} is called on a table with existing data.
	 */
	@SuppressWarnings("unchecked")
	static Index<ABlob, AVector<ABlob>> buildColumnIndex(AVector<ACell> state, int colIdx) {
		Index<ABlob, ACell> rows = (Index<ABlob, ACell>) state.get(POS_ROWS);
		if (rows == null) return Index.none();

		@SuppressWarnings("rawtypes")
		Index[] result = {Index.none()};
		rows.forEach((bk, block) -> {
			if (RowBlock.isBlock(block)) {
				RowBlock.forEach(block, (pk, row) -> {
					if (SQLRow.isLive(row)) {
						AVector<ACell> values = SQLRow.getValues(row);
						if (colIdx < (int) values.count() && values.get(colIdx) != null) {
							ABlob iKey = ColumnIndex.indexKey(values.get(colIdx), pk);
							AVector<ABlob> bucket = (AVector<ABlob>) result[0].get(iKey);
							result[0] = result[0].assoc(iKey, addToBucket(bucket, pk));
						}
					}
				});
			}
		});
		return (Index<ABlob, AVector<ABlob>>) result[0];
	}

	/**
	 * Returns a copy of the indices map with a new or updated entry for the given row insert.
	 * Handles both new rows and updates to existing rows (removes old index entry first).
	 *
	 * @param indices  Current indices map (may be null)
	 * @param schema   Table schema (for column position lookup)
	 * @param pk       Primary key of the row being inserted
	 * @param oldValues Old column values (null if this is a new row, not an update)
	 * @param newValues New column values
	 */
	@SuppressWarnings("unchecked")
	static Index<AString, Index<ABlob, AVector<ABlob>>> indexAddRow(
			Index<AString, Index<ABlob, AVector<ABlob>>> indices,
			AVector<AVector<ACell>> schema,
			ABlob pk, AVector<ACell> oldValues, AVector<ACell> newValues) {
		if (indices == null || indices.count() == 0) return indices;

		@SuppressWarnings("rawtypes")
		final Index[] result = {indices};
		indices.forEach((colName, colIndex) -> {
			int ci = findColIdxInSchema(schema, (AString) colName);
			if (ci < 0) return;

			Index<ABlob, AVector<ABlob>> col = (Index<ABlob, AVector<ABlob>>) colIndex;

			// Remove old entry (if updating an existing live row)
			if (oldValues != null && ci < (int) oldValues.count()) {
				ACell oldVal = oldValues.get(ci);
				if (oldVal != null) {
					ABlob oldKey = ColumnIndex.indexKey(oldVal, pk);
					AVector<ABlob> bucket = removeFromBucket(col.get(oldKey), pk);
					col = (bucket == null) ? col.dissoc(oldKey) : col.assoc(oldKey, bucket);
				}
			}
			// Add new entry
			if (ci < (int) newValues.count()) {
				ACell newVal = newValues.get(ci);
				if (newVal != null) {
					ABlob newKey = ColumnIndex.indexKey(newVal, pk);
					col = col.assoc(newKey, addToBucket(col.get(newKey), pk));
				}
			}

			result[0] = result[0].assoc(colName, col);
		});
		return (Index<AString, Index<ABlob, AVector<ABlob>>>) result[0];
	}

	/**
	 * Returns a copy of the indices map with the given row removed (delete/tombstone).
	 */
	@SuppressWarnings("unchecked")
	static Index<AString, Index<ABlob, AVector<ABlob>>> indexRemoveRow(
			Index<AString, Index<ABlob, AVector<ABlob>>> indices,
			AVector<AVector<ACell>> schema,
			ABlob pk, AVector<ACell> values) {
		if (indices == null || indices.count() == 0) return indices;

		@SuppressWarnings("rawtypes")
		final Index[] result = {indices};
		indices.forEach((colName, colIndex) -> {
			int ci = findColIdxInSchema(schema, (AString) colName);
			if (ci < 0 || ci >= (int) values.count()) return;
			ACell val = values.get(ci);
			if (val == null) return;
			Index<ABlob, AVector<ABlob>> col = (Index<ABlob, AVector<ABlob>>) colIndex;
			ABlob key = ColumnIndex.indexKey(val, pk);
			AVector<ABlob> bucket = removeFromBucket(col.get(key), pk);
			col = (bucket == null) ? col.dissoc(key) : col.assoc(key, bucket);
			result[0] = result[0].assoc(colName, col);
		});
		return (Index<AString, Index<ABlob, AVector<ABlob>>>) result[0];
	}

	/**
	 * Extends or updates the indices slot (slot 5) in a state vector.
	 *
	 * <p>Preserves slot 6 (composite PK column count) from the incoming state if
	 * present. Without this, the first insertRow/deleteRow on any table would
	 * silently drop pkCount, causing every write after that to fall back to a
	 * single-column key (row.get(0) only) — collapsing distinct rows that share
	 * the same first PK column onto one key and overwriting each other.
	 */
	private static AVector<ACell> withIndices(AVector<ACell> state,
			ACell blockVec, CVMLong liveCount,
			Index<AString, Index<ABlob, AVector<ABlob>>> indices,
			ACell rows, ACell timestamp) {
		AVector<ACell> result = Vectors.of(state.get(POS_SCHEMA), rows, timestamp, liveCount, blockVec, indices);
		if (state.count() > POS_PK_COUNT) {
			result = result.append(state.get(POS_PK_COUNT));
		}
		return result;
	}

	// ========== Cursor-backed Instance Methods ==========

	/**
	 * Returns the underlying state vector.
	 */
	public AVector<ACell> getState() {
		return cursor.get();
	}

	/**
	 * Gets the schema from this table.
	 *
	 * @return Schema vector, or null if tombstone
	 */
	@SuppressWarnings("unchecked")
	public AVector<AVector<ACell>> getSchema() {
		AVector<ACell> state = cursor.get();
		if (state == null) return null;
		return (AVector<AVector<ACell>>) state.get(POS_SCHEMA);
	}

	/**
	 * Scans every row in this table for {@link BlobCAS} reference cells,
	 * returning the set of hashes they point to — without resolving them, so
	 * this never throws even if some referenced hash isn't locally fetchable
	 * yet (the whole point is finding out what to fetch before assuming it's
	 * there). Used by replication (see {@code DbaseServer.replicateOneSchema}/
	 * {@code replicateOneDatabase}'s own class docs) to discover which
	 * hashes a just-pulled table's data depends on — a CAS reference is an
	 * ordinary opaque blob value, not a genuine Convex {@code Ref}, so
	 * nothing else would ever notice it needs fetching.
	 */
	public Set<Hash> findCasReferenceHashes() {
		Set<Hash> hashes = new HashSet<>();
		Index<ABlob, ACell> rows = getRows();
		if (rows == null) return hashes;
		rows.forEach((bk, block) -> {
			if (RowBlock.isBlock(block)) {
				RowBlock.forEach(block, (pk, row) -> hashes.addAll(SQLRow.findCasReferenceHashes((Blob) row.get(0))));
			}
		});
		return hashes;
	}

	/**
	 * Gets the rows index from this table.
	 *
	 * @return Row block index (ABlob prefix → RowBlock), or null if tombstone
	 */
	@SuppressWarnings("unchecked")
	public Index<ABlob, ACell> getRows() {
		AVector<ACell> state = cursor.get();
		if (state == null) return null;
		return (Index<ABlob, ACell>) state.get(POS_ROWS);
	}

	/**
	 * Gets the number of leading columns (after pkFirst ordering) that form this
	 * table's composite primary key. Returns 1 for a plain single-column PK.
	 *
	 * <p>Callers doing single-column equality pushdown (e.g. {@code WHERE col0 = ?})
	 * must check this is 1 before treating it as a full primary-key lookup —
	 * for a composite PK, an equality on only the first column can match zero,
	 * one, or many rows, not a unique row.
	 */
	public int getPkCount() {
		return getPkCount(cursor.get());
	}

	/**
	 * Gets the update timestamp.
	 */
	public CVMLong getTimestamp() {
		AVector<ACell> state = cursor.get();
		if (state == null) return null;
		return (CVMLong) state.get(POS_UTIME);
	}

	/**
	 * Gets the sequential block vector (slot 4) for fast full scans.
	 * Returns null if not present, invalidated (null slot), or if slot 4 holds
	 * a non-AVector value (e.g. VersionedSQLTable's history Index).
	 */
	@SuppressWarnings("unchecked")
	public AVector<ACell> getBlockVec() {
		AVector<ACell> state = cursor.get();
		if (state == null || state.count() <= POS_BLOCK_VEC) return null;
		ACell slot = state.get(POS_BLOCK_VEC);
		return (slot instanceof AVector) ? (AVector<ACell>) slot : null;
	}

	/**
	 * Checks if this table is a tombstone (dropped).
	 */
	public boolean isTombstone() {
		AVector<ACell> state = cursor.get();
		return state != null && state.get(POS_SCHEMA) == null;
	}

	/**
	 * Checks if this table is live (not dropped).
	 */
	public boolean isLive() {
		return isLiveState(cursor.get());
	}

	/**
	 * Gets the column index for a column name.
	 *
	 * @return Column index, or -1 if not found
	 */
	public int getColumnIndex(AString columnName) {
		AVector<AVector<ACell>> schema = getSchema();
		if (schema == null) return -1;
		for (int i = 0; i < schema.count(); i++) {
			AVector<ACell> col = schema.get(i);
			if (columnName.equals(col.get(0))) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * Gets the number of columns.
	 */
	public long getColumnCount() {
		AVector<AVector<ACell>> schema = getSchema();
		if (schema == null) return 0;
		return schema.count();
	}

	/**
	 * Gets the number of live (non-tombstone) rows. O(1).
	 */
	@SuppressWarnings("unchecked")
	public long getRowCount() {
		AVector<ACell> state = cursor.get();
		if (state == null) return 0;
		if (state.count() <= POS_LIVE_COUNT) {
			// Legacy state vector without liveCount — fall back to Index.count()
			Index<ABlob, ACell> rows = getRows();
			return (rows != null) ? rows.count() : 0;
		}
		CVMLong lc = (CVMLong) state.get(POS_LIVE_COUNT);
		return (lc != null) ? lc.longValue() : 0;
	}

	// ========== Secondary Index Methods ==========

	/**
	 * Returns true if a secondary index exists on the named column.
	 */
	public boolean hasColumnIndex(AString colName) {
		Index<AString, Index<ABlob, AVector<ABlob>>> indices = getIndicesFromState(cursor.get());
		return indices != null && indices.get(colName) != null;
	}

	/**
	 * Creates a secondary index on the named column.
	 * Builds the index from existing rows. Returns false if the column doesn't exist
	 * or the index already exists.
	 */
	@SuppressWarnings("unchecked")
	public boolean createColumnIndex(AString colName) {
		boolean[] created = {false};
		cursor.updateAndGet(state -> {
			if (state == null || state.get(POS_SCHEMA) == null) return state;
			AVector<AVector<ACell>> schema = (AVector<AVector<ACell>>) state.get(POS_SCHEMA);
			int colIdx = findColIdxInSchema(schema, colName);
			if (colIdx < 0) return state; // column not found

			ACell rawIndices = (state.count() > POS_INDICES) ? state.get(POS_INDICES) : null;
			Index<AString, Index<ABlob, AVector<ABlob>>> indices =
				(rawIndices instanceof Index) ? (Index<AString, Index<ABlob, AVector<ABlob>>>) rawIndices
				                              : Index.none();
			if (indices.get(colName) != null) return state; // already exists

			// Build index from existing rows
			Index<ABlob, AVector<ABlob>> colIndex = buildColumnIndex(state, colIdx);
			indices = indices.assoc(colName, colIndex);
			created[0] = true;

			// Extend state to 6 slots if needed
			if (state.count() <= POS_INDICES) {
				return state.append(indices);
			}
			return state.assoc(POS_INDICES, indices);
		});
		return created[0];
	}

	/**
	 * Drops the secondary index on the named column.
	 * Returns false if no such index exists.
	 */
	@SuppressWarnings("unchecked")
	public boolean dropColumnIndex(AString colName) {
		boolean[] dropped = {false};
		cursor.updateAndGet(state -> {
			if (state == null || state.count() <= POS_INDICES) return state;
			ACell rawIndices = state.get(POS_INDICES);
			if (!(rawIndices instanceof Index)) return state;
			Index<AString, Index<ABlob, AVector<ABlob>>> indices =
				(Index<AString, Index<ABlob, AVector<ABlob>>>) rawIndices;
			if (indices.get(colName) == null) return state; // doesn't exist
			indices = indices.dissoc(colName);
			dropped[0] = true;
			return state.assoc(POS_INDICES, indices);
		});
		return dropped[0];
	}

	/**
	 * Looks up a single row by its primary key blob (bypassing ACell→ABlob conversion).
	 * Returns the column values vector, or null if not found or tombstoned.
	 */
	@SuppressWarnings("unchecked")
	public AVector<ACell> selectByKeyBlob(ABlob pk) {
		Index<ABlob, ACell> rows = getRows();
		if (rows == null) return null;
		ABlob bk = RowBlock.blockKey(pk);
		ACell block = rows.get(bk);
		AVector<ACell> row = RowBlock.get(block, pk);
		if (row == null || !SQLRow.isLive(row)) return null;
		return SQLRow.getValues(row);
	}

	/**
	 * Finds all live rows whose {@code colName} column equals {@code value}, using
	 * the secondary index on that column (O(log n + k), k = matching rows) instead
	 * of a full table scan.
	 *
	 * <p>Every candidate pk (resolved from the matching index bucket — see
	 * {@link ColumnIndex} class javadoc for why a bucket rather than a single pk)
	 * is verified against the actual row's column value before being included,
	 * both to guard against the residual chance of a pk-hash collision and to
	 * filter stale entries left behind by concurrent/merged writes.
	 *
	 * @return matching rows, or {@code null} if no index exists on {@code colName}
	 *         (caller should fall back to a full scan in that case)
	 */
	@SuppressWarnings("unchecked")
	public List<AVector<ACell>> selectByIndex(AString colName, ACell value) {
		Index<AString, Index<ABlob, AVector<ABlob>>> indices = getIndicesFromState(cursor.get());
		if (indices == null) return null;
		Index<ABlob, AVector<ABlob>> colIndex = indices.get(colName);
		if (colIndex == null) return null;

		AVector<AVector<ACell>> schema = getSchema();
		int colIdx = (schema != null) ? findColIdxInSchema(schema, colName) : -1;

		byte[] vb = ColumnIndex.encodeValue(value);
		long n = colIndex.count();

		// Binary search for the first entry whose key's value-prefix >= vb.
		// Index keys are ordered [valueLen, value_bytes, pkHash_bytes], so all
		// entries for the same value are contiguous.
		long lo = 0, hi = n;
		while (lo < hi) {
			long mid = (lo + hi) >>> 1;
			ABlob key = colIndex.entryAt(mid).getKey();
			if (compareKeyValuePrefix(key, vb) < 0) lo = mid + 1; else hi = mid;
		}

		List<AVector<ACell>> results = new ArrayList<>();
		for (long i = lo; i < n; i++) {
			var entry = colIndex.entryAt(i);
			ABlob key = entry.getKey();
			if (!ColumnIndex.matchesValue(key, value)) break; // past the matching range
			AVector<ABlob> bucket = entry.getValue();
			for (long b = 0; b < bucket.count(); b++) {
				ABlob pk = bucket.get((int) b);
				AVector<ACell> row = selectByKeyBlob(pk);
				if (row == null) continue; // stale entry: row deleted since indexed
				if (colIdx >= 0 && colIdx < (int) row.count() && value.equals(row.get(colIdx))) {
					results.add(row);
				}
			}
		}
		return results;
	}

	/**
	 * Compares an index key's {@code [valueLen, value_bytes]} prefix against a
	 * target encoded value, ignoring the trailing pk-hash bytes.
	 */
	private static int compareKeyValuePrefix(ABlob key, byte[] vb) {
		int targetLen = 2 + vb.length;
		int n = (int) Math.min(targetLen, key.count());
		for (int i = 0; i < n; i++) {
			int kb = key.byteAtUnchecked(i) & 0xFF;
			int tb = (i == 0) ? (vb.length >> 8) & 0xFF
					: (i == 1) ? vb.length & 0xFF
					: vb[i - 2] & 0xFF;
			if (kb != tb) return kb - tb;
		}
		return (int) key.count() - targetLen;
	}

	// ========== Mutation Methods ==========

	/**
	 * Inserts a row into this table.
	 *
	 * @param pk Primary key (blob-encoded)
	 * @param values Full row values (including PK as first element)
	 * @param timestamp Insert timestamp
	 * @return true if inserted successfully
	 */
	@SuppressWarnings("unchecked")
	public boolean insertRow(ABlob pk, AVector<ACell> values, CVMLong timestamp) {
		boolean[] result = new boolean[1];
		cursor.updateAndGet(state -> {
			if (state == null || state.get(POS_SCHEMA) == null) return state;
			Index<ABlob, ACell> rows = (Index<ABlob, ACell>) state.get(POS_ROWS);
			if (rows == null) rows = BlockTableLattice.INSTANCE.zero();
			ABlob bk = RowBlock.blockKey(pk);
			ACell block = rows.get(bk);
			AVector<ACell> existing = RowBlock.get(block, pk);
			boolean addsLive = (existing == null || !SQLRow.isLive(existing));
			AVector<ACell> oldValues = (existing != null && SQLRow.isLive(existing))
				? SQLRow.getValues(existing) : null;
			ACell newBlock = RowBlock.put(block, pk, SQLRow.create(values, timestamp));
			rows = rows.assoc(bk, newBlock);
			long liveCount = getLiveCount(state) + (addsLive ? 1 : 0);
			result[0] = true;

			// Update column indices (remove old entry if updating, add new)
			AVector<AVector<ACell>> schema = (AVector<AVector<ACell>>) state.get(POS_SCHEMA);
			Index<AString, Index<ABlob, AVector<ABlob>>> indices =
				indexAddRow(getIndicesFromState(state), schema, pk, oldValues, values);

			return withIndices(state, null, CVMLong.create(liveCount), indices, rows, timestamp);
		});
		return result[0];
	}

	/**
	 * Inserts a row only if no live row currently occupies {@code pk} —
	 * atomically, within the same lattice {@code updateAndGet} as the
	 * check, unlike a separate exists-check followed by a later {@link
	 * #insertRow} call (which would leave a race window between the two:
	 * something else could claim the key in between, and {@code insertRow}
	 * would then silently overwrite it — it always upserts unconditionally).
	 *
	 * <p>Used by auto-increment value generation ({@code
	 * AutoIncrementCounters}), where a generated candidate must never
	 * silently clobber a row that's already there (e.g. from a concurrent
	 * write on another node not yet observed locally) — the caller is
	 * expected to retry with the next candidate when this returns false,
	 * not treat it as a fatal error.
	 *
	 * @return true if the row was inserted (the slot was genuinely free),
	 *         false if a live row already occupied {@code pk} (nothing written)
	 */
	@SuppressWarnings("unchecked")
	public boolean insertRowIfAbsent(ABlob pk, AVector<ACell> values, CVMLong timestamp) {
		boolean[] inserted = new boolean[1];
		cursor.updateAndGet(state -> {
			if (state == null || state.get(POS_SCHEMA) == null) return state;
			Index<ABlob, ACell> rows = (Index<ABlob, ACell>) state.get(POS_ROWS);
			if (rows == null) rows = BlockTableLattice.INSTANCE.zero();
			ABlob bk = RowBlock.blockKey(pk);
			ACell block = rows.get(bk);
			AVector<ACell> existing = RowBlock.get(block, pk);
			if (existing != null && SQLRow.isLive(existing)) {
				inserted[0] = false;
				return state; // slot occupied -- no-op, don't touch state
			}
			ACell newBlock = RowBlock.put(block, pk, SQLRow.create(values, timestamp));
			rows = rows.assoc(bk, newBlock);
			long liveCount = getLiveCount(state) + 1;
			inserted[0] = true;

			AVector<AVector<ACell>> schema = (AVector<AVector<ACell>>) state.get(POS_SCHEMA);
			Index<AString, Index<ABlob, AVector<ABlob>>> indices =
				indexAddRow(getIndicesFromState(state), schema, pk, null, values);

			return withIndices(state, null, CVMLong.create(liveCount), indices, rows, timestamp);
		});
		return inserted[0];
	}

	/**
	 * Batch-inserts pre-sorted rows in a single atomic update.
	 *
	 * <p>Rows sharing the same block-key prefix are grouped into one block and
	 * committed with a single {@code Index.assoc()} call, reducing intermediate
	 * allocation from O(N × trie-depth) to O(blocks × trie-depth).
	 *
	 * @param sortedEntries (pk → row-values) pairs sorted by pk ascending
	 * @param timestamp     Write timestamp
	 * @return number of newly-live rows inserted
	 */
	@SuppressWarnings("unchecked")
	public int insertRows(List<Map.Entry<ABlob, AVector<ACell>>> sortedEntries, CVMLong timestamp) {
		if (sortedEntries.isEmpty()) return 0;
		int[] newLive = {0};
		cursor.updateAndGet(state -> {
			if (state == null || state.get(POS_SCHEMA) == null) return state;
			Index<ABlob, ACell> rows = (Index<ABlob, ACell>) state.get(POS_ROWS);
			if (rows == null) rows = BlockTableLattice.INSTANCE.zero();

			// Group entries by block key, then use putAll for each block.
			// For fresh (empty) tables: collect blocks inline to build blockVec at O(batch)
			// cost. For non-empty tables: invalidate blockVec (avoids O(n) full-Index traversal).
			boolean wasEmpty = getLiveCount(state) == 0;
			ABlob curBk = null;
			List<ABlob> blockPks = new ArrayList<>();
			List<AVector<ACell>> blockRows = new ArrayList<>();
			List<ACell> blockList = wasEmpty ? new ArrayList<>() : null;

			for (var e : sortedEntries) {
				ABlob pk = e.getKey();
				ABlob bk = RowBlock.blockKey(pk);
				if (curBk == null || !bk.equals(curBk)) {
					if (curBk != null) {
						ACell existing = rows.get(curBk);
						ACell newBlock = RowBlock.putAll(existing, blockPks, blockRows, newLive);
						rows = rows.assoc(curBk, newBlock);
						if (blockList != null) blockList.add(newBlock);
						blockPks = new ArrayList<>();
						blockRows = new ArrayList<>();
					}
					curBk = bk;
				}
				blockPks.add(pk);
				blockRows.add(SQLRow.create(e.getValue(), timestamp));
			}
			if (curBk != null) {
				ACell existing = rows.get(curBk);
				ACell newBlock = RowBlock.putAll(existing, blockPks, blockRows, newLive);
				rows = rows.assoc(curBk, newBlock);
				if (blockList != null) blockList.add(newBlock);
			}

			long liveCount = getLiveCount(state) + newLive[0];
			// blockVec: inline-built for fresh tables (O(batch)), null for incremental inserts
			AVector<ACell> blockVec = (blockList != null) ? Vectors.create(blockList) : null;

			// Update column indices for all inserted rows
			AVector<AVector<ACell>> schema = (AVector<AVector<ACell>>) state.get(POS_SCHEMA);
			Index<AString, Index<ABlob, AVector<ABlob>>> indices = getIndicesFromState(state);
			if (indices != null && indices.count() > 0) {
				for (var e : sortedEntries) {
					// For batch inserts (typically fresh tables), no old-value removal needed
					indices = indexAddRow(indices, schema, e.getKey(), null, e.getValue());
				}
			}

			return withIndices(state, blockVec, CVMLong.create(liveCount), indices, rows, timestamp);
		});
		return newLive[0];
	}

	/**
	 * Deletes a row by primary key.
	 *
	 * @param key Primary key (blob-encoded)
	 * @param timestamp Deletion timestamp
	 * @return true if a live row was deleted
	 */
	@SuppressWarnings("unchecked")
	public boolean deleteRow(ABlob key, CVMLong timestamp) {
		boolean[] result = new boolean[1];
		cursor.updateAndGet(state -> {
			if (state == null || state.get(POS_SCHEMA) == null) return state;
			Index<ABlob, ACell> rows = (Index<ABlob, ACell>) state.get(POS_ROWS);
			if (rows == null) return state;
			ABlob bk = RowBlock.blockKey(key);
			ACell block = rows.get(bk);
			AVector<ACell> existing = RowBlock.get(block, key);
			if (existing == null || !SQLRow.isLive(existing)) return state;
			AVector<ACell> oldValues = SQLRow.getValues(existing);
			ACell newBlock = RowBlock.put(block, key, SQLRow.createTombstone(timestamp));
			rows = rows.assoc(bk, newBlock);
			long liveCount = getLiveCount(state) - 1;
			result[0] = true;

			// Remove from column indices
			AVector<AVector<ACell>> schema = (AVector<AVector<ACell>>) state.get(POS_SCHEMA);
			Index<AString, Index<ABlob, AVector<ABlob>>> indices =
				indexRemoveRow(getIndicesFromState(state), schema, key, oldValues);

			// Invalidate blockVec (tombstone inserted; scan must skip it)
			return withIndices(state, null, CVMLong.create(liveCount), indices, rows, timestamp);
		});
		return result[0];
	}

	// ========== Helpers ==========

	/**
	 * Extracts the live count from a state vector, handling legacy format.
	 */
	static long getLiveCount(AVector<ACell> state) {
		if (state == null || state.count() <= POS_LIVE_COUNT) return 0;
		CVMLong lc = (CVMLong) state.get(POS_LIVE_COUNT);
		return (lc != null) ? lc.longValue() : 0;
	}

	/**
	 * Extracts blockVec from a raw state vector, or null if absent/invalid.
	 */
	@SuppressWarnings("unchecked")
	static AVector<ACell> getBlockVecFromState(AVector<ACell> state) {
		if (state == null || state.count() <= POS_BLOCK_VEC) return null;
		ACell slot = state.get(POS_BLOCK_VEC);
		return (slot instanceof AVector) ? (AVector<ACell>) slot : null;
	}

	/**
	 * Computes live row count by scanning an Index. Used only during merge.
	 */
	@SuppressWarnings("unchecked")
	static long computeLiveCount(Index<ABlob, ACell> rows) {
		if (rows == null) return 0;
		long[] count = new long[1];
		rows.forEach((bk, block) -> {
			if (RowBlock.isBlock(block)) {
				RowBlock.forEach(block, (pk, row) -> { if (SQLRow.isLive(row)) count[0]++; });
			} else if (block instanceof AVector && SQLRow.isLive((AVector<ACell>)block)) {
				// Legacy single raw row entry (backward compat)
				count[0]++;
			}
		});
		return count[0];
	}

	/**
	 * Merges two column indices maps using union semantics.
	 * Entries from both sides are unioned (bucket-wise, so two replicas that each
	 * added a different pk under the same index key both survive). Stale entries
	 * (for deleted rows) are filtered at query time by re-validating against the
	 * row store.
	 */
	@SuppressWarnings("unchecked")
	static Index<AString, Index<ABlob, AVector<ABlob>>> mergeColumnIndices(
			Index<AString, Index<ABlob, AVector<ABlob>>> a,
			Index<AString, Index<ABlob, AVector<ABlob>>> b) {
		if (a == null || a.count() == 0) return b;
		if (b == null || b.count() == 0) return a;

		@SuppressWarnings({"unchecked", "rawtypes"})
		final Index[] result = {a};
		b.forEach((colName, bColIdx) -> {
			ACell rawA = ((Index<AString, ?>) result[0]).get(colName);
			if (!(rawA instanceof Index)) {
				// a doesn't have this column index — use b's
				result[0] = result[0].assoc(colName, bColIdx);
			} else {
				// Both have it — union merge: merge b's buckets into a's, key by key
				@SuppressWarnings("rawtypes")
				final Index[] mergedCol = {(Index) rawA};
				((Index<ABlob, AVector<ABlob>>) bColIdx).forEach((k, bBucket) -> {
					AVector<ABlob> aBucket = (AVector<ABlob>) mergedCol[0].get(k);
					mergedCol[0] = mergedCol[0].assoc(k, mergeBuckets(aBucket, bBucket));
				});
				result[0] = result[0].assoc(colName, mergedCol[0]);
			}
		});
		return (Index<AString, Index<ABlob, AVector<ABlob>>>) result[0];
	}

	// ========== Static Merge (used by lattice layer) ==========

	/**
	 * Merges two table state vectors.
	 * Schema uses LWW (latest timestamp wins).
	 * Rows merge using BlockTableLattice.
	 * Column indices use union merge.
	 */
	@SuppressWarnings("unchecked")
	public static AVector<ACell> merge(AVector<ACell> a, AVector<ACell> b) {
		if (a == null) return b;
		if (b == null) return a;

		CVMLong timeA = (CVMLong) a.get(POS_UTIME);
		CVMLong timeB = (CVMLong) b.get(POS_UTIME);

		// Determine schema winner (LWW)
		AVector<ACell> schemaWinner;
		if (timeA == null && timeB == null) {
			schemaWinner = a;
		} else if (timeA == null) {
			schemaWinner = b;
		} else if (timeB == null) {
			schemaWinner = a;
		} else if (timeB.longValue() > timeA.longValue()) {
			schemaWinner = b;
		} else {
			schemaWinner = a;
		}

		// If schema winner is tombstone, return tombstone
		if (schemaWinner.get(POS_SCHEMA) == null) {
			return schemaWinner;
		}

		// Merge rows
		Index<ABlob, ACell> rowsA = (Index<ABlob, ACell>) a.get(POS_ROWS);
		Index<ABlob, ACell> rowsB = (Index<ABlob, ACell>) b.get(POS_ROWS);
		Index<ABlob, ACell> mergedRows = BlockTableLattice.INSTANCE.merge(rowsA, rowsB);

		// Compute live count and blockVec for merged result
		long liveCount;
		AVector<ACell> blockVec;
		Index<AString, Index<ABlob, AVector<ABlob>>> mergedIndices;
		if (mergedRows == rowsA) {
			liveCount = getLiveCount(a);
			blockVec = getBlockVecFromState(a);
			mergedIndices = getIndicesFromState(a);
		} else if (mergedRows == rowsB) {
			liveCount = getLiveCount(b);
			blockVec = getBlockVecFromState(b);
			mergedIndices = getIndicesFromState(b);
		} else {
			// Rows actually merged — recompute liveCount; invalidate blockVec;
			// union-merge column indices (stale entries filtered at query time)
			liveCount = computeLiveCount(mergedRows);
			blockVec = null;
			mergedIndices = mergeColumnIndices(
				getIndicesFromState(a), getIndicesFromState(b));
		}

		// Return merged table with schema winner's schema (preserving pkCount)
		ACell pkCount = (schemaWinner.count() > POS_PK_COUNT) ? schemaWinner.get(POS_PK_COUNT) : CVMLong.create(1);
		return Vectors.of(schemaWinner.get(POS_SCHEMA), mergedRows,
			schemaWinner.get(POS_UTIME), CVMLong.create(liveCount),
			blockVec, mergedIndices, pkCount);
	}
}
