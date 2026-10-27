package convex.db.lattice;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import convex.core.data.ABlob;
import convex.core.data.ACell;
import convex.core.data.AString;
import convex.core.data.AVector;
import convex.core.data.Index;
import convex.core.data.prim.CVMLong;

/**
 * Per-table lazily-seeded monotonic counters generating small, sequential
 * auto-increment primary key values — the counterpart to
 * {@link AutoIncrementRegistry}'s purely declarative marker.
 *
 * <p>That describes {@link AutoIncrementMode#SEQUENTIAL}. A NODE or TIME
 * table instead generates keys carrying this node's number (see
 * {@link #setNodeNumber}), so nodes that cannot see each other never
 * collide; TIME uses the logical clock described at {@link #nextCandidate}.
 *
 * <p>Deliberately NOT the {@code Math.max(prev+1, System.currentTimeMillis())}
 * HLC-style pattern used by {@code SQLSchema.WRITE_SEQ}/{@code
 * VersionedSQLTable.nextHistorySeq}/{@code Querylog.nextLogId} — those exist
 * to generate ordering values where the actual number doesn't matter, only
 * relative ordering across a distributed system, and produce huge
 * millis-scale values as a result. An auto-increment PRIMARY KEY is a
 * business-visible value an application may embed in URLs, expect to fit an
 * ordinary column, etc. — callers need small, sequential-looking ids
 * matching what MySQL/PostgreSQL would generate, seeded from whatever data
 * (e.g. migrated historical rows) already exists.
 *
 * <p>Process-local, like the HLC counters above — not persisted to the
 * lattice. A fresh process re-seeds a table's counter from {@code
 * MAX(existing PK)} the first time either {@link #nextValue} or {@link
 * #recordValue} touches that table, which is always correct (reflects the
 * live table's actual current state) rather than relying on remembered
 * state from a previous run. Both methods seed identically on first use —
 * regardless of whether a table's very first touch in this process is a
 * generate or an explicit-value insert, the counter always starts from the
 * table's true current max, not an incomplete view.
 *
 * <p>Safe under the rare cross-node race this system's distributed nature
 * allows (two nodes both generating a value before either replicates to the
 * other): {@link #nextCandidate} only ever hands out a <em>candidate</em> —
 * the caller (see {@code SQLSchema.insert}) must claim it via {@code
 * SQLTable#insertRowIfAbsent}, which atomically checks-and-writes within a
 * single lattice update, and retry with the next candidate if that reports
 * the slot was already taken. Deliberately not validated here via a
 * separate pre-check-then-later-write: a check now paired with a write
 * later would leave the exact same TOCTOU race window {@code
 * insertRowIfAbsent} exists to close (found live 2026-08-11, before this
 * version shipped — a candidate could pass a standalone existence check and
 * then still collide with something written in the gap before the caller's
 * own subsequent write, silently clobbering it).
 */
public class AutoIncrementCounters {

	private static final ConcurrentHashMap<AString, ConcurrentHashMap<AString, AtomicLong>> BY_SCHEMA =
		new ConcurrentHashMap<>();

	/**
	 * This node's number, put in the low bits of NODE and TIME keys. Set once
	 * at boot by the embedder (dbase: {@code meta.node.NODEID}); null until then,
	 * and a NODE or TIME table then refuses to generate a key.
	 */
	private static volatile Long nodeNumber;

	private AutoIncrementCounters() {}

	/** Sets the node number used in NODE and TIME keys. */
	public static void setNodeNumber(long node) {
		if (node < 0 || node > AutoIncrementMode.MAX_NODE) {
			throw new IllegalArgumentException("Node number " + node + " does not fit in "
				+ AutoIncrementMode.NODE_BITS + " bits");
		}
		nodeNumber = node;
	}

	/** The node number used in NODE and TIME keys, or null if not set. */
	public static Long getNodeNumber() {
		return nodeNumber;
	}

	private static long requireNode(AString tableName, AutoIncrementMode mode) {
		Long node = nodeNumber;
		if (node == null) {
			throw new IllegalStateException("Table \"" + tableName + "\" generates " + mode
				+ " keys, but this node has no node number");
		}
		return node;
	}

	private static ConcurrentHashMap<AString, AtomicLong> tablesFor(AString schemaName) {
		return BY_SCHEMA.computeIfAbsent(schemaName, k -> new ConcurrentHashMap<>());
	}

	private static AutoIncrementMode modeOf(SQLSchema schema, AString tableName) {
		AutoIncrementMode mode = AutoIncrementRegistry.getMode(schema.getRegistryKey(), tableName);
		return (mode == null) ? AutoIncrementMode.SEQUENTIAL : mode;
	}

	/**
	 * Returns the next candidate auto-increment value for {@code
	 * tableName}'s (single-column) primary key, seeding the counter from
	 * the table's own existing keys on first use. A candidate is
	 * NOT guaranteed free — the caller must claim it atomically (see this
	 * class's own doc) and call this again for a fresh candidate if the
	 * claim fails. Never reuses a gap left by a deleted row, matching
	 * MySQL's own auto_increment behaviour.
	 *
	 * <p>NODE: the counter counts this node's own keys only. TIME: the clock
	 * is max(previous + 1, wall clock, highest key in the table + 1), so a key
	 * is above every key this node has received, even when its wall clock is
	 * behind the node that wrote them.
	 */
	public static long nextCandidate(SQLSchema schema, AString tableName) {
		AutoIncrementMode mode = modeOf(schema, tableName);
		switch (mode) {
			case NODE: {
				long node = requireNode(tableName, mode);
				long counter = counterFor(schema, tableName, mode).incrementAndGet();
				return compose(counter, node, tableName);
			}
			case TIME: {
				long node = requireNode(tableName, mode);
				AtomicLong clock = counterFor(schema, tableName, mode);
				long wall = System.currentTimeMillis() - AutoIncrementMode.TIME_EPOCH_MS;
				long seen = highestKey(schema, tableName) >>> AutoIncrementMode.NODE_BITS;
				long prev, next;
				do {
					prev = clock.get();
					next = Math.max(Math.max(prev + 1, wall), seen + 1);
				} while (!clock.compareAndSet(prev, next));
				return compose(next, node, tableName);
			}
			default:
				return counterFor(schema, tableName, mode).incrementAndGet();
		}
	}

	private static long compose(long high, long node, AString tableName) {
		if (high >= (1L << (63 - AutoIncrementMode.NODE_BITS))) {
			throw new IllegalStateException("Table \"" + tableName + "\" has run out of generated keys");
		}
		return (high << AutoIncrementMode.NODE_BITS) | node;
	}

	/**
	 * Records that {@code valueUsed} was just written (whether generated by
	 * {@link #nextCandidate} or supplied explicitly by a caller) so the counter
	 * never later generates a value that collides with it — mirrors
	 * MySQL's own behaviour where an explicit INSERT ahead of the current
	 * auto_increment counter pulls the counter forward. For a NODE table only
	 * a key carrying this node's number moves the counter; for a TIME table any
	 * key moves the clock.
	 */
	public static void recordValue(SQLSchema schema, AString tableName, long valueUsed) {
		AutoIncrementMode mode = modeOf(schema, tableName);
		long pull = valueUsed;
		if (mode == AutoIncrementMode.NODE) {
			Long node = nodeNumber;
			if (node == null || valueUsed < 0 || (valueUsed & AutoIncrementMode.MAX_NODE) != node) return;
			pull = valueUsed >>> AutoIncrementMode.NODE_BITS;
		} else if (mode == AutoIncrementMode.TIME) {
			if (valueUsed < 0) return;
			pull = valueUsed >>> AutoIncrementMode.NODE_BITS;
		}
		AtomicLong counter = counterFor(schema, tableName, mode);
		long prev;
		do {
			prev = counter.get();
			if (prev >= pull) return;
		} while (!counter.compareAndSet(prev, pull));
	}

	/** Clears a table's counter (e.g. on DROP TABLE), so a later re-CREATE of the same name re-seeds from scratch. */
	public static void unmark(AString schemaName, AString tableName) {
		Map<AString, AtomicLong> tables = BY_SCHEMA.get(schemaName);
		if (tables != null) tables.remove(tableName);
	}

	private static AtomicLong counterFor(SQLSchema schema, AString tableName, AutoIncrementMode mode) {
		return tablesFor(schema.getRegistryKey()).computeIfAbsent(tableName,
			k -> new AtomicLong(seedFromExistingData(schema, tableName, mode)));
	}

	private static long seedFromExistingData(SQLSchema schema, AString tableName, AutoIncrementMode mode) {
		if (mode == AutoIncrementMode.TIME) {
			return highestKey(schema, tableName) >>> AutoIncrementMode.NODE_BITS;
		}
		if (mode == AutoIncrementMode.NODE) {
			// The highest counter among this node's own keys, deleted ones included
			long node = requireNode(tableName, mode);
			long[] max = {0};
			forEachKey(schema, tableName, v -> {
				if ((v & AutoIncrementMode.MAX_NODE) == node) {
					max[0] = Math.max(max[0], v >>> AutoIncrementMode.NODE_BITS);
				}
			});
			return max[0];
		}
		long max = 0;
		Index<ABlob, AVector<ACell>> all = schema.selectAll(tableName);
		if (all != null) {
			for (Map.Entry<ABlob, AVector<ACell>> e : all.entrySet()) {
				ACell pk = e.getValue().get(0);
				if (pk instanceof CVMLong l) max = Math.max(max, l.longValue());
			}
		}
		return max;
	}

	/**
	 * The highest non-negative key in the table, deleted rows included, or 0.
	 * Keys are stored as 8-byte big-endian blobs in a sorted index, so this
	 * reads from the end and stops at the first non-negative one: a few steps,
	 * not a scan.
	 */
	static long highestKey(SQLSchema schema, AString tableName) {
		SQLTable table = schema.getLiveTable(tableName);
		Index<ABlob, ACell> rows = (table == null) ? null : table.getRows();
		if (rows == null) return 0;
		for (long b = rows.count() - 1; b >= 0; b--) {
			ACell block = rows.entryAt(b).getValue();
			for (int i = RowBlock.count(block) - 1; i >= 0; i--) {
				long v = keyValue(RowBlock.getKey(block, i));
				if (v >= 0) return v;
			}
		}
		return 0;
	}

	private static void forEachKey(SQLSchema schema, AString tableName, java.util.function.LongConsumer action) {
		SQLTable table = schema.getLiveTable(tableName);
		Index<ABlob, ACell> rows = (table == null) ? null : table.getRows();
		if (rows == null) return;
		for (long b = 0; b < rows.count(); b++) {
			ACell block = rows.entryAt(b).getValue();
			for (int i = 0; i < RowBlock.count(block); i++) {
				long v = keyValue(RowBlock.getKey(block, i));
				if (v >= 0) action.accept(v);
			}
		}
	}

	/** An 8-byte key as a long; -1 for any other length. */
	private static long keyValue(ABlob key) {
		if (key.count() != 8) return -1;
		long v = 0;
		for (int i = 0; i < 8; i++) v = (v << 8) | (key.byteAtUnchecked(i) & 0xFF);
		return v;
	}
}
