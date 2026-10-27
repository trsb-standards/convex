package convex.db.lattice;

/**
 * How an {@code AUTOINCREMENT} table generates its primary key. Chosen at
 * {@code CREATE TABLE} (or the first {@code ALTER TABLE ... AUTOINCREMENT})
 * and fixed afterwards: keys already handed out cannot be renumbered.
 *
 * <p>The NODE and TIME keys put this node's number in the low
 * {@link #NODE_BITS} bits, so two nodes never generate the same key, even
 * while they cannot see each other. See PLAN_owner_tree_replication.md,
 * "Generated keys".
 */
public enum AutoIncrementMode {

	/** 1, 2, 3 ... Unique only where one node checks every insert. Plain {@code AUTOINCREMENT}. */
	SEQUENTIAL(1),

	/** (counter &lt;&lt; NODE_BITS) | node. Unique offline; not sorted by creation. */
	NODE(2),

	/**
	 * (logical milliseconds since {@link #TIME_EPOCH_MS} &lt;&lt; NODE_BITS) | node.
	 * Unique offline and sorted by creation: a key is always above every key
	 * already in the table on this node.
	 */
	TIME(3);

	/** Bits of the key that hold the node number: about 4 million nodes. */
	public static final int NODE_BITS = 22;

	/** Largest node number that fits in {@link #NODE_BITS}. */
	public static final long MAX_NODE = (1L << NODE_BITS) - 1;

	/**
	 * Start of the TIME clock: 2026-01-01T00:00:00Z. The 41 bits left above the
	 * node number last until 2095.
	 */
	public static final long TIME_EPOCH_MS = 1767225600000L;

	/** The value stored for this mode in {@code meta.ot.AUTOINCREMENT} (0 there means none). */
	public final int code;

	AutoIncrementMode(int code) {
		this.code = code;
	}

	/** The mode for a {@code meta.ot.AUTOINCREMENT} value, or null for 0 / unknown. */
	public static AutoIncrementMode fromCode(long code) {
		for (AutoIncrementMode m : values()) {
			if (m.code == code) return m;
		}
		return null;
	}

	/** The mode named in SQL ({@code SEQUENTIAL}, {@code NODE}, {@code TIME}); null or empty means SEQUENTIAL. */
	public static AutoIncrementMode parse(String word) {
		if (word == null || word.isBlank()) return SEQUENTIAL;
		return valueOf(word.trim().toUpperCase());
	}
}
