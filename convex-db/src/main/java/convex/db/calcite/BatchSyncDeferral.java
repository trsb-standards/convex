package convex.db.calcite;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-Avatica-connection registry defering {@code ConvexMeta.syncIfAutoCommit}
 * from firing on every single {@code Execute} in a pg-wire extended-protocol
 * pipeline to firing once, at the next {@code Sync} — the actual flush
 * boundary the wire protocol defines.
 *
 * <p><b>Found live 2026-09-13</b>, tracing a "hung" benchmark: {@code
 * PgProtocolHandler.executeWithParameters} calls the real internal {@code
 * java.sql.PreparedStatement#execute()} once per {@code Bind+Execute} the
 * client sends — including every row of a JDBC {@code
 * PreparedStatement#executeBatch()} — and {@code ConvexMeta} calls {@code
 * syncIfAutoCommit()} (announce + persist + broadcast) after <em>every</em>
 * one of those, unconditionally. A "batch" therefore paid the full per-
 * statement sync cost per row regardless of client-side batching, giving
 * JDBC batching against a live node zero throughput benefit over unbatched
 * single-row inserts — measured live: 100,000 rows via {@code
 * executeBatch()} at ~237 rows/sec, ~7 minutes, indistinguishable from a
 * plain unbatched loop and easily mistaken for a genuine hang.
 *
 * <p>Fix: {@code executeWithParameters} calls {@link #begin} before running
 * a bound statement (always the extended protocol — the simple-query path
 * used by a person at {@code psql} never touches this); {@code
 * ConvexMeta#syncIfAutoCommit} checks {@link #isDeferring} and, if so, just
 * records whether a write happened ({@link #markDirty}) instead of actually
 * syncing; {@code PgProtocolHandler#handleSync} — the wire protocol's own
 * pipeline-flush signal, sent once after however many Binds+Executes the
 * client pipelined — calls {@link #end}, and if it returns true (something
 * was dirty), triggers exactly one real sync via {@code
 * ConvexMeta#flushDeferredSync}.
 *
 * <p><b>Durability/consistency note, not a correctness gap</b>: local reads
 * on the same connection already see every write immediately regardless of
 * {@code sync()} — the lattice cursor is mutated in place before any
 * broadcast/persist happens (see {@code ConvexMeta}'s own class doc on how
 * autocommit publishing works). Deferring sync only widens the window in
 * which an uncommitted batch's durability-to-disk and visibility-to-peers
 * lags behind local visibility — the same trade-off every JDBC driver's own
 * batch semantics already make (durability is guaranteed at flush/commit
 * boundaries, not per individual statement inside a batch), not a new or
 * looser guarantee than JDBC batching implies elsewhere.
 */
public final class BatchSyncDeferral {

	private BatchSyncDeferral() {}

	private static final Set<String> deferring = ConcurrentHashMap.newKeySet();
	private static final Set<String> dirty = ConcurrentHashMap.newKeySet();

	/** Marks {@code connectionId} as inside a deferred-sync window (an in-flight Execute pipeline, not yet Synced). */
	public static void begin(String connectionId) {
		if (connectionId != null) deferring.add(connectionId);
	}

	/** True if {@code connectionId} is currently inside a deferred-sync window. */
	public static boolean isDeferring(String connectionId) {
		return connectionId != null && deferring.contains(connectionId);
	}

	/** Records that a write happened during {@code connectionId}'s current deferred-sync window. */
	public static void markDirty(String connectionId) {
		if (connectionId != null) dirty.add(connectionId);
	}

	/**
	 * Ends the deferred-sync window for {@code connectionId} (called from the
	 * wire protocol's own {@code Sync} handler).
	 *
	 * @return true if a write happened during the window and a real sync is
	 *         now owed; false if the window was read-only (or there was no
	 *         open window at all) and nothing needs to happen
	 */
	public static boolean end(String connectionId) {
		if (connectionId == null) return false;
		deferring.remove(connectionId);
		return dirty.remove(connectionId);
	}

	/**
	 * Discards any deferred-sync state for {@code connectionId} without
	 * flushing — for connection teardown (a client that disconnects mid-
	 * pipeline, without ever sending a final {@code Sync}), so these maps
	 * never accumulate entries for connections that are already gone.
	 */
	public static void discard(String connectionId) {
		if (connectionId == null) return;
		deferring.remove(connectionId);
		dirty.remove(connectionId);
	}
}
