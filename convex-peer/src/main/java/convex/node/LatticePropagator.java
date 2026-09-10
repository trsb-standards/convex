package convex.node;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import convex.api.Convex;
import convex.core.Result;
import convex.core.cpos.CPoSConstants;
import convex.core.data.AccountKey;
import convex.core.data.ACell;
import convex.core.data.AMap;
import convex.core.data.AVector;
import convex.core.data.Blob;
import convex.core.data.Cells;
import convex.core.data.Format;
import convex.core.exceptions.MissingDataException;
import convex.core.data.Hash;
import convex.core.data.Ref;
import convex.core.data.Vectors;
import convex.core.data.prim.CVMLong;
import convex.core.lang.RT;
import convex.core.message.Message;
import convex.core.message.MessageTag;
import convex.core.message.MessageType;
import convex.core.store.AStore;
import convex.core.util.LatestUpdateQueue;
import convex.core.util.ThreadUtils;
import convex.core.util.Utils;
import convex.lattice.cursor.Root;
import convex.lattice.ALattice;
import convex.lattice.LatticeContext;
import convex.lattice.cursor.Cursors;
import convex.lattice.cursor.ALatticeCursor;
import convex.lattice.cursor.RootLatticeCursor;

/**
 * Self-contained component for propagating lattice values.
 *
 * <p>A LatticePropagator handles the complete output pipeline for a lattice node:
 * announce to store (writes cells + tracks novelty), set root data (persistence),
 * and broadcast deltas to peers. Snapshot processing returns the store-backed
 * value to its caller; it does not mutate a cursor through a side callback.
 *
 * <p>A LatticePropagator owns:
 * <ul>
	 *   <li>An {@link AStore} — for delta tracking (announce/novelty detection),
	 *       persistence (setRootData), and scoped DATA_REQUEST resolution.</li>
 *   <li>A {@link LatticeConnectionManager} — outbound peer connections and broadcast.</li>
 *   <li>A background thread — event-driven processing loop with periodic root sync.</li>
 * </ul>
 *
 * <p>Values are pushed in via {@link #triggerBroadcast(ACell)}. Each propagator owns
 * its filter and lattice-aware working view, while NodeServer owns the authoritative
 * root cursor. For synchronous primary snapshots the caller owns installation of the
 * returned value. Pull operations only acquire store-backed values; NodeServer owns
 * authoritative merge and re-propagation through its root cursor.
 *
	 * <p>The store also scopes peer capabilities: peer connections are configured with
	 * the propagator's store, so DATA_REQUEST from peers can only resolve data that exists
	 * there. The filter governs outbound publication; independently acquired inbound cells
	 * may already exist in the store and remain an operator access-policy concern.
 *
 * <p>Designed so the peer {@code BeliefPropagator} can eventually compose or extend
 * this class. Belief is an ACell; belief broadcast uses the same delta encoding
 * ({@code Cells.announce} + {@code Format.encodeDelta}).
 */
public class LatticePropagator implements Closeable {

	private static final Logger log = LoggerFactory.getLogger(LatticePropagator.class.getName());

	/**
	 * Interval between root-only sync broadcasts (milliseconds).
	 * Provides a lightweight periodic sync mechanism for divergence detection.
	 */
	public static final long ROOT_SYNC_INTERVAL = 30_000L;

	/**
	 * How long a single root-sync send to one peer waits for that peer's
	 * response before treating the attempt as failed, for backoff purposes.
	 * Bounded and modest -- this runs on the propagator's own background
	 * thread, never a client-facing one, so briefly blocking it here is
	 * safe, but an unbounded or overly generous wait would still delay this
	 * thread's other duties (draining {@link #triggerQueue}) for no benefit.
	 */
	private static final long ROOT_SYNC_ACK_TIMEOUT_MS = 3_000L;

	/** Backoff ceiling for a peer that keeps failing root sync (milliseconds). */
	private static final long ROOT_SYNC_MAX_BACKOFF = 10 * 60_000L;

	/**
	 * Store for delta tracking (novelty detection via announce), persistence
	 * (setRootData), and peer data resolution. Missing data requests for an announced
	 * value should be routed here.
	 */
	private final AStore store;

	/**
	 * Connection manager for outbound peer connections and broadcast.
	 */
	private final LatticeConnectionManager connectionManager;

	/** Lattice used to reconcile this propagator's own subset with new snapshots. */
	private ALattice<ACell> lattice;

	/** Merge context shared with the owning NodeServer. */
	private LatticeContext mergeContext = LatticeContext.EMPTY;

	/** Projection applied before any value crosses this propagator's store boundary. */
	private LatticeFilter<ACell> filter = value -> value;

	/**
	 * This propagator's current logical view. It may contain accepted inbound values
	 * which have not yet appeared in a later projection from the authoritative root.
	 */
	private RootLatticeCursor<ACell> workingCursor;

	/** Primary publication replaces its view from the authoritative cursor. */
	private boolean primary;

	/**
	 * Background propagation thread
	 */
	private Thread propagationThread;

	/**
	 * Flag indicating if the propagator is running
	 */
	private volatile boolean running = false;

	/**
	 * Queue for receiving lattice values to process.
	 * Uses LatestUpdateQueue which only stores the most recent value,
	 * coalescing rapid updates into a single processing of the latest state.
	 * Safe because lattice values are monotonic (V2 >= V1 implies V1 is subsumed).
	 */
	private final LatestUpdateQueue<ACell> triggerQueue = new LatestUpdateQueue<>();

	/** Whether snapshots publish the store root after announcement. */
	private volatile boolean persistenceEnabled = true;

	/** Maximum encoded body size for one outbound delta or DATA-ahead chunk. */
	private volatile int maxDeltaMessageSize = NodeConfig.DEFAULT_MAX_MESSAGE_SIZE;

	/** Maximum combined encoded bodies materialised for one eager delta broadcast. */
	private volatile int maxDeltaBroadcastSize = NodeConfig.DEFAULT_MAX_DELTA_BROADCAST_SIZE;

	/**
	 * Logical size ceiling (bytes, via {@link ACell#getMemorySize}) for a single
	 * top-level entry {@link #sendUnscopedRootSync} will reference during
	 * periodic root sync. Defaults to this node's own {@code
	 * NodeConfig#getMaxInboundValueSize()} (wired by {@code NodeServer}, same
	 * pattern as {@link #maxDeltaMessageSize}) -- a reasonable proxy for what
	 * most peers will actually accept, since nodes in a fleet typically share
	 * the same default.
	 *
	 * <p>Found live 2026-08-25: sending an indirect reference to a schema that
	 * will obviously be rejected for size anyway still makes the receiver walk
	 * its full {@code Acquiror}/DATA_REQUEST acquisition path for it first --
	 * {@code NodeServer.withinInboundSizeLimit} only runs *after* acquisition
	 * completes. Fetching an 11MB+ value in that many-chunk exchange, over and
	 * over on every periodic sync tick to an oversized schema, is exactly the
	 * kind of high-volume, multi-message-per-second traffic that surfaced a
	 * separate, still-not-fully-root-caused decode/framing corruption on a
	 * freshly (re)established connection (see this project's own live
	 * incident notes) -- checking size on the SENDING side, before ever
	 * referencing the data at all, means acquisition for an obviously
	 * oversized entry is never attempted in the first place, sidestepping
	 * that failure mode entirely rather than chasing it deeper into the
	 * acquisition layer.
	 */
	private volatile long maxOutboundReferenceSize = Long.MAX_VALUE;

	/** Whether root publication has changed the persistent store since its last checkpoint. */
	private boolean dirty;

	/**
	 * Cursor holding the last value announced to this propagator's store.
	 *
	 * <p>This is the propagator's cached view of what it has published — the
	 * store-backed snapshot it most recently announced. LATTICE_QUERY
	 * responses are served from this cursor, so peers only see data this
	 * propagator has actually committed. NodeServer exposes this view only to
	 * connections assigned to this propagator; reverse DATA_REQUEST resolution
	 * uses the same store.
	 *
	 * <p>Each propagator owns its own announced cursor. This keeps query and data
	 * access scoped to one store. Filtering and working-view reconciliation complete
	 * before this cursor advances.
	 */
	private final Root<ACell> announcedCursor = new Root<>();

	/**
	 * Timestamp of last broadcast. Volatile for cross-thread visibility — the
	 * caller's thread (synchronous publication path) and the background propagation
	 * thread may both read and write this.
	 */
	private volatile long lastBroadcastTime = 0L;

	/**
	 * Timestamp of last root sync broadcast (background thread only). Kept
	 * for {@link #getLastRootSyncTime()}'s existing observability contract
	 * (tracks the most recent round across every peer); the actual per-peer
	 * due/backoff decision is {@link #rootSyncState} below, not this field.
	 */
	private long lastRootSyncTime = 0L;

	/**
	 * Per-peer root-sync scheduling state -- when a peer is next due for a
	 * sync attempt, and how many consecutive attempts have failed.
	 *
	 * <p>Found live 2026-08-25: the pre-existing design used a single,
	 * propagator-wide {@link #lastRootSyncTime} with no per-peer memory of
	 * outcome at all, so a peer that had just rejected an oversized sync
	 * (see the fix on {@code NodeServer.withinInboundSizeLimit} and {@link
	 * #sendUnscopedRootSync}) was retried at the exact same fixed interval
	 * as a healthy one, forever -- turning what should have been a
	 * self-limiting or self-resolving issue into a sustained, silent
	 * failure loop for the life of the process. Background thread only, so
	 * plain (unsynchronized) mutable fields on {@link PeerSyncState} are
	 * safe -- see that class's own doc.
	 */
	private final Map<AccountKey, PeerSyncState> rootSyncState = new java.util.concurrent.ConcurrentHashMap<>();

	/**
	 * Mutable scheduling state for one peer's root sync. Only ever read or
	 * written from {@link #maybePerformRootSync}, which always runs on this
	 * propagator's own single background thread (see {@link
	 * #propagationLoop}) -- never concurrently with itself -- so plain
	 * fields (no {@code volatile}/atomics) are sufficient; only the owning
	 * {@link #rootSyncState} map itself needs concurrent-safe structure,
	 * for the (background-thread-only) {@code computeIfAbsent} pattern.
	 */
	private static final class PeerSyncState {
		long nextAttemptTime = 0L;
		int consecutiveFailures = 0;

		boolean dueFor(long now) {
			return now >= nextAttemptTime;
		}

		void recordSuccess(long now) {
			consecutiveFailures = 0;
			nextAttemptTime = now + ROOT_SYNC_INTERVAL;
		}

		void recordFailure(long now) {
			consecutiveFailures++;
			// Exponential backoff, capped: INTERVAL, 2x, 4x, ... up to the ceiling.
			long backoff = ROOT_SYNC_INTERVAL << Math.min(consecutiveFailures, 10);
			if (backoff < 0 || backoff > ROOT_SYNC_MAX_BACKOFF) backoff = ROOT_SYNC_MAX_BACKOFF;
			nextAttemptTime = now + backoff;
		}
	}

	/**
	 * Count of broadcasts sent. Atomic because both the caller's thread and
	 * the background thread may increment.
	 */
	private final java.util.concurrent.atomic.AtomicLong broadcastCount = new java.util.concurrent.atomic.AtomicLong();

	/**
	 * Count of root sync broadcasts sent
	 */
	private long rootSyncCount = 0L;

	/**
	 * Serialises all store-writing pipelines through this propagator. The
	 * propagator is the sole live writer of {@code setRootData} on its store
	 * (see {@code PERSISTENCE.md} — sole-writer invariant), and pipelines
	 * must not interleave: an older snapshot's {@code setRootData} landing
	 * after a newer snapshot's would silently demote the published root. {@link
	 * #processSnapshot} and {@link #persist} both acquire this lock so the
	 * caller's thread (sync hook), the background propagation thread (pull,
	 * drain), and explicit persistence calls run their full pipelines
	 * sequentially.
	 */
	private final Object writeLock = new Object();

	/**
	 * Creates a new LatticePropagator with the given store and connection manager.
	 *
	 * @param store Store for delta tracking and persistence
	 * @param connectionManager Connection manager for outbound peers
	 */
	public LatticePropagator(AStore store, LatticeConnectionManager connectionManager) {
		if (store == null) throw new IllegalArgumentException("Store must not be null");
		if (connectionManager == null) throw new IllegalArgumentException("ConnectionManager must not be null");
		this.store = store;
		this.connectionManager = connectionManager;
	}

	/**
	 * Creates a new LatticePropagator with the given store, creating a new
	 * ConnectionManager that uses the same store.
	 *
	 * @param store Store for delta tracking, persistence, and peer data resolution
	 */
	public LatticePropagator(AStore store) {
		this(store, new LatticeConnectionManager(store));
	}

	/**
	 * Creates a propagator which owns a lattice projection and reconciles later
	 * snapshots with its current view using current/propagator state as {@code own}.
	 */
	@SuppressWarnings("unchecked")
	public <V extends ACell> LatticePropagator(AStore store, ALattice<V> lattice,
			LatticeFilter<V> filter) {
		this(store, new LatticeConnectionManager(store), lattice, filter);
	}

	/** Creates a filtered propagator with an explicitly configured connection manager. */
	@SuppressWarnings("unchecked")
	public <V extends ACell> LatticePropagator(AStore store,
			LatticeConnectionManager connectionManager, ALattice<V> lattice,
			LatticeFilter<V> filter) {
		this(store, connectionManager);
		if (lattice == null) throw new IllegalArgumentException("Lattice must not be null");
		if (filter == null) throw new IllegalArgumentException("Lattice filter must not be null");
		this.lattice = (ALattice<ACell>) lattice;
		this.filter = (LatticeFilter<ACell>) filter;
	}

	/** Configures the lattice semantics supplied by the owning node before launch. */
	@SuppressWarnings("unchecked")
	void configure(ALattice<?> lattice, LatticeContext context, boolean primary) {
		synchronized (writeLock) {
			if (running) throw new IllegalStateException("Cannot configure a running propagator");
			this.lattice = (ALattice<ACell>) lattice;
			this.mergeContext = (context != null) ? context : LatticeContext.EMPTY;
			this.primary = primary;
			if (workingCursor != null) workingCursor.setContext(this.mergeContext);
		}
	}

	// ========== Configuration ==========

	/**
	 * Enables or disables root publication. Announcement still runs when disabled
	 * because it provides delta tracking and store-backed references.
	 *
	 * @param enabled true to publish roots
	 */
	public void setPersistenceEnabled(boolean enabled) {
		this.persistenceEnabled = enabled;
	}

	/** Configures the encoded body limit for outbound delta chunks. */
	public void setMaxDeltaMessageSize(int limit) {
		if (limit<1 || limit>CPoSConstants.MAX_MESSAGE_LENGTH) {
			throw new IllegalArgumentException("Delta message limit must be between 1 and "
				+CPoSConstants.MAX_MESSAGE_LENGTH+": "+limit);
		}
		this.maxDeltaMessageSize=limit;
		if (maxDeltaBroadcastSize<limit) maxDeltaBroadcastSize=limit;
	}

	public int getMaxDeltaMessageSize() {
		return maxDeltaMessageSize;
	}

	/** Configures the total encoded working-set limit for one eager delta. */
	public void setMaxDeltaBroadcastSize(int limit) {
		if (limit<maxDeltaMessageSize || limit>CPoSConstants.MAX_MESSAGE_LENGTH) {
			throw new IllegalArgumentException("Delta broadcast limit must be between "
				+maxDeltaMessageSize+" and "+CPoSConstants.MAX_MESSAGE_LENGTH+": "+limit);
		}
		this.maxDeltaBroadcastSize=limit;
	}

	public int getMaxDeltaBroadcastSize() {
		return maxDeltaBroadcastSize;
	}

	/**
	 * Configures the logical size ceiling for a single top-level entry
	 * referenced during periodic root sync (see {@link
	 * #maxOutboundReferenceSize}'s own doc for why this exists).
	 */
	public void setMaxOutboundReferenceSize(long limit) {
		if (limit < 1) {
			throw new IllegalArgumentException("Max outbound reference size must be positive: " + limit);
		}
		this.maxOutboundReferenceSize = limit;
	}

	public long getMaxOutboundReferenceSize() {
		return maxOutboundReferenceSize;
	}

	// ========== Accessors ==========

	/**
	 * Gets the connection manager for this propagator.
	 *
	 * @return The connection manager (for adding/removing peers)
	 */
	public LatticeConnectionManager getConnectionManager() {
		return connectionManager;
	}

	/**
	 * Gets the store used by this propagator.
	 *
	 * @return The store (delta tracking + persistence + security boundary)
	 */
	public AStore getStore() {
		return store;
	}

	// ========== Peer Management ==========

	/**
	 * Adds an outbound peer connection with known identity. The peer's store
	 * is set to this propagator's store, establishing the security boundary.
	 *
	 * <p>Blocks (briefly) until the connection is actually admitted into
	 * {@code connectionManager}'s live connection set, or logs a warning and
	 * returns on failure/timeout — this method's own signature promises a
	 * void, best-effort "the peer is now usable" contract, but {@link
	 * LatticeConnectionManager#addPeer(AccountKey, Convex)} now performs a
	 * genuine identity-verification handshake (challenge/response, when this
	 * propagator has a keypair configured) before a connection is actually
	 * admitted — a caller that fires this and immediately calls {@code
	 * cursor.sync()}, expecting the peer to already be broadcast-eligible,
	 * would otherwise race the handshake and silently broadcast to nobody.
	 *
	 * @param peerKey AccountKey identifying the remote peer
	 * @param peer Convex connection to the peer node
	 */
	public void addPeer(AccountKey peerKey, Convex peer) {
		try {
			connectionManager.addPeer(peerKey, peer).get(10, TimeUnit.SECONDS);
		} catch (Exception e) {
			log.warn("Peer admission did not complete for {}: {}", peerKey, e.getMessage());
		}
	}

	/**
	 * Removes a peer by identity, closing the connection if active.
	 *
	 * @param peerKey AccountKey of the peer to remove
	 */
	public void removePeer(AccountKey peerKey) {
		connectionManager.removePeer(peerKey);
	}

	/**
	 * Gets a snapshot of current peer connections.
	 *
	 * @return Defensive copy of the peer set
	 */
	public Set<Convex> getPeers() {
		return connectionManager.getPeers();
	}

	/**
	 * Restores the last persisted value from this propagator's store.
	 * The restored value also becomes this propagator's working view, ready for
	 * directional reconciliation with the next primary projection.
	 *
	 * @return The restored value, or null if no persisted value exists
	 *         or the store is not persistent
	 */
	public ACell restore() {
		if (!store.isPersistent()) return null;
		try {
			ACell restored = store.getRootData();
			if (restored != null) {
				synchronized (writeLock) {
					workingCursor = Cursors.createLattice(lattice, restored, mergeContext);
				}
			}
			return restored;
		} catch (IOException e) {
			log.warn("Error restoring lattice value from store", e);
			return null;
		}
	}

	public boolean isRunning() { return running; }
	public long getBroadcastCount() { return broadcastCount.get(); }
	public ACell getLastAnnouncedValue() { return announcedCursor.get(); }

	/**
	 * Future completing with the next value announced by this propagator.
	 *
	 * <p>Gives callers something to wait on for propagation: capture the future
	 * <em>before</em> triggering the change, then {@code get(timeout)} — no
	 * sleep-polling on {@link #getLastAnnouncedValue()} required. Each announce
	 * completes the current future and installs a fresh one, so the returned
	 * future always reflects an announce that happens after the call.
	 *
	 * @return Future for the next announced (store-backed) value
	 */
	public CompletableFuture<ACell> nextAnnounce() { return nextAnnounceFuture; }

	/**
	 * Future for the next announce. Swapped under {@link #writeLock} in
	 * {@link #processSnapshot}, completed outside it (dependent actions must
	 * not run while holding the pipeline lock).
	 */
	private volatile CompletableFuture<ACell> nextAnnounceFuture = new CompletableFuture<>();
	/**
	 * Cursor holding the last value announced by this propagator. See
	 * {@link #announcedCursor} for ownership and security semantics.
	 */
	public Root<ACell> getAnnouncedCursor() { return announcedCursor; }
	public long getLastBroadcastTime() { return lastBroadcastTime; }
	public long getLastRootSyncTime() { return lastRootSyncTime; }
	public long getRootSyncCount() { return rootSyncCount; }

	// ========== Lifecycle ==========

	/**
	 * Starts the propagation thread.
	 */
	public synchronized void start() {
		if (running) {
			log.warn("LatticePropagator already running");
			return;
		}

		running = true;
		lastBroadcastTime = 0L;
		lastRootSyncTime = 0L;
		rootSyncState.clear();
		broadcastCount.set(0L);

		propagationThread = new Thread(this::propagationLoop, "Lattice propagator thread");
		propagationThread.setDaemon(true);
		propagationThread.start();

		log.debug("LatticePropagator started");
	}

	/**
	 * Triggers a final value and shuts down gracefully.
	 *
	 * <p>The propagator processes any remaining queued values (including the
	 * final value if non-null) before stopping. This is the only blocking
	 * handoff in the system — used during shutdown to guarantee persistence.
	 *
	 * @param finalValue Final value to process before stopping, or null
	 */
	public void triggerAndClose(ACell finalValue) {
		if (!running && propagationThread == null) return;

		running = false;

		if (finalValue != null) {
			triggerQueue.offer(finalValue); // wakes thread via notify
		} else if (propagationThread != null) {
			propagationThread.interrupt(); // wake thread from poll wait
		}

		if (propagationThread != null) {
			try {
				propagationThread.join(10_000);
				if (propagationThread.isAlive()) {
					log.warn("LatticePropagator thread did not drain within timeout, interrupting");
					propagationThread.interrupt();
					propagationThread.join(2000);
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			propagationThread = null;
		}

		// Drain any values the loop did not consume. The loop's exit check
		// (running || !queue.isEmpty()) can observe running==false before the
		// final value above lands in the queue, and exit without processing it —
		// which would silently lose the last writes on a clean shutdown.
		// processSnapshot is callable from any thread (serialised by writeLock),
		// so this drain is safe even if the thread had to be abandoned after the
		// join timeout.
		ACell remaining;
		while ((remaining = triggerQueue.poll()) != null) {
			processSnapshotSafe(remaining);
		}

		log.debug("LatticePropagator closed (sent {} delta broadcasts, {} root syncs)",
			broadcastCount, rootSyncCount);
	}

	/**
	 * Stops the propagator gracefully. Equivalent to {@code triggerAndClose(null)}.
	 */
	@Override
	public void close() {
		triggerAndClose(null);
	}

	// ========== Trigger API ==========

	/**
	 * Triggers processing of the given lattice value.
	 *
	 * <p>Non-blocking: the value is queued and processed by the background thread.
	 * Uses LatestUpdateQueue which automatically coalesces rapid triggers —
	 * safe because lattice values are monotonic (V2 >= V1 implies V1 is subsumed).
	 *
	 * @param value The lattice value to process (must not be null)
	 */
	public void triggerBroadcast(ACell value) {
		if (!running) return;
		if (value == null) return;
		triggerQueue.offer(value);
	}

	/**
	 * Stages an accepted inbound value in this propagator's own working view before
	 * the authoritative root is fanned back out. Current view state is the
	 * directional merge's {@code own} value. Inbound state remains complete here;
	 * this propagator's filter applies only when the reconciled view is published.
	 *
	 * <p>No store publication occurs here. NodeServer publishes the authoritative
	 * root synchronously, then its normal fan-out causes this propagator to reconcile
	 * and announce the resulting subset.</p>
	 */
	ACell mergeInbound(ACell[] path, ACell value) {
		synchronized (writeLock) {
			if (lattice == null) return workingCursor == null ? null : workingCursor.get();
			if (workingCursor == null) {
				ACell zero = lattice.zero();
				workingCursor = Cursors.createLattice(lattice, zero, mergeContext);
			}

			// Work on a fork so a rejecting lattice leaves the working view unchanged.
			ALatticeCursor<ACell> staged = workingCursor.fork();
			staged.path(path).merge(value);
			ACell merged = staged.get();
			workingCursor.set(merged);
			return merged;
		}
	}

	// ========== Propagation Loop ==========

	/**
	 * Main propagation loop. Processes values from the trigger queue through
	 * the full output pipeline: announce, setRootData, broadcast.
	 *
	 * <p>When {@code running} is false, switches to drain mode: processes
	 * remaining queued values without waiting, then exits.
	 */
	private void propagationLoop() {
		while (running || !triggerQueue.isEmpty()) {
			try {
				ACell value;
				if (running) {
					value = triggerQueue.poll(ROOT_SYNC_INTERVAL, TimeUnit.MILLISECONDS);
				} else {
					// Drain mode: non-blocking poll, exit when empty
					value = triggerQueue.poll();
					if (value == null) break;
				}

				if (value != null) {
					processSnapshotSafe(value);
				}

				// Periodic root sync only while running
				if (running) {
					maybePerformRootSync(Utils.getCurrentTimestamp());
				}

			} catch (InterruptedException e) {
				// Drain remaining items before exiting
				ACell remaining;
				while ((remaining = triggerQueue.poll()) != null) {
					processSnapshotSafe(remaining);
				}
				break;
			} catch (Exception e) {
				log.warn("Unexpected error in propagation loop", e);
				if (!running) break;
			}
		}
		log.debug("LatticePropagator loop ended");
	}

	/**
	 * Background-thread wrapper around {@link #processSnapshot}. IOException
	 * is logged rather than propagated — the background path is best-effort.
	 */
	private void processSnapshotSafe(ACell value) {
		try {
			processSnapshot(value);
		} catch (IOException e) {
			log.warn("Error processing lattice value", e);
		}
	}

	/**
	 * Processes a single lattice value through the full output pipeline:
	 * <ol>
	 *   <li>Announce to store — writes cells, collects novelty for delta encoding</li>
	 *   <li>Publish root data — anchor for restore (if persist enabled)</li>
	 *   <li>Broadcast delta to peers (if peers exist)</li>
	 * </ol>
	 *
	 * <p>Announce always runs (for delta tracking and store-backed refs).
	 * setRootData is gated by the persistence setting. Broadcast is gated by
	 * peer existence and minimum delay. The returned value is the sole handoff
	 * back to a synchronous caller; the pull merge callback is not invoked.
	 *
	 * <p>Callable from any thread. The background propagation loop calls this
	 * for queued triggers; for synchronous publication, NodeServer's sync callback
	 * calls this directly on the caller's thread for the primary propagator.
	 * Pipelines are serialised by {@link #writeLock} — see field javadoc for
	 * the sole-writer invariant.
	 *
	 * @param value Snapshot to process (must not be null)
	 * @return The announced (store-backed) value
	 * @throws IOException If announce or root publication fails
	 */
	public ACell processSnapshot(ACell value) throws IOException {
		AnnouncedSnapshot snapshot;
		synchronized (writeLock) {
			snapshot = announceAndPersist(value);

			// 3. Broadcast to peers. Background triggers are already coalesced by
			// LatestUpdateQueue; an explicitly processed snapshot must not be dropped.
			if (snapshot.hasPeers()) {
				try {
					broadcastToPeers(snapshot.value(), snapshot.novelty());
				} catch (RuntimeException e) {
					log.warn("Unable to encode or queue lattice delta; periodic root sync will retry",e);
				}
			}
		}
		return snapshot.value();
	}

	/**
	 * Result of the announce+persist portion of {@link #processSnapshot}'s
	 * pipeline (its steps 1-2) — the announced, store-backed value; whether
	 * any peers are currently connected (so a caller knows whether
	 * broadcasting is even meaningful); and the collected novelty (cells
	 * newly announced this call), needed to build a delta for broadcast.
	 */
	private record AnnouncedSnapshot(ACell value, boolean hasPeers, ArrayList<ACell> novelty) {}

	/**
	 * Runs steps 1-2 of {@link #processSnapshot}'s pipeline (announce to
	 * store, set root data) and completes this call's {@link #nextAnnounce()}
	 * future -- but does not broadcast. Extracted so {@link
	 * #publishWithAckTarget} can do the same local, durable announce+persist
	 * work synchronously while deferring or bounding the broadcast step
	 * separately (see that method's own doc for why).
	 *
	 * <p>Must be called with {@link #writeLock} held -- same requirement
	 * {@link #processSnapshot} itself already had for this work, now just
	 * factored out rather than inlined.
	 *
	 * @param value Snapshot to process (must not be null)
	 * @return The announced snapshot plus peer/novelty info for broadcast
	 * @throws IOException If announce or root publication fails
	 */
	private AnnouncedSnapshot announceAndPersist(ACell value) throws IOException {
		// Primary input is already authoritative. A secondary instead keeps its
		// established view as own, preserving local refs and pending inbound values.
		if ((workingCursor != null) && !primary && (lattice != null)) {
			value = lattice.merge(mergeContext, workingCursor.get(), value);
		}

		// Filtering is outbound-only: pending inbound state participates in the
		// reconciliation above, then projection precedes every outbound operation.
		value = filter.filter(value);
		if (value == null) throw new IllegalArgumentException("Lattice filter returned null");

		// 1. Announce to store (writes cells, collects novelty for delta)
		boolean hasPeers=!connectionManager.getPeers().isEmpty();
		Cells.NoveltyCollector noveltyCollector=hasPeers
			?new Cells.NoveltyCollector(maxDeltaBroadcastSize):null;
		value = Cells.announce(value, noveltyCollector, store);
		if (workingCursor == null) {
			workingCursor = Cursors.createLattice(lattice, value, mergeContext);
		} else {
			workingCursor.set(value);
		}

		// 2. Set root data for restore (if persist enabled)
		if (persistenceEnabled) {
			store.setRootData(value);
			dirty = true;
		}

		announcedCursor.set(value);
		CompletableFuture<ACell> announceFuture = nextAnnounceFuture;
		nextAnnounceFuture = new CompletableFuture<>();
		announceFuture.complete(value);

		ArrayList<ACell> novelty = hasPeers ? noveltyCollector.getCells() : null;
		return new AnnouncedSnapshot(value, hasPeers, novelty);
	}

	/**
	 * Publishes a snapshot the same way {@link #processSnapshot} does for
	 * its announce+persist steps (synchronous, local, durable -- unchanged),
	 * but bounds how much of the broadcast step this call actually waits
	 * for, rather than always blocking the caller until every connected
	 * peer's send has been attempted.
	 *
	 * <p>Built to let a synchronous, client-facing caller (e.g. dbase's own
	 * {@code ConvexMeta.syncIfAutoCommit}) choose a durability/latency
	 * tradeoff per call, instead of always paying full ambient-broadcast
	 * latency for every write regardless of whether the caller needed that
	 * guarantee. Mirrors how Convex's own CPoS consensus layer never blocks
	 * a processing thread on peer network I/O -- the difference here is
	 * this method lets the caller opt back into waiting, up to a bound,
	 * rather than always reporting success only once a background loop
	 * reports finality.
	 *
	 * <p>{@code minAcks <= 0}: fire the broadcast and return immediately
	 * once locally persisted -- the broadcast itself is still attempted
	 * (queued via the same {@link #triggerBroadcast} mechanism already used
	 * for secondary propagators, so it still runs promptly on this
	 * propagator's own background thread), but this call never waits on it.
	 *
	 * <p>{@code minAcks > 0}: waits (outside any lock -- see below) until at
	 * least {@code min(minAcks, connectedPeerCount)} peers have returned a
	 * response for this specific broadcast, or {@code timeoutMs} elapses,
	 * whichever comes first. "Responded" means the peer's own {@code
	 * NodeServer.processLatticeValue} handler completed and replied -- a
	 * rejected merge counts as a response (the peer definitely processed
	 * this delta, even if it declined to apply it), a hung or unreachable
	 * peer does not. A peer count below what's connected right now is not
	 * an error: {@code minAcks} is a request, not a guarantee, and this
	 * method never throws on a timeout -- it returns the announced value
	 * regardless, matching this class's established best-effort tolerance
	 * for broadcast-step failures elsewhere.
	 *
	 * <p>Only tracks acks for peers reachable via the single-message delta
	 * path (the ordinary case for an SQL-statement-sized write). A delta too
	 * large to fit in one message falls back to the untracked, chunked
	 * {@link #sendBoundedDelta} path for those peers -- correct (never a
	 * false-positive ack), just not counted, so a caller asking for more
	 * acks than can be tracked simply times out rather than getting a
	 * dishonest result.
	 *
	 * @param value Snapshot to publish (must not be null)
	 * @param minAcks Minimum number of peer responses to wait for (0 = don't wait at all)
	 * @param timeoutMs Maximum time to wait for those responses
	 * @return The announced (store-backed) value
	 * @throws IOException If announce or root publication fails
	 */
	public ACell publishWithAckTarget(ACell value, int minAcks, long timeoutMs) throws IOException {
		AnnouncedSnapshot snapshot;
		List<CompletableFuture<Result>> ackFutures = List.of();
		synchronized (writeLock) {
			snapshot = announceAndPersist(value);
			if (snapshot.hasPeers()) {
				if (minAcks > 0) {
					ackFutures = broadcastToPeersWithAcks(snapshot.value(), snapshot.novelty());
				} else {
					// No caller is waiting: send the same way processSnapshot's
					// own default path already does -- synchronous,
					// non-blocking trySend, still inside this lock acquisition.
					//
					// Deliberately NOT triggerBroadcast() (an earlier version
					// of this method used it): that defers the send to the
					// background propagation thread, which reprocesses the
					// value through a *second*, redundant announceAndPersist
					// (real disk persist, not free) and needs this same
					// writeLock to do it. Under a tight back-to-back write
					// loop, that pits the foreground thread (persisting row
					// N+1) against the background thread (re-persisting row N
					// before it can finally broadcast) for the same lock --
					// found live 2026-08-24: acks=0 measured *slower* than
					// acks=2 because of exactly this self-inflicted
					// contention, despite acks=2 doing strictly more work
					// (an actual wait for real peer responses).
					try {
						broadcastToPeers(snapshot.value(), snapshot.novelty());
					} catch (RuntimeException e) {
						log.warn("Unable to encode or queue lattice delta; periodic root sync will retry",e);
					}
				}
			}
		}
		if (minAcks > 0 && !ackFutures.isEmpty()) {
			awaitAcks(ackFutures, minAcks, timeoutMs);
		}
		return snapshot.value();
	}

	/**
	 * Waits for at least {@code minAcks} of the given futures to complete
	 * (successfully or exceptionally -- either way, the peer responded),
	 * capped at {@code futures.size()}, or until {@code timeoutMs} elapses.
	 * Never throws on timeout -- see {@link #publishWithAckTarget}'s own doc
	 * for why a caller asking for more acks than arrive in time still gets
	 * its announced value back rather than an error.
	 */
	private void awaitAcks(List<CompletableFuture<Result>> futures, int minAcks, long timeoutMs) {
		int target = Math.min(minAcks, futures.size());
		if (target <= 0) return;
		java.util.concurrent.atomic.AtomicInteger responded = new java.util.concurrent.atomic.AtomicInteger();
		CompletableFuture<Void> reached = new CompletableFuture<>();
		for (CompletableFuture<Result> f : futures) {
			f.whenComplete((r, e) -> {
				if (responded.incrementAndGet() >= target) reached.complete(null);
			});
		}
		try {
			reached.get(timeoutMs, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			log.debug("publishWithAckTarget: only {}/{} peers responded within {}ms",
				responded.get(), target, timeoutMs);
		}
	}

	// ========== Broadcast Scoping ==========

	/** Path array denoting the lattice root, for peers with no declared scope. */
	private static final ACell[] ROOT_PATH = new ACell[0];

	/**
	 * Sends a snapshot to every connected peer, honouring each peer's declared
	 * broadcast scope (see {@link LatticeConnectionManager#getPeerScope}).
	 *
	 * <p>A peer with no declared scope receives the full root, as one or more
	 * size-bounded delta messages (see {@link #sendBoundedDelta}) — exactly
	 * the behaviour every peer got before per-peer scoping existed, now also
	 * chunked/bounded the same way upstream's own {@code broadcastDelta} is
	 * (DATA-ahead chunks plus a trailing root announcement when a delta
	 * exceeds {@link #maxDeltaMessageSize}), instead of the old single
	 * unbounded {@code Format.encodeDelta} call. A peer with one or more
	 * declared scope paths only ever receives its own subtree(s), each as a
	 * small indirect-ref message (see {@link #sendScopedUpdate}) — this is
	 * what stops an unrelated region of the lattice growing large from ever
	 * being pushed to, or acquired by, a peer that never asked for it (the
	 * root cause of #611: an oversized, unrelated schema tripped every
	 * peer's inbound size limit because the ambient broadcast path always
	 * targeted the full root, regardless of what a given peer had actually
	 * pulled). Scoping itself has no upstream equivalent — upstream's own
	 * {@code broadcastDelta} always targets every connection uniformly via
	 * {@code AConnectionManager.broadcastSequence}, which can't express "send
	 * different content to different peers"; {@link #sendBoundedDelta} below
	 * replicates just enough of that method's per-connection send loop,
	 * scoped to the subset of peers that actually need the full message.
	 *
	 * @param value Full current root snapshot (store-backed)
	 * @param novelty Cells newly announced this call — consumed to build the
	 *                full-root delta, only when at least one peer needs it
	 */
	private void broadcastToPeers(ACell value, ArrayList<ACell> novelty) {
		Map<AccountKey, Convex> peers = connectionManager.getConnections();

		List<Convex> unscoped = new ArrayList<>();
		for (Map.Entry<AccountKey, Convex> entry : peers.entrySet()) {
			Convex peerConnection = entry.getValue();
			if (peerConnection == null || !peerConnection.isConnected()) continue;

			List<ACell[]> scope = connectionManager.getPeerScope(entry.getKey());
			if (scope.isEmpty()) {
				unscoped.add(peerConnection);
			} else {
				for (ACell[] path : scope) {
					ACell subValue = RT.getIn(value, path);
					if (subValue != null) sendScopedUpdate(peerConnection, path, subValue);
				}
			}
		}
		if (!unscoped.isEmpty()) {
			sendBoundedDelta(unscoped, value, novelty);
		}
	}

	/**
	 * Sends one bounded delta, or DATA-ahead chunks followed by a root
	 * announcement, to exactly the given (already-connected, unscoped)
	 * peers — adapted from upstream's own {@code broadcastDelta}, scoped to
	 * a peer subset instead of every connection (see {@link
	 * #broadcastToPeers}'s own doc for why this dbase-fork-only scoping
	 * can't just call {@code AConnectionManager.broadcastSequence} directly).
	 * The per-peer send loop below mirrors that method's own semantics
	 * (each peer gets the full ordered sequence, or the trailing root
	 * message as a fallback if the sequence doesn't fully enqueue).
	 */
	private void sendBoundedDelta(List<Convex> targets, ACell value, ArrayList<ACell> novelty) {
		// Embedded cells already travel inside their nearest non-embedded parent and
		// are invalid as trailing multi-cell children.
		novelty.removeIf(ACell::isEmbedded);
		if (!value.isEmbedded()
				&& (novelty.isEmpty() || !novelty.get(novelty.size() - 1).equals(value))) {
			novelty.add(value);
		}

		AVector<ACell> emptyPath = Vectors.empty();
		AVector<?> payload = Vectors.create(MessageTag.LATTICE_VALUE, null, emptyPath, value);
		Message rootMessage = Message.create(MessageType.LATTICE_VALUE, payload, payload.getEncoding());
		if (rootMessage.getMessageData().count()>maxDeltaMessageSize) {
			log.warn("Lattice root announcement exceeds delta message limit of {} bytes; root sync will recover",
				maxDeltaMessageSize);
			return;
		}
		ArrayList<ACell> delta = new ArrayList<>(novelty.size()+1);
		delta.addAll(novelty);
		delta.add(payload);

		List<Message> messages;
		if (Format.getDeltaEncodingLength(delta)<=maxDeltaMessageSize) {
			Blob deltaData=Format.encodeDelta(delta,maxDeltaMessageSize);
			messages=List.of(Message.create(MessageType.LATTICE_VALUE,payload,deltaData));
		} else {
			try {
				long dataBudget=maxDeltaBroadcastSize-rootMessage.getMessageData().count();
				messages=(dataBudget>0)
					?new ArrayList<>(Message.createDataMessages(
						novelty,maxDeltaMessageSize,dataBudget))
					:new ArrayList<>();
				messages.add(rootMessage);
			} catch (IllegalArgumentException e) {
				// A single non-embedded cell may exceed an application-selected chunk
				// limit. Announce the root only and let the receiver pull that branch.
				messages=List.of(rootMessage);
			}
		}

		int dropped=0;
		for (Convex peerConnection : targets) {
			boolean sent=true;
			for (Message message:messages) {
				if (!peerConnection.trySend(message)) {
					sent=false;
					break;
				}
			}
			if (!sent && !peerConnection.trySend(rootMessage)) dropped++;
		}
		if (dropped>0) {
			log.debug("Dropped lattice delta for {} peer(s); root sync will recover",dropped);
		}
		lastBroadcastTime = Utils.getCurrentTimestamp();
		broadcastCount.incrementAndGet();
	}

	/**
	 * Sends one peer a small, path-scoped update, encoded as an indirect ref
	 * (a hash-only reference inside the envelope) rather than an inlined
	 * delta — the receiver's own DATA_REQUEST/Acquiror machinery pulls
	 * whatever it's actually missing under this path, exactly as the initial
	 * scoped {@link #pullPath} already relies on. A null {@code subValue} is
	 * never sent: {@code NodeServer.processLatticeValue} rejects a
	 * LATTICE_VALUE with a missing value outright (and repeated rejects trip
	 * the peer's consecutive-reject circuit breaker), so a path this node has
	 * nothing for yet is silently skipped rather than sent as a reject-bound
	 * no-op.
	 *
	 * @param peerConnection Live connection to send on
	 * @param path Lattice path this update is scoped to (empty = root)
	 * @param subValue Value at that path (must not be null)
	 */
	private void sendScopedUpdate(Convex peerConnection, ACell[] path, ACell subValue) {
		// 4-element payload (tag, reserved, path, value) -- matches the format
		// every other LATTICE_VALUE sender in this class and NodeServer's own
		// receiving-side validation now use post-merge (upstream's own
		// senders always pass null for the reserved slot; NodeServer's
		// processLatticeValue/acquireLatticeMessage never read it).
		AVector<ACell> pathVector = (path.length == 0) ? Vectors.empty() : Vectors.of((Object[]) path);
		AVector<?> payload = Vectors.create(MessageTag.LATTICE_VALUE, null, pathVector, subValue);
		Blob envelopeData = payload.getEncoding();
		Message message = Message.create(MessageType.LATTICE_VALUE, payload, envelopeData);
		peerConnection.trySend(message);
	}

	/**
	 * Ack-tracked counterpart to {@link #broadcastToPeers}, for {@link
	 * #publishWithAckTarget}. Sends the identical scoped/unscoped delta to
	 * every connected peer, but via {@link Convex#requestNonBlocking(Message)}
	 * instead of {@link Convex#trySend(Message)} so each send's completion
	 * (success or peer-side rejection -- either way, a response) is
	 * observable.
	 *
	 * <p><b>Must use {@code requestNonBlocking}, not the ordinary {@link
	 * Convex#request(Message)}</b> -- found live: {@code request()}'s send
	 * path uses {@code AConnection.sendMessage}, documented to block with a
	 * bounded timeout under backpressure, and this method runs inside
	 * {@link #writeLock} (see {@link #publishWithAckTarget}'s own call
	 * site). A blocking send there doesn't just slow this one write down --
	 * it holds the sole-writer lock, stalling every other write on this
	 * propagator for as long as the block lasts. Reproduced live under a
	 * tight single-row insert benchmark loop with {@code SET WRITE_ACKS=2}:
	 * the run never completed. {@code requestNonBlocking} uses {@code
	 * AConnection.trySendMessage} instead (guaranteed non-blocking,
	 * documented as such), matching every other send in this class.
	 *
	 * <p>Reuses the same 4-element {@code [tag, reserved-slot, path, value]}
	 * LATTICE_VALUE payload shape every other sender in this class uses --
	 * {@link Message#withID} fills the reserved slot with the correlation
	 * ID {@code requestNonBlocking()} allocates, and {@code
	 * NodeServer.processLatticeValue} already replies once that ID is
	 * present (see its own doc: "completion is the acknowledgement"). No
	 * wire-format change was needed for this.
	 *
	 * <p>Only tracks the single-message case (the ordinary size for an
	 * SQL-statement-sized write) for unscoped peers -- a delta too large to
	 * fit in one message falls back to the untracked, chunked {@link
	 * #sendBoundedDelta} for those peers specifically. Correct either way
	 * (never a false-positive ack), just not counted; see {@link
	 * #publishWithAckTarget}'s own doc for why that's the right failure
	 * mode. Scoped peers are always tracked -- their update is always one
	 * small indirect-ref message, never chunked.
	 *
	 * @param value Full current root snapshot (store-backed)
	 * @param novelty Cells newly announced this call
	 * @return One future per peer this call could track
	 */
	private List<CompletableFuture<Result>> broadcastToPeersWithAcks(ACell value, ArrayList<ACell> novelty) {
		Map<AccountKey, Convex> peers = connectionManager.getConnections();
		List<CompletableFuture<Result>> acks = new ArrayList<>();
		List<Convex> unscoped = new ArrayList<>();

		for (Map.Entry<AccountKey, Convex> entry : peers.entrySet()) {
			Convex peerConnection = entry.getValue();
			if (peerConnection == null || !peerConnection.isConnected()) continue;

			List<ACell[]> scope = connectionManager.getPeerScope(entry.getKey());
			if (scope.isEmpty()) {
				unscoped.add(peerConnection);
			} else {
				for (ACell[] path : scope) {
					ACell subValue = RT.getIn(value, path);
					if (subValue == null) continue;
					AVector<ACell> pathVector = (path.length == 0) ? Vectors.empty() : Vectors.of((Object[]) path);
					AVector<?> scopedPayload = Vectors.create(MessageTag.LATTICE_VALUE, null, pathVector, subValue);
					Message message = Message.create(MessageType.LATTICE_VALUE, scopedPayload, scopedPayload.getEncoding());
					acks.add(peerConnection.requestNonBlocking(message));
				}
			}
		}

		if (unscoped.isEmpty()) return acks;

		novelty.removeIf(ACell::isEmbedded);
		if (!value.isEmbedded()
				&& (novelty.isEmpty() || !novelty.get(novelty.size() - 1).equals(value))) {
			novelty.add(value);
		}
		AVector<ACell> emptyPath = Vectors.empty();
		AVector<?> payload = Vectors.create(MessageTag.LATTICE_VALUE, null, emptyPath, value);
		ArrayList<ACell> delta = new ArrayList<>(novelty.size()+1);
		delta.addAll(novelty);
		delta.add(payload);

		if (Format.getDeltaEncodingLength(delta) <= maxDeltaMessageSize) {
			Blob deltaData = Format.encodeDelta(delta, maxDeltaMessageSize);
			Message message = Message.create(MessageType.LATTICE_VALUE, payload, deltaData);
			for (Convex peerConnection : unscoped) {
				acks.add(peerConnection.requestNonBlocking(message));
			}
			lastBroadcastTime = Utils.getCurrentTimestamp();
			broadcastCount.incrementAndGet();
		} else {
			// Too large to track acks for this call -- fall back to the
			// ordinary chunked, untracked broadcast. novelty here is already
			// preprocessed (embedded cells stripped, value appended);
			// sendBoundedDelta's own identical preprocessing is idempotent
			// against that, so passing it through is safe.
			sendBoundedDelta(unscoped, value, novelty);
		}
		return acks;
	}

	// ========== Root Sync ==========

	/**
	 * Performs periodic root-only sync broadcast for divergence detection,
	 * honouring each peer's declared broadcast scope exactly as {@link
	 * #broadcastToPeers} does for trigger-driven broadcasts. Reads the last
	 * announced value directly (rather than taking it as a parameter, as
	 * this used to) to match {@link #createRootSyncMessage}'s own "use
	 * whatever is currently store-backed" contract, which
	 * {@code LatticePropagatorTest} exercises directly.
	 *
	 * <p>Each connected peer is scheduled independently via {@link
	 * #rootSyncState} -- called every propagation-loop cycle, but a given
	 * peer is only actually attempted once its own {@link
	 * PeerSyncState#dueFor} fires, so a peer that just failed backs off
	 * (see that class's own doc) without affecting any other peer's
	 * ordinary {@link #ROOT_SYNC_INTERVAL} cadence. An unscoped peer's
	 * sync is decomposed per top-level entry rather than sent as one
	 * whole-root reference (see {@link #sendUnscopedRootSync}).
	 *
	 * <p>Package-visible for testing (same convention as {@code
	 * NodeServer.withinInboundSizeLimit}) -- lets a test drive a sync
	 * attempt directly instead of waiting a real {@link #ROOT_SYNC_INTERVAL}.
	 */
	void maybePerformRootSync(long currentTime) {
		if (connectionManager.getPeers().isEmpty()) return;

		ACell value = announcedCursor.get();
		if (value == null) return;

		try {
			int sent = 0;
			boolean anyAttempted = false;
			for (Map.Entry<AccountKey, Convex> entry : connectionManager.getConnections().entrySet()) {
				AccountKey peerKey = entry.getKey();
				Convex peerConnection = entry.getValue();
				if (peerConnection == null || !peerConnection.isConnected()) continue;

				PeerSyncState state = rootSyncState.computeIfAbsent(peerKey, k -> new PeerSyncState());
				if (!state.dueFor(currentTime)) continue;
				anyAttempted = true;

				List<ACell[]> scope = connectionManager.getPeerScope(peerKey);
				boolean ok;
				if (scope.isEmpty()) {
					ok = sendUnscopedRootSync(peerConnection, value);
					sent++;
				} else {
					ok = true;
					for (ACell[] path : scope) {
						ACell subValue = RT.getIn(value, path);
						if (subValue != null) {
							ok &= sendScopedUpdateTracked(peerConnection, path, subValue);
							sent++;
						}
					}
				}
				if (ok) state.recordSuccess(currentTime);
				else state.recordFailure(currentTime);
			}
			if (anyAttempted) {
				lastRootSyncTime = currentTime;
				rootSyncCount++;
				log.debug("Sent root sync to {} peer path(s)", sent);
			}
		} catch (Exception e) {
			log.warn("Error during root sync broadcast", e);
		}
	}

	/**
	 * Sends a periodic root sync to an unscoped peer -- one wanting
	 * everything, as opposed to a peer with a declared narrow scope (see
	 * {@link #sendScopedUpdateTracked}, used directly for those).
	 *
	 * <p>Found live 2026-08-25: referencing the entire root as a single
	 * value (the original behaviour) meant this method's own inbound-size
	 * guard on the receiving end (see {@code NodeServer.withinInboundSizeLimit})
	 * judged the sync by the total size of <em>everything</em> this node
	 * hosts, not by how much had actually changed since the peer's last
	 * successful sync -- once accumulated data across every database
	 * exceeded the receiver's inbound limit, every single periodic sync to
	 * that peer failed, forever, with no way to self-resolve (the size only
	 * grows). If the root is a map (true for every real dbase deployment --
	 * {@code ConvexDB.DATABASE_MAP_LATTICE} keys by database name), this
	 * decomposes it into one reference per top-level entry instead of one
	 * reference to the whole thing -- each individual database's own size
	 * is what gets checked, not the fleet-wide total, and an unchanged
	 * database's reference is now recognised as already fully possessed
	 * (see that same guard's companion fix) and never rejected for size at
	 * all.
	 *
	 * <p>A single database that has itself grown past {@link
	 * #maxOutboundReferenceSize} is never referenced at all -- found live
	 * 2026-08-25: sending the reference anyway, relying entirely on the
	 * receiver's own post-acquisition {@code withinInboundSizeLimit} guard,
	 * meant the receiver still had to walk its full {@code Acquiror}/
	 * DATA_REQUEST acquisition path for an 11MB+ value before ever reaching
	 * that check -- repeated every sync interval, this many-message,
	 * many-times-a-second exchange coincided with a still-not-fully-
	 * root-caused decode/framing corruption observed live on freshly
	 * (re)established connections. Checking size here, before referencing
	 * the data at all, means that acquisition attempt is never made in the
	 * first place. See {@link #maxOutboundReferenceSize}'s own doc.
	 *
	 * @return true if every top-level entry (or the whole value, if not
	 *         decomposable) was accepted by the peer or already known too
	 *         large to send
	 */
	private boolean sendUnscopedRootSync(Convex peerConnection, ACell value) {
		if (value instanceof AMap<?, ?> map) {
			boolean allOk = true;
			for (Map.Entry<?, ?> e : map.entrySet()) {
				ACell key = (ACell) e.getKey();
				ACell subValue = (ACell) e.getValue();
				if (subValue == null) continue;
				if (ACell.getMemorySize(subValue) > maxOutboundReferenceSize) {
					log.debug("Skipping periodic root sync of {} to {}: {} bytes exceeds outbound reference limit of {}",
						key, peerConnection, ACell.getMemorySize(subValue), maxOutboundReferenceSize);
					allOk = false;
					continue;
				}
				allOk &= sendScopedUpdateTracked(peerConnection, new ACell[]{key}, subValue);
			}
			return allOk;
		}
		return sendScopedUpdateTracked(peerConnection, ROOT_PATH, value);
	}

	/**
	 * Ack-tracked counterpart to {@link #sendScopedUpdate}, for root sync's
	 * per-peer success/failure tracking (see {@link PeerSyncState}). Same
	 * indirect-ref wire shape (the receiver's own DATA_REQUEST/Acquiror
	 * machinery resolves whatever it's actually missing), but sent via
	 * {@link Convex#requestNonBlocking(Message)} (non-blocking send, same
	 * reasoning as {@link #broadcastToPeersWithAcks}'s own doc -- this
	 * runs on the propagator's single background thread, which must not
	 * block on network I/O) and briefly, boundedly waited on -- acceptable
	 * here specifically because this background thread has no client
	 * connection depending on it, unlike a synchronous client-facing
	 * publish.
	 *
	 * @return true if the peer responded without error inside {@link
	 *         #ROOT_SYNC_ACK_TIMEOUT_MS}; false on any error or timeout
	 */
	private boolean sendScopedUpdateTracked(Convex peerConnection, ACell[] path, ACell subValue) {
		AVector<ACell> pathVector = (path.length == 0) ? Vectors.empty() : Vectors.of((Object[]) path);
		AVector<?> payload = Vectors.create(MessageTag.LATTICE_VALUE, null, pathVector, subValue);
		Message message = Message.create(MessageType.LATTICE_VALUE, payload, payload.getEncoding());
		try {
			Result result = peerConnection.requestNonBlocking(message)
				.get(ROOT_SYNC_ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
			return result != null && !result.isError();
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * Creates a root sync for the last value successfully announced to this
	 * propagator's serving store. A triggered value is deliberately not externally
	 * visible until {@link Cells#announce(ACell, Consumer, AStore)} has completed.
	 */
	Message createRootSyncMessage() {
		ACell value = announcedCursor.get();
		if (value == null) return null;
		AVector<ACell> emptyPath = Vectors.empty();
		AVector<?> payload = Vectors.create(MessageTag.LATTICE_VALUE, null, emptyPath, value);
		// The value may be encoded as an indirect ref; DATA_REQUEST resolution is
		// safe because announcedCursor advances only after the store is populated.
		return Message.create(MessageType.LATTICE_VALUE, payload, payload.getEncoding());
	}

	// ========== Explicit Persistence ==========

	/**
	 * Explicitly persists a value to the store. Used for forced persistence
	 * (e.g. {@link NodeServer#persistSnapshot}) regardless of the automatic
	 * root-publication setting.
	 *
	 * @param value The value to persist
	 * @throws IOException If persistence or its durability barrier fails
	 */
	void persist(ACell value) throws IOException {
		if (value == null) return;
		if (!store.isPersistent()) return;
		synchronized (writeLock) {
			value = Cells.announce(value, r -> {}, store);
			store.setRootData(value);
			dirty = true;
			store.flush();
			dirty = false;
			log.debug("Persisted lattice snapshot to store");
		}
	}

	/**
	 * Completes a durability barrier when this propagator has unpublished
	 * persistent changes.
	 *
	 * @return true if a barrier was completed
	 * @throws IOException If the barrier fails
	 */
	boolean checkpoint() throws IOException {
		if (!store.isPersistent()) return false;
		synchronized (writeLock) {
			if (!dirty) return false;
			store.flush();
			dirty = false;
			return true;
		}
	}

	// ========== Pull (Fetch from Peers) ==========

	/**
	 * Pulls the latest lattice value from a specific peer into this propagator's store.
	 *
	 * <p>Sends a LATTICE_QUERY to the peer, acquires the full value tree into
	 * this propagator's store via {@link Convex#acquire}, then returns that
	 * store-backed value to the caller.
	 *
	 * <p>This method deliberately performs no merge, root publication or broadcast.
	 * Only NodeServer knows the lattice and current root, so it must merge the acquired
	 * value and call {@code cursor.sync()} before anything is re-propagated. Persisting
	 * the raw peer value here could demote the root when local state already dominates it.
	 *
	 * @param peer Convex connection to the peer node
	 * @return CompletableFuture that completes with the acquired value
	 */
	public CompletableFuture<ACell> pull(Convex peer) {
		return pullPath(peer);
	}

	/**
	 * Pulls one path of a peer's latest lattice value into this propagator's
	 * store. Path selection scopes transfer and storage work; it is not an
	 * access-control boundary.
	 *
	 * <p>The caller must not mutate {@code path} while the returned operation is
	 * outstanding.</p>
	 *
	 * @param peer Convex connection to the peer node
	 * @param path path within the peer's announced lattice value
	 * @return future completing with the acquired value, or {@code null} when absent
	 */
	public CompletableFuture<ACell> pullPath(Convex peer, ACell... path) {
		if (peer == null) {
			return CompletableFuture.failedFuture(new IllegalArgumentException("Peer cannot be null"));
		}

		return CompletableFuture.supplyAsync(() -> {
			try {
				if (!peer.isConnected()) {
					throw new RuntimeException("Peer is not connected");
				}

				AVector<?> queryPayload = Vectors.create(
						MessageTag.LATTICE_QUERY, null, Vectors.create(path));
				Message queryMessage = Message.create(MessageType.LATTICE_QUERY, queryPayload);

				CompletableFuture<Result> resultFuture = peer.request(queryMessage);
				Result result = resultFuture.get(10, TimeUnit.SECONDS);

				if (result.isError()) {
					throw new RuntimeException("Pull query failed: " + result);
				}

				ACell receivedValue = result.getValue();
				if (receivedValue == null) return null;

				// 2. Store the received value locally. For small values that are
				// fully encoded in the result, announce succeeds immediately.
				// For large values with missing children, fall back to acquire.
				ACell acquired;
				try {
					acquired = Cells.announce(receivedValue, r -> {}, store);
				} catch (MissingDataException mde) {
					// Value has children not in our store — acquire full tree from peer
					Hash rootHash = Hash.get(receivedValue);
					acquired = peer.acquire(rootHash, store).get(30, TimeUnit.SECONDS);
				}

				log.debug("Acquired pulled lattice path from peer: {}", peer.getHostAddress());
				return acquired;

			} catch (Exception e) {
				log.warn("Lattice pull failed from peer: {}", peer.getHostAddress(), e);
				throw new RuntimeException("Lattice pull failed from peer", e);
			}
		},ThreadUtils.getVirtualExecutor());
	}

	/**
	 * Pulls the latest lattice value from all connected peers.
	 *
	 * <p>Sends LATTICE_QUERY to each connected peer in parallel, acquires their
	 * values into this propagator's store. The returned values remain unmerged;
	 * NodeServer integrates them through its authoritative root cursor.
	 *
	 * @return future containing all acquired values when every pull is complete
	 */
	public CompletableFuture<List<ACell>> pullAll() {
		Set<Convex> peerSet = connectionManager.getPeers();
		if (peerSet.isEmpty()) {
			return CompletableFuture.completedFuture(List.of());
		}

		List<CompletableFuture<ACell>> futures = new ArrayList<>();
		for (Convex peer : peerSet) {
			if (peer != null && peer.isConnected()) {
				futures.add(pull(peer));
			}
		}

		if (futures.isEmpty()) {
			return CompletableFuture.completedFuture(List.of());
		}

		return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
			.thenApply(v -> {
				List<ACell> acquired = new ArrayList<>(futures.size());
				for (CompletableFuture<ACell> future : futures) {
					acquired.add(future.join());
				}
				return acquired;
			});
	}

	/**
	 * Pulls just a sub-path of a peer's lattice value (e.g. one entry of a
	 * keyed map) instead of its entire root, into this propagator's store.
	 *
	 * <p>Sends a LATTICE_QUERY with a real path — the server side
	 * ({@code NodeServer#processLatticeQuery}) already honours an optional
	 * path parameter; only the full-root convenience methods above ({@link
	 * #pull(Convex)}/{@link #pullAll()}) always request the whole root. This
	 * exists for applications where a peer's lattice root aggregates many
	 * independent regions (e.g. one entry per named sub-database) and a
	 * caller only wants — and is only entitled to receive — one of them,
	 * without transitively pulling everything else that peer happens to
	 * host too.
	 *
	 * <p>Like {@link #pull(Convex)}, this deliberately performs no merge,
	 * root publication or broadcast — only the caller (typically {@code
	 * NodeServer}) knows which cursor path the acquired value belongs at,
	 * and must merge it there itself before any re-propagation.
	 *
	 * @param peer Convex connection to the peer node
	 * @param path Path within the peer's lattice to fetch
	 * @return CompletableFuture completing with the value at that path (or null if absent there)
	 */
}
