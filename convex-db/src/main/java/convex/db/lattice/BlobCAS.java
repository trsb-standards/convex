package convex.db.lattice;

import java.io.IOException;

import convex.core.data.ABlobLike;
import convex.core.data.ACell;
import convex.core.data.Blob;
import convex.core.data.Cells;
import convex.core.data.Hash;
import convex.core.data.Ref;
import convex.core.data.Strings;
import convex.core.store.AStore;

/**
 * Content-addressable access to large blobs and strings that exceed the CAD3
 * inline threshold, backed by the node's own {@link AStore} (its etch file).
 *
 * <p>When a BLOB or (large) TEXT/VARCHAR column value exceeds {@link #THRESHOLD}
 * bytes, storing it inline in a Convex row causes CAD3 to split it into a
 * BlobTree/StringTree with Refs that require the etch store to decode. The
 * JDBC/Calcite layer does not have etch store access at that point in the
 * decode path, so a naive inline store would fail with "Cannot read Ref
 * without a store".
 *
 * <p>BlobCAS solves this by durably persisting large values as ordinary
 * hash-addressed cells in the node's own etch store (via {@link Cells#announce}
 * — the same mechanism {@code NodeServer.processLatticeValue} uses to persist
 * incoming lattice data) and replacing the inline cell with a compact 35-byte
 * reference: {@code [magic] + hash(cell)}, where {@code magic} is one of
 * {@link #MAGIC_BLOB} or {@link #MAGIC_STRING} depending on whether the
 * original cell was an {@code ABlob} or an {@code AString} — this lets the
 * reader reconstruct the correct CVM type. On read, refs are resolved back to
 * the original cell via {@link AStore#refForHash}.
 *
 * <p>Because the value lives in the same etch store as everything else, it is
 * a first-class, hash-addressed part of the lattice's reachable object graph
 * — ordinary replication (peer sync's {@code pullPath}/{@code Acquiror})
 * fetches it exactly like any other referenced cell, with no separate
 * replication mechanism needed. (An older revision of this class stored large
 * values as loose files in a local-only directory, sibling to the etch file —
 * that data was never replicated at all; superseded by this store-backed
 * design.)
 *
 * <p>Usage:
 * <pre>
 *   BlobCAS.init(store);  // at startup, once the node's AStore exists
 *   // ... use SQLSchema normally; large blobs/strings are handled automatically
 *   BlobCAS.shutdown();   // at shutdown
 * </pre>
 *
 * <p>Deduplication is automatic: identical content stored multiple times (even
 * under different magic, e.g. once as a blob and once as a string) resolves to
 * the same underlying hash and costs no extra storage.
 */
public class BlobCAS {

	/** Values larger than this many bytes are stored in CAS rather than inline. */
	public static final int THRESHOLD = 4096;

	/** Magic prefix for blob CAS references: 3 bytes identifying an ABlob-typed CAS ref. */
	static final byte[] MAGIC_BLOB = {0x00, (byte) 0xCA, 0x5B};

	/** Magic prefix for string CAS references: 3 bytes identifying an AString-typed CAS ref. */
	static final byte[] MAGIC_STRING = {0x00, (byte) 0xCA, 0x53};

	/** Total size of a CAS reference blob: 3 magic bytes + hash bytes. */
	static final int REF_SIZE = MAGIC_BLOB.length + Hash.LENGTH;

	private static volatile BlobCAS INSTANCE;

	private final AStore store;

	private BlobCAS(AStore store) {
		this.store = store;
	}

	// ── Lifecycle ─────────────────────────────────────────────────────────────

	/**
	 * Initialises the global BlobCAS singleton against the given store.
	 * No-op if already initialised.
	 */
	public static synchronized void init(AStore store) {
		if (INSTANCE != null) return;
		INSTANCE = new BlobCAS(store);
	}

	/** Returns the active singleton, or {@code null} if not initialised. */
	public static BlobCAS instance() {
		return INSTANCE;
	}

	/** Shuts down the singleton. */
	public static synchronized void shutdown() {
		INSTANCE = null;
	}

	// ── CAS ref detection ─────────────────────────────────────────────────────

	/**
	 * Returns true if the given Blob is a CAS reference of any kind
	 * (35 bytes, starts with a recognised magic prefix).
	 */
	public static boolean isCasRef(Blob blob) {
		return isBlobRef(blob) || isStringRef(blob);
	}

	/** Returns true if the given Blob is a CAS reference for an original {@code ABlob} cell. */
	public static boolean isBlobRef(Blob blob) {
		return hasMagic(blob, MAGIC_BLOB);
	}

	/** Returns true if the given Blob is a CAS reference for an original {@code AString} cell. */
	public static boolean isStringRef(Blob blob) {
		return hasMagic(blob, MAGIC_STRING);
	}

	private static boolean hasMagic(Blob blob, byte[] magic) {
		if (blob.count() != REF_SIZE) return false;
		for (int i = 0; i < magic.length; i++) {
			if (blob.byteAt(i) != magic[i]) return false;
		}
		return true;
	}

	// ── Store / retrieve ──────────────────────────────────────────────────────

	/**
	 * Persists {@code data} as an {@code ABlob} cell in the store and returns a
	 * 35-byte reference Blob tagged for reconstruction as an {@code ABlob}.
	 */
	public Blob storeAndRef(byte[] data) throws IOException {
		return persistAndRef(Blob.wrap(data), MAGIC_BLOB);
	}

	/**
	 * Persists {@code data} (the raw UTF-8 bytes of an {@code AString}) as an
	 * {@code AString} cell in the store and returns a 35-byte reference Blob
	 * tagged for reconstruction as an {@code AString}.
	 */
	public Blob storeAndRefForString(byte[] data) throws IOException {
		ACell cell = Strings.create(new String(data, java.nio.charset.StandardCharsets.UTF_8));
		return persistAndRef(cell, MAGIC_STRING);
	}

	private Blob persistAndRef(ACell cell, byte[] magic) throws IOException {
		ACell persisted = Cells.announce(cell, r -> {}, store);
		Hash hash = persisted.getHash();
		byte[] ref = new byte[REF_SIZE];
		System.arraycopy(magic, 0, ref, 0, magic.length);
		hash.getBytes(ref, magic.length);
		return Blob.wrap(ref);
	}

	/**
	 * Retrieves the original bytes for a CAS reference blob.
	 *
	 * @param casRef A 35-byte CAS reference Blob (as returned by {@link #storeAndRef})
	 * @return The original blob bytes
	 * @throws IOException if the referenced cell can't be resolved from the store
	 *         (e.g. genuinely missing — not yet replicated from a peer)
	 */
	public byte[] retrieve(Blob casRef) throws IOException {
		Hash hash = extractHash(casRef);
		ACell cell = lookup(hash);
		if (cell == null) {
			throw new IOException("BlobCAS value not found in store for hash " + hash.toHexString());
		}
		return ((ABlobLike<?>) cell).getBytes();
	}

	/**
	 * Resolves a raw hash (not a tagged 35-byte reference) directly against the
	 * store, returning the real cell (an {@code ABlob} or {@code AString}) or
	 * {@code null} if not present locally. Used by {@code ConvexFilesTable} for
	 * ad-hoc {@code SELECT ... FROM files WHERE hash = ...} lookups, where the
	 * caller only has a bare hash, not one of this class's own tagged refs.
	 */
	public ACell lookup(Hash hash) {
		Ref<ACell> ref = store.refForHash(hash);
		return (ref == null) ? null : ref.getValue();
	}

	/** Package-visible for {@link SQLRow#findCasReferenceHashes} — extracts the hash from a tagged reference blob. */
	static Hash extractHash(Blob casRef) {
		byte[] hashBytes = new byte[Hash.LENGTH];
		for (int i = 0; i < Hash.LENGTH; i++) hashBytes[i] = casRef.byteAt(MAGIC_BLOB.length + i);
		return Hash.wrap(hashBytes);
	}
}
