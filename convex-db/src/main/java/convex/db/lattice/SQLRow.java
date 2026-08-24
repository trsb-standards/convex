package convex.db.lattice;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import convex.core.data.ABlob;
import convex.core.data.ACell;
import convex.core.data.AString;
import convex.core.data.AVector;
import convex.core.data.Blob;
import convex.core.data.CAD3Encoder;
import convex.core.data.Cells;
import convex.core.data.Hash;
import convex.core.data.Strings;
import convex.core.data.Vectors;
import convex.core.data.prim.CVMLong;
import convex.core.exceptions.BadFormatException;
import convex.core.util.Utils;

/**
 * Utility class for SQL row entries in the table lattice.
 *
 * <p>Current row format (v4):
 * <ul>
 *   <li>Live row  — 2-element vector: [blob_values, version_blob8]</li>
 *   <li>Tombstone — 3-element vector: [null,        version_blob8, version_blob8]</li>
 * </ul>
 * {@code blob_values} is the standard CAD3 encoding of the column-values AVector,
 * stored as a single {@link Blob}.  This reduces per-row heap cost by ~4× compared
 * to storing the AVector and its element cells separately.
 *
 * <p>{@code version_blob8} is an 8-byte {@link Blob} holding a monotonic write
 * sequence number supplied by {@link SQLSchema}.  The counter is initialised from
 * {@code System.currentTimeMillis()} at schema creation time and incremented
 * atomically on every write, so every row gets a unique version — no two writes
 * within a single schema instance can tie.
 *
 * <p>Legacy format (v3, read-only): [blob_values, utime_blob4] — 4-byte compact
 * seconds since {@link #COMPACT_EPOCH_S}.  Values decode to ~1.7e12, which is
 * below any v4 counter (also initialised near 1.7e12 from currentTimeMillis),
 * so old rows sort before new rows in LWW comparisons.
 * Legacy format (v2, read-only): [AVector(values), utime_blob4].
 * Legacy format (v1, read-only): [AVector(values), CVMLong_ms, CVMLong_ms_or_null].
 * {@link #getVersion} handles all formats transparently.
 *
 * <p>Merge semantics: highest version wins; equal versions → deletion wins.
 */
public class SQLRow {

	/** Position of column values in the row vector */
	static final int POS_VALUES  = 0;
	/** Position of version / write sequence number */
	static final int POS_UTIME   = 1;
	/** Position of deletion version (tombstones only) */
	static final int POS_DELETED = 2;

	/** Unix seconds for 2020-01-01 00:00:00 UTC — used only for v3 legacy decode. */
	static final long COMPACT_EPOCH_S = 1_577_836_800L;

	private SQLRow() {}

	// ── Version codec ───────────────────────────────────────────────────────

	/**
	 * Encodes a write-sequence version as an 8-byte Blob (v4 format).
	 * The {@code version} value is the raw long from {@link SQLSchema}'s write counter.
	 */
	static Blob encodeTimestamp(long version) {
		byte[] bs = new byte[8];
		Utils.writeLong(bs, 0, version);
		return Blob.wrap(bs);
	}

	/**
	 * Decodes the version from a row's timestamp blob.
	 * Handles v4 (8-byte, raw long), v3 (4-byte compact seconds), and
	 * v1/v2 (CVMLong milliseconds) transparently.
	 */
	static long decodeTimestampMs(ABlob blob) {
		if (blob.count() == 8) return blob.longValue();          // v4: raw sequence number
		long s = blob.longValue() & 0xFFFFFFFFL;                  // v3: unsigned 32-bit compact seconds
		return (COMPACT_EPOCH_S + s) * 1000L;
	}

	// ── Values codec (v3) ──────────────────────────────────────────────────

	/**
	 * Marks the flat per-cell values encoding (see {@link #encodeFlat}), as
	 * opposed to the legacy {@code Cells.encode(AVector)} format (v3, whose
	 * first byte is always {@code Tag.VECTOR} = {@code 0x80}). Chosen to be
	 * unambiguous with any real CAD3 tag.
	 */
	static final byte FLAT_MARKER = (byte) 0xFF;

	/**
	 * Encodes column values to a compact Blob.
	 *
	 * <p>If {@link BlobCAS#instance()} is active, any ABlob or AString cell
	 * exceeding {@link BlobCAS#THRESHOLD} bytes is stored in the CAS and
	 * replaced with a compact 35-byte CAS reference before encoding.
	 *
	 * <p>The result uses the flat per-cell layout (see {@link #encodeFlat}),
	 * not a plain {@code Cells.encode(AVector)} — Convex vectors only embed
	 * their last 16 elements directly; anything beyond that lives behind an
	 * out-of-line "prefix" Ref once its own encoding exceeds the inline size
	 * limit, which real tables with more than ~16 columns hit routinely,
	 * independent of any single column's size. Since that Ref's target was
	 * never persisted to a store (this encoding is a pure in-memory byte
	 * computation), it can never be resolved later — a structural decode
	 * failure, not something BlobCAS-style externalization or store access
	 * at decode time could fix. The flat layout sidesteps this entirely by
	 * encoding each cell independently, so no cross-cell Ref is ever produced.
	 */
	static Blob encodeValues(AVector<ACell> values) {
		try {
			BlobCAS cas = BlobCAS.instance();
			if (cas != null) values = substituteLargeBlobs(values, cas);
			return encodeFlat(values);
		} catch (IOException e) {
			throw new IllegalStateException("Compact row encode failed", e);
		}
	}

	/**
	 * Decodes column values from a compact Blob. Dispatches on the leading
	 * byte: {@link #FLAT_MARKER} for the current flat per-cell format, or
	 * anything else (i.e. {@code Tag.VECTOR}) for legacy {@code Cells.encode(AVector)}
	 * data written before this format existed.
	 *
	 * <p>If {@link BlobCAS#instance()} is active, any CAS reference cells in
	 * the decoded vector are resolved back to their original blob/string. A
	 * reference that can't be resolved (genuinely missing from the store —
	 * e.g. not yet replicated from a peer) is a loud failure here, not a
	 * silently-substituted placeholder — see {@code resolveCasRefs}.
	 */
	@SuppressWarnings("unchecked")
	static AVector<ACell> decodeValues(Blob blob) {
		try {
			AVector<ACell> values = decodeRawValues(blob);
			BlobCAS cas = BlobCAS.instance();
			if (cas != null) values = resolveCasRefs(values, cas);
			return values;
		} catch (BadFormatException | IOException e) {
			throw new IllegalStateException("Compact row decode failed", e);
		}
	}

	/**
	 * Decodes column values without resolving CAS references — cells backed
	 * by {@link BlobCAS} still come back as their raw 35-byte tagged
	 * reference blob, not the real content. Used by {@link #findCasReferenceHashes}
	 * to discover which hashes a just-pulled row depends on, before those
	 * hashes are necessarily fetchable — {@link #decodeValues}'s normal
	 * resolve-or-throw behavior would be the wrong tool here, since the whole
	 * point is inspecting a reference that may not resolve yet.
	 */
	@SuppressWarnings("unchecked")
	private static AVector<ACell> decodeRawValues(Blob blob) throws BadFormatException {
		return (blob.count() > 0 && blob.byteAt(0) == FLAT_MARKER)
				? decodeFlat(blob)
				: (AVector<ACell>) CAD3Encoder.INSTANCE.decode(blob); // legacy v3
	}

	/**
	 * Scans a row's encoded values for {@link BlobCAS} reference cells,
	 * returning the set of hashes they point to — used by replication (after
	 * a schema/db pull lands the row data itself, but before any large
	 * values it references are necessarily present locally) to know which
	 * hashes still need an explicit fetch from the source peer. See {@code
	 * DbaseServer.replicateOneSchema}/{@code replicateOneDatabase}'s own
	 * class docs for why this step exists: a CAS reference is an ordinary
	 * opaque blob value, not a genuine Convex {@code Ref}, so it is invisible
	 * to {@code pullPath}'s normal Ref-graph acquisition — nothing else would
	 * ever fetch the value it points to.
	 */
	static Set<Hash> findCasReferenceHashes(Blob blob) {
		Set<Hash> hashes = new HashSet<>();
		AVector<ACell> values;
		try {
			values = decodeRawValues(blob);
		} catch (BadFormatException e) {
			throw new IllegalStateException("Compact row decode failed", e);
		}
		int n = (int) values.count();
		for (int i = 0; i < n; i++) {
			ACell cell = values.get(i);
			if (cell instanceof Blob ref && BlobCAS.isCasRef(ref)) {
				hashes.add(BlobCAS.extractHash(ref));
			}
		}
		return hashes;
	}

	/**
	 * Encodes each cell independently — {@code [FLAT_MARKER][4-byte count]
	 * {4-byte length + CAD3 bytes}*count}. After {@link #substituteLargeBlobs},
	 * every remaining cell's own CAD3 encoding is small (SQL column types are
	 * flat scalars/CAS-refs, never large nested structures), so each one is
	 * guaranteed to decode standalone with the storeless encoder — no Ref
	 * spanning multiple cells (like a vector's chunking prefix) is ever created.
	 */
	private static Blob encodeFlat(AVector<ACell> values) {
		int n = (int) values.count();
		Blob[] cellBytes = new Blob[n];
		int total = 1 + 4;
		for (int i = 0; i < n; i++) {
			Blob b = Cells.encode(values.get(i));
			cellBytes[i] = b;
			total += 4 + (int) b.count();
		}
		byte[] out = new byte[total];
		int pos = 0;
		out[pos++] = FLAT_MARKER;
		pos = writeInt(out, pos, n);
		for (Blob b : cellBytes) {
			pos = writeInt(out, pos, (int) b.count());
			byte[] bytes = b.getBytes();
			System.arraycopy(bytes, 0, out, pos, bytes.length);
			pos += bytes.length;
		}
		return Blob.wrap(out);
	}

	/** Decodes the flat per-cell layout produced by {@link #encodeFlat}. */
	private static AVector<ACell> decodeFlat(Blob blob) throws BadFormatException {
		byte[] data = blob.getBytes();
		int pos = 1; // skip marker
		int n = readInt(data, pos);
		pos += 4;
		ACell[] cells = new ACell[n];
		for (int i = 0; i < n; i++) {
			int len = readInt(data, pos);
			pos += 4;
			cells[i] = CAD3Encoder.INSTANCE.decode(Blob.wrap(data, pos, len));
			pos += len;
		}
		return Vectors.create(cells);
	}

	private static int writeInt(byte[] arr, int pos, int value) {
		arr[pos]     = (byte) ((value >>> 24) & 0xFF);
		arr[pos + 1] = (byte) ((value >>> 16) & 0xFF);
		arr[pos + 2] = (byte) ((value >>> 8) & 0xFF);
		arr[pos + 3] = (byte) (value & 0xFF);
		return pos + 4;
	}

	private static int readInt(byte[] arr, int pos) {
		return ((arr[pos] & 0xFF) << 24) | ((arr[pos + 1] & 0xFF) << 16)
				| ((arr[pos + 2] & 0xFF) << 8) | (arr[pos + 3] & 0xFF);
	}

	/**
	 * Replaces ABlob/AString cells above the CAS threshold with compact CAS
	 * reference blobs. The reference is tagged so {@link #resolveCasRefs}
	 * can reconstruct the correct CVM type.
	 */
	private static AVector<ACell> substituteLargeBlobs(AVector<ACell> values, BlobCAS cas) throws IOException {
		int n = (int) values.count();
		ACell[] cells = null; // allocated lazily only if we actually substitute
		for (int i = 0; i < n; i++) {
			ACell cell = values.get(i);
			if (cell instanceof ABlob blob && blob.count() > BlobCAS.THRESHOLD) {
				if (cells == null) {
					cells = new ACell[n];
					for (int j = 0; j < i; j++) cells[j] = values.get(j);
				}
				cells[i] = cas.storeAndRef(blob.getBytes());
			} else if (cell instanceof AString str && str.count() > BlobCAS.THRESHOLD) {
				if (cells == null) {
					cells = new ACell[n];
					for (int j = 0; j < i; j++) cells[j] = values.get(j);
				}
				cells[i] = cas.storeAndRefForString(str.getBytes());
			} else if (cells != null) {
				cells[i] = cell;
			}
		}
		return (cells != null) ? Vectors.of(cells) : values;
	}

	/** Resolves CAS reference blobs back to their original ABlob bytes or AString. */
	private static AVector<ACell> resolveCasRefs(AVector<ACell> values, BlobCAS cas) throws IOException {
		int n = (int) values.count();
		ACell[] cells = null; // allocated lazily
		for (int i = 0; i < n; i++) {
			ACell cell = values.get(i);
			if (cell instanceof Blob ref && BlobCAS.isBlobRef(ref)) {
				if (cells == null) {
					cells = new ACell[n];
					for (int j = 0; j < i; j++) cells[j] = values.get(j);
				}
				cells[i] = Blob.wrap(cas.retrieve(ref));
			} else if (cell instanceof Blob ref && BlobCAS.isStringRef(ref)) {
				if (cells == null) {
					cells = new ACell[n];
					for (int j = 0; j < i; j++) cells[j] = values.get(j);
				}
				cells[i] = Strings.create(Blob.wrap(cas.retrieve(ref)));
			} else if (cells != null) {
				cells[i] = cell;
			}
		}
		return (cells != null) ? Vectors.of(cells) : values;
	}

	// ── Factory methods ────────────────────────────────────────────────────

	/**
	 * Creates a new live row entry (v3 compact format).
	 *
	 * @param values    Column values for the row
	 * @param timestamp Update timestamp (milliseconds)
	 * @return 2-element row vector [blob_values, utime_blob4]
	 */
	public static AVector<ACell> create(AVector<ACell> values, CVMLong timestamp) {
		return Vectors.of(encodeValues(values), encodeTimestamp(timestamp.longValue()));
	}

	/**
	 * Creates a tombstone entry for a deleted row.
	 *
	 * @param timestamp Deletion timestamp (milliseconds)
	 * @return 3-element tombstone vector [null, utime_blob4, deleted_blob4]
	 */
	public static AVector<ACell> createTombstone(CVMLong timestamp) {
		Blob ts = encodeTimestamp(timestamp.longValue());
		return Vectors.of(null, ts, ts);
	}

	// ── Accessors ──────────────────────────────────────────────────────────

	/**
	 * Gets the column values from a row entry.
	 * Handles v3 (Blob-encoded), v2 (AVector+Blob4), and v1 (AVector+CVMLong).
	 *
	 * @param row Row entry vector
	 * @return Column values, or null if tombstone
	 */
	@SuppressWarnings("unchecked")
	public static AVector<ACell> getValues(AVector<ACell> row) {
		if (row == null) return null;
		ACell cell = row.get(POS_VALUES);
		if (cell instanceof Blob blob) return decodeValues(blob);   // v3 compact
		return (AVector<ACell>) cell;                               // v1/v2 legacy
	}

	/**
	 * Gets the update timestamp as a CVMLong (milliseconds).
	 * Handles both compact Blob(4) format (v2) and legacy CVMLong format (v1).
	 *
	 * @param row Row entry vector
	 * @return Update timestamp in milliseconds, or null
	 */
	public static CVMLong getTimestamp(AVector<ACell> row) {
		if (row == null) return null;
		ACell cell = row.get(POS_UTIME);
		if (cell instanceof CVMLong) return (CVMLong) cell;          // v1 legacy
		if (cell instanceof ABlob)  return CVMLong.create(decodeTimestampMs((ABlob) cell)); // v2
		return null;
	}

	/**
	 * Gets the deletion timestamp from a tombstone row entry.
	 *
	 * @param row Row entry vector
	 * @return Deletion timestamp in milliseconds, or null if live
	 */
	public static CVMLong getDeleted(AVector<ACell> row) {
		if (row == null || row.count() < 3) return null;
		ACell cell = row.get(POS_DELETED);
		if (cell instanceof CVMLong) return (CVMLong) cell;
		if (cell instanceof ABlob)  return CVMLong.create(decodeTimestampMs((ABlob) cell));
		return null;
	}

	/**
	 * Checks if a row entry is a tombstone (deleted).
	 * Tombstones always have 3 elements; live rows have 2.
	 *
	 * @param row Row entry vector
	 * @return true if tombstone
	 */
	public static boolean isTombstone(AVector<ACell> row) {
		if (row == null) return false;
		return row.count() == 3;
	}

	/**
	 * Checks if a row entry is live (not deleted).
	 *
	 * @param row Row entry vector
	 * @return true if live
	 */
	public static boolean isLive(AVector<ACell> row) {
		return row != null && !isTombstone(row);
	}

	/**
	 * Updates a row entry with new values and a new timestamp.
	 *
	 * @param row       Original row entry (unused; kept for API symmetry)
	 * @param values    New column values
	 * @param timestamp New update timestamp (milliseconds)
	 * @return Updated live row entry
	 */
	public static AVector<ACell> withValues(AVector<ACell> row, AVector<ACell> values, CVMLong timestamp) {
		return Vectors.of(encodeValues(values), encodeTimestamp(timestamp.longValue()));
	}

	// ── Merge ──────────────────────────────────────────────────────────────

	/**
	 * Merges two row entries using LWW semantics.
	 * Latest timestamp wins. If equal, deletion wins.
	 *
	 * @param a First row entry
	 * @param b Second row entry
	 * @return Merged row entry
	 */
	public static AVector<ACell> merge(AVector<ACell> a, AVector<ACell> b) {
		if (a == null) return b;
		if (b == null) return a;

		CVMLong timeA = getTimestamp(a);
		CVMLong timeB = getTimestamp(b);

		if (timeA == null && timeB == null) return a;
		if (timeA == null) return b;
		if (timeB == null) return a;

		long ta = timeA.longValue();
		long tb = timeB.longValue();

		if (ta > tb) return a;
		if (tb > ta) return b;

		// Equal timestamps: deletion wins
		if (isTombstone(b)) return b;
		return a;
	}
}
