package convex.db.lattice;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import convex.core.data.ABlob;
import convex.core.data.ACell;
import convex.core.data.AString;
import convex.core.data.Blob;
import convex.core.data.prim.CVMLong;

/**
 * Utility class for secondary column index key encoding and matching.
 *
 * <p>Index key format: {@code [valueLenHi, valueLenLo, value_bytes..., pkHash_bytes...]}
 * <ul>
 *   <li>Bytes 0–1: big-endian unsigned length of the encoded value (max 65535 bytes)</li>
 *   <li>Bytes 2 to 2+valueLen: sortable encoding of the column value</li>
 *   <li>Remaining {@link #PK_HASH_BYTES} bytes: truncated SHA-256 hash of the pk bytes</li>
 * </ul>
 *
 * <p><b>Why the pk is hashed, not stored raw:</b> {@link convex.core.data.Index} is a radix
 * trie hard-capped at {@code MAX_DEPTH = 64} hex digits (32 bytes) — it can only distinguish
 * keys up to that many bytes of common prefix; beyond that, all such keys silently collapse
 * into a single trie entry (see {@code Index.assoc}/{@code MAX_DEPTH} javadoc). A raw
 * {@code [valueLen][value][full pk]} key easily exceeds 32 bytes once real composite PKs are
 * involved, and for exactly the case these indices exist for — many rows sharing the same
 * indexed value — the bytes that actually differ between rows are the pk bytes, which would
 * land past the cutoff. Hashing the pk down to a small fixed size keeps every key well under
 * the limit regardless of PK width.
 *
 * <p>Because the key no longer contains the literal pk, and two different pks can (rarely)
 * hash to the same {@link #PK_HASH_BYTES}, the column index's value is a <b>bucket</b>
 * ({@code AVector<ABlob>} of pks) rather than a single pk — see {@link SQLTable#indexAddRow}/
 * {@link SQLTable#indexRemoveRow}. Readers must resolve every pk in a matching bucket and
 * verify the actual row's column value before accepting it, rather than trusting the key
 * match alone.
 *
 * <p>The value encoding is sortable, enabling range queries:
 * <ul>
 *   <li>{@link CVMLong}: 8 bytes big-endian with sign bit XOR'd for correct signed ordering</li>
 *   <li>{@link AString}: raw UTF-8 bytes (lexicographic order)</li>
 *   <li>Other: CAD3 cell encoding (exact match only)</li>
 * </ul>
 */
public class ColumnIndex {

	/**
	 * Number of bytes the pk is hashed down to. 16 bytes (128 bits) makes accidental
	 * collisions between two different pks negligible for any realistic table size,
	 * and genuine collisions are handled correctly regardless (see class javadoc).
	 */
	public static final int PK_HASH_BYTES = 16;

	private ColumnIndex() {}

	// ── Value encoding ───────────────────────────────────────────────────────

	/**
	 * Encodes a column value to a sortable byte array for use as index key prefix.
	 */
	public static byte[] encodeValue(ACell value) {
		if (value instanceof CVMLong n) {
			// 8-byte big-endian with sign bit flipped for signed ordering
			long v = n.longValue() ^ Long.MIN_VALUE;
			byte[] b = new byte[8];
			for (int i = 7; i >= 0; i--) { b[i] = (byte) (v & 0xFF); v >>= 8; }
			return b;
		}
		if (value instanceof AString s) {
			return s.toString().getBytes(StandardCharsets.UTF_8);
		}
		// Fallback: CAD3 encoding (exact match only, not range-sortable across types)
		Blob encoded = convex.core.data.Cells.encode(value);
		byte[] b = new byte[(int) encoded.count()];
		for (int i = 0; i < b.length; i++) b[i] = encoded.byteAtUnchecked(i);
		return b;
	}

	// ── Key construction ─────────────────────────────────────────────────────

	/**
	 * Builds the index key for a (value, pk) pair.
	 * Format: 2-byte-big-endian(valueLen) ++ value_bytes ++ hash(pk_bytes)
	 */
	public static ABlob indexKey(ACell value, ABlob pk) {
		byte[] vb   = encodeValue(value);
		byte[] pkh  = hashPk(pk);
		byte[] key = new byte[2 + vb.length + PK_HASH_BYTES];
		key[0] = (byte) (vb.length >> 8);
		key[1] = (byte) (vb.length);
		System.arraycopy(vb,  0, key, 2,             vb.length);
		System.arraycopy(pkh, 0, key, 2 + vb.length, PK_HASH_BYTES);
		return Blob.wrap(key);
	}

	/** Truncated SHA-256 hash of the pk's bytes, {@link #PK_HASH_BYTES} bytes long. */
	static byte[] hashPk(ABlob pk) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			byte[] full = md.digest(blobBytes(pk));
			byte[] truncated = new byte[PK_HASH_BYTES];
			System.arraycopy(full, 0, truncated, 0, PK_HASH_BYTES);
			return truncated;
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 not available", e);
		}
	}

	// ── Key inspection ───────────────────────────────────────────────────────

	/**
	 * Returns the stored value length (bytes 0–1 of the index key).
	 */
	public static int getValueLen(ABlob indexKey) {
		return ((indexKey.byteAtUnchecked(0) & 0xFF) << 8)
			| (indexKey.byteAtUnchecked(1) & 0xFF);
	}

	// ── Match predicates ─────────────────────────────────────────────────────

	/**
	 * Returns true if the index key encodes exactly {@code value}.
	 */
	public static boolean matchesValue(ABlob indexKey, ACell value) {
		byte[] vb = encodeValue(value);
		if (indexKey.count() < 2 + vb.length) return false;
		int storedLen = getValueLen(indexKey);
		if (storedLen != vb.length) return false;
		for (int i = 0; i < vb.length; i++) {
			if (indexKey.byteAtUnchecked(2 + i) != vb[i]) return false;
		}
		return true;
	}

	/**
	 * Returns true if the value encoded in the index key is in the inclusive range [from, to].
	 *
	 * <p>Requires that {@code from} and {@code to} produce same-length encodings
	 * (true for CVMLong, true for fixed-length value types). If lengths differ,
	 * returns false.
	 */
	public static boolean matchesRange(ABlob indexKey, ACell from, ACell to) {
		byte[] fromBytes = encodeValue(from);
		byte[] toBytes   = encodeValue(to);
		if (fromBytes.length != toBytes.length) return false;
		int storedLen = getValueLen(indexKey);
		if (storedLen != fromBytes.length) return false;

		// Lexicographic comparison of stored value bytes vs [from, to]
		int cmpFrom = 0, cmpTo = 0;
		for (int i = 0; i < fromBytes.length; i++) {
			int b = indexKey.byteAtUnchecked(2 + i) & 0xFF;
			if (cmpFrom == 0) {
				if      (b > (fromBytes[i] & 0xFF)) cmpFrom = 1;
				else if (b < (fromBytes[i] & 0xFF)) cmpFrom = -1;
			}
			if (cmpTo == 0) {
				if      (b < (toBytes[i] & 0xFF)) cmpTo = -1;
				else if (b > (toBytes[i] & 0xFF)) cmpTo = 1;
			}
		}
		return cmpFrom >= 0 && cmpTo <= 0; // key_val >= from AND key_val <= to
	}

	// ── Internal helpers ─────────────────────────────────────────────────────

	static byte[] blobBytes(ABlob blob) {
		int n = (int) blob.count();
		byte[] b = new byte[n];
		for (int i = 0; i < n; i++) b[i] = blob.byteAtUnchecked(i);
		return b;
	}
}
