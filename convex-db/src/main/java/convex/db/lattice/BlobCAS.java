package convex.db.lattice;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import convex.core.data.Blob;

/**
 * Content-addressable storage for large blobs and strings that exceed the CAD3
 * inline threshold.
 *
 * <p>When a BLOB or (large) TEXT/VARCHAR column value exceeds {@link #THRESHOLD}
 * bytes, storing it inline in a Convex row causes CAD3 to split it into a
 * BlobTree/StringTree with Refs that require the etch store to decode. The
 * JDBC/Calcite layer does not have etch store access, so decode fails with
 * "Cannot read Ref without a store".
 *
 * <p>BlobCAS solves this by storing large values as files named by their SHA-256
 * hash, and replacing the inline cell with a compact 35-byte CAS reference:
 * {@code [magic] + sha256(data)}, where {@code magic} is one of
 * {@link #MAGIC_BLOB} or {@link #MAGIC_STRING} depending on whether the
 * original cell was an {@code ABlob} or an {@code AString} — this lets the
 * reader reconstruct the correct CVM type. On read, CAS refs are resolved back
 * to the original bytes (and type) transparently.
 *
 * <p>Usage:
 * <pre>
 *   BlobCAS.init(Path.of("/data/etch/mydb_blobs"));  // at startup
 *   // ... use SQLSchema normally; large blobs/strings are handled automatically
 *   BlobCAS.shutdown();                               // at shutdown
 * </pre>
 *
 * <p>Deduplication is automatic: identical content stored multiple times (even
 * under different magic, e.g. once as a blob and once as a string) costs only
 * one file on disk.
 */
public class BlobCAS {

    /** Values larger than this many bytes are stored in CAS rather than inline. */
    public static final int THRESHOLD = 4096;

    /** Magic prefix for blob CAS references: 3 bytes identifying an ABlob-typed CAS ref. */
    static final byte[] MAGIC_BLOB = {0x00, (byte) 0xCA, 0x5B};

    /** Magic prefix for string CAS references: 3 bytes identifying an AString-typed CAS ref. */
    static final byte[] MAGIC_STRING = {0x00, (byte) 0xCA, 0x53};

    /** Total size of a CAS reference blob: 3 magic bytes + 32 SHA-256 bytes. */
    static final int REF_SIZE = MAGIC_BLOB.length + 32;

    private static volatile BlobCAS INSTANCE;

    private final Path dir;

    private BlobCAS(Path dir) {
        this.dir = dir;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    /**
     * Initialises the global BlobCAS singleton, creating the directory if needed.
     * No-op if already initialised.
     */
    public static synchronized void init(Path casDir) throws IOException {
        if (INSTANCE != null) return;
        Files.createDirectories(casDir);
        INSTANCE = new BlobCAS(casDir);
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
     * Stores {@code data} in CAS and returns a 35-byte CAS reference Blob tagged
     * for reconstruction as an {@code ABlob}. If the same content was already
     * stored (under any tag), no file is written (idempotent).
     */
    public Blob storeAndRef(byte[] data) throws IOException {
        return storeAndRef(data, MAGIC_BLOB);
    }

    /**
     * Stores {@code data} (the raw UTF-8 bytes of an {@code AString}) in CAS and
     * returns a 35-byte CAS reference Blob tagged for reconstruction as an
     * {@code AString}. If the same content was already stored (under any tag),
     * no file is written (idempotent).
     */
    public Blob storeAndRefForString(byte[] data) throws IOException {
        return storeAndRef(data, MAGIC_STRING);
    }

    private Blob storeAndRef(byte[] data, byte[] magic) throws IOException {
        byte[] hash = sha256(data);
        Path file = dir.resolve(hex(hash));
        if (!Files.exists(file)) {
            Files.write(file, data, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        }
        byte[] ref = new byte[REF_SIZE];
        System.arraycopy(magic, 0, ref, 0, magic.length);
        System.arraycopy(hash, 0, ref, magic.length, 32);
        return Blob.wrap(ref);
    }

    /**
     * Retrieves the original bytes for a CAS reference blob.
     *
     * @param casRef A 35-byte CAS reference Blob (as returned by {@link #storeAndRef})
     * @return The original blob bytes
     * @throws IOException if the CAS file is missing or unreadable
     */
    public byte[] retrieve(Blob casRef) throws IOException {
        byte[] hash = new byte[32];
        for (int i = 0; i < 32; i++) hash[i] = casRef.byteAt(MAGIC_BLOB.length + i);
        Path file = dir.resolve(hex(hash));
        return Files.readAllBytes(file);
    }

    // ── Internals ─────────────────────────────────────────────────────────────

    private static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
