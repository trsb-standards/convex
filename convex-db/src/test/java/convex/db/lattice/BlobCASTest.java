package convex.db.lattice;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import convex.core.data.ACell;
import convex.core.data.ABlob;
import convex.core.data.Blob;
import convex.core.data.Hash;
import convex.core.store.MemoryStore;

/**
 * Coverage for the store-backed redesign of {@link BlobCAS} (2026-08-10) —
 * replacing a local-only file directory (never replicated, see TODO.md) with
 * durable persistence into the node's own {@code AStore}, so large values
 * ride along with ordinary lattice replication ({@code pullPath}/{@code
 * Acquiror}) instead of a separate, non-replicating side channel.
 */
class BlobCASTest {

	@AfterEach
	void shutdown() {
		BlobCAS.shutdown();
	}

	private static byte[] repeatedBytes(int length, byte value) {
		byte[] data = new byte[length];
		Arrays.fill(data, value);
		return data;
	}

	@Test
	void largeBlobRoundTripsThroughTheStore() throws IOException {
		BlobCAS.init(new MemoryStore());
		BlobCAS cas = BlobCAS.instance();
		byte[] original = repeatedBytes(5000, (byte) 0x42);

		Blob ref = cas.storeAndRef(original);

		assertEquals(BlobCAS.REF_SIZE, ref.count());
		assertTrue(BlobCAS.isBlobRef(ref));
		assertArrayEquals(original, cas.retrieve(ref));
	}

	@Test
	void largeStringRoundTripsThroughTheStore() throws IOException {
		BlobCAS.init(new MemoryStore());
		BlobCAS cas = BlobCAS.instance();
		String original = "x".repeat(5000);
		byte[] utf8 = original.getBytes(StandardCharsets.UTF_8);

		Blob ref = cas.storeAndRefForString(utf8);

		assertTrue(BlobCAS.isStringRef(ref));
		assertArrayEquals(utf8, cas.retrieve(ref));
	}

	@Test
	void retrieveOnAHashNeverStoredThrowsInsteadOfSubstitutingSomethingElse() throws IOException {
		BlobCAS.init(new MemoryStore());
		BlobCAS cas = BlobCAS.instance();
		// A reference the store never actually persisted (a genuinely unrelated
		// hash, not one produced by storeAndRef) -- must fail loudly, not
		// silently hand back a placeholder. See SQLRow.resolveCasRefs' own doc
		// for the live corruption bug this replaces.
		byte[] fakeRef = new byte[BlobCAS.REF_SIZE];
		fakeRef[0] = 0x00; fakeRef[1] = (byte) 0xCA; fakeRef[2] = 0x5B; // MAGIC_BLOB
		Blob neverStored = Blob.wrap(fakeRef);

		assertThrows(IOException.class, () -> cas.retrieve(neverStored));
	}

	@Test
	void identicalContentDeduplicatesToTheSameReference() throws IOException {
		BlobCAS.init(new MemoryStore());
		BlobCAS cas = BlobCAS.instance();
		byte[] data = repeatedBytes(5000, (byte) 0x7A);

		Blob refA = cas.storeAndRef(data);
		Blob refB = cas.storeAndRef(data);

		assertEquals(refA, refB, "identical content must resolve to the same hash, stored once");
	}

	@Test
	void lookupByRawHashReturnsTheRealCellForConvexFilesTable() throws IOException {
		BlobCAS.init(new MemoryStore());
		BlobCAS cas = BlobCAS.instance();
		byte[] data = repeatedBytes(5000, (byte) 0x11);
		Blob ref = cas.storeAndRef(data);
		Hash hash = extractHashForTest(ref);

		ACell cell = cas.lookup(hash);

		assertNotNull(cell);
		assertArrayEquals(data, ((ABlob) cell).getBytes());
	}

	@Test
	void lookupByRawHashReturnsNullWhenNeverStored() {
		BlobCAS.init(new MemoryStore());
		BlobCAS cas = BlobCAS.instance();

		assertNull(cas.lookup(Hash.fromHex("00".repeat(Hash.LENGTH))));
	}

	@Test
	void stringAndBlobKindsShareTheSameContentDeduplication() throws IOException {
		BlobCAS.init(new MemoryStore());
		BlobCAS cas = BlobCAS.instance();
		byte[] data = "shared content example, padded".repeat(200).getBytes(StandardCharsets.UTF_8);

		Blob blobRef = cas.storeAndRef(data);
		Blob stringRef = cas.storeAndRefForString(data);

		// Different magic tags (so the two kinds stay distinguishable on read),
		// but retrieving both gives back the identical bytes -- the underlying
		// store only ever wrote this content once, per BlobCAS's own class doc.
		assertArrayEquals(data, cas.retrieve(blobRef));
		assertArrayEquals(data, cas.retrieve(stringRef));
	}

	private static Hash extractHashForTest(Blob ref) {
		byte[] hashBytes = new byte[Hash.LENGTH];
		for (int i = 0; i < Hash.LENGTH; i++) hashBytes[i] = ref.byteAt(3 + i);
		return Hash.wrap(hashBytes);
	}
}
