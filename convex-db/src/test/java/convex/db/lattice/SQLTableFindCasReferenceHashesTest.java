package convex.db.lattice;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import convex.core.data.Hash;
import convex.core.store.MemoryStore;
import convex.db.calcite.ConvexType;

/**
 * Covers {@link SQLTable#findCasReferenceHashes()}, used by replication
 * (see {@code DbaseServer.acquireCasReferencedValues}) to discover which
 * {@link BlobCAS}-backed hashes a just-pulled table's rows depend on.
 *
 * <p>Found live 2026-10-01 replicating the real "ose" schema: a deleted
 * (tombstone) row's {@code POS_VALUES} slot is {@code null} (see {@code
 * RowBlock}'s own {@code [values|null, utime, utime]} shape) — the original
 * implementation unconditionally cast every row's values slot to {@code
 * Blob} with no liveness check, throwing a {@code NullPointerException} on
 * the very first tombstone it scanned. Because {@code SQLTable} iterates
 * every table in a schema inside one unguarded loop
 * ({@code DbaseServer.acquireCasReferencedValues}), this didn't just skip
 * one row — it aborted the ENTIRE replication attempt for every table in
 * the schema, with the whole db ending up marked {@code STS_FAILED} and
 * every large value left permanently unreplicated.
 */
class SQLTableFindCasReferenceHashesTest {

	@AfterEach
	void shutdown() {
		BlobCAS.shutdown();
	}

	@Test
	void doesNotThrowOnATombstoneRowAndStillFindsHashesFromLiveRows() throws Exception {
		BlobCAS.init(new MemoryStore());

		SQLSchema schema = SQLSchema.create();
		schema.createTable("t", new String[]{"id", "body"},
			new ConvexType[]{ConvexType.INTEGER, ConvexType.VARCHAR}, 1);

		byte[] large = new byte[BlobCAS.THRESHOLD + 1];
		java.util.Arrays.fill(large, (byte) 0x5A);
		String largeBody = new String(large, java.nio.charset.StandardCharsets.UTF_8);

		// One row that stays live (its large value is CAS-referenced) and one
		// that gets deleted afterward -- the deleted row is what used to
		// throw a NullPointerException before this fix, unconditionally
		// aborting the scan before the live row's hash was ever collected.
		schema.insert("t", 1L, largeBody);
		schema.insert("t", 2L, largeBody);
		schema.deleteByKey("t", convex.core.data.prim.CVMLong.create(2L));

		Set<Hash> hashes = schema.getTable("t").findCasReferenceHashes();

		assertEquals(1, hashes.size(), "only the live row's referenced hash should be found");
		assertTrue(BlobCAS.instance().lookup(hashes.iterator().next()) != null,
			"the found hash must actually resolve against the store");
	}

	@Test
	void returnsEmptyForATableWithNoCasReferencedValues() {
		SQLSchema schema = SQLSchema.create();
		schema.createTable("t", new String[]{"id", "name"},
			new ConvexType[]{ConvexType.INTEGER, ConvexType.VARCHAR}, 1);
		schema.insert("t", 1L, "short value");

		Set<Hash> hashes = schema.getTable("t").findCasReferenceHashes();

		assertTrue(hashes.isEmpty());
	}
}
