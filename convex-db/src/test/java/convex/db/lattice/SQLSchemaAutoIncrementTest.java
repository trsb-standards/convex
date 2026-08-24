package convex.db.lattice;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import convex.core.data.ACell;
import convex.core.data.AVector;
import convex.core.data.Strings;
import convex.core.data.Vectors;
import convex.core.data.prim.CVMLong;
import convex.db.calcite.ConvexColumnType;
import convex.db.calcite.ConvexType;

/**
 * Covers auto-generating (single-column) primary keys — omitted/NULL on
 * INSERT generates the next value, an explicit value is honored and pulls
 * the counter forward past it, and a rare collision (something else already
 * occupying the generated candidate) advances to the next free slot rather
 * than colliding destructively. See {@link AutoIncrementCounters}'s own doc
 * for why this is a plain sequential counter (not the HLC-style pattern
 * {@code WRITE_SEQ}/{@code nextHistorySeq} use), seeded from a table's own
 * {@code MAX(pk)} rather than persisted/wall-clock-based.
 */
class SQLSchemaAutoIncrementTest {

	private static SQLSchema autoIncrementTable(String name) {
		SQLSchema schema = SQLSchema.create();
		schema.createTable(Strings.create(name),
			new String[]{"ID", "VAL"},
			new ConvexColumnType[]{
				ConvexColumnType.of(ConvexType.INTEGER),
				ConvexColumnType.of(ConvexType.VARCHAR)},
			1, false, true);
		return schema;
	}

	@Test
	void insertOmittingPkGeneratesSequentialValuesStartingAt1() {
		SQLSchema schema = autoIncrementTable("t");
		schema.insert("t", Vectors.of(null, "alpha"));
		schema.insert("t", Vectors.of(null, "beta"));
		schema.insert("t", Vectors.of(null, "gamma"));

		assertEquals("alpha", schema.selectByKey("t", CVMLong.create(1L)).get(1).toString());
		assertEquals("beta", schema.selectByKey("t", CVMLong.create(2L)).get(1).toString());
		assertEquals("gamma", schema.selectByKey("t", CVMLong.create(3L)).get(1).toString());
	}

	@Test
	void explicitValueIsHonoredAndCounterAdvancesPastIt() {
		SQLSchema schema = autoIncrementTable("t");
		schema.insert("t", Vectors.of(CVMLong.create(50L), "fifty"));

		schema.insert("t", Vectors.of(null, "next"));

		// Must not collide with 50 -- and per MySQL's own behaviour, the next
		// generated value is pulled ahead of the explicit one, not left at 1.
		assertEquals("next", schema.selectByKey("t", CVMLong.create(51L)).get(1).toString());
	}

	@Test
	void counterSeedsFromExistingDataOnFirstGenerate() {
		// Simulates the real migration scenario: historical rows already
		// present (explicit ids, e.g. from a migrated source database)
		// before the application ever generates its own first id.
		SQLSchema schema = autoIncrementTable("t");
		schema.insert("t", Vectors.of(CVMLong.create(5L), "five"));
		schema.insert("t", Vectors.of(CVMLong.create(196L), "one-ninety-six"));
		schema.insert("t", Vectors.of(CVMLong.create(80L), "eighty")); // out of order, must not confuse seeding

		schema.insert("t", Vectors.of(null, "new"));

		assertEquals("new", schema.selectByKey("t", CVMLong.create(197L)).get(1).toString());
	}

	@Test
	void convertToAutoIncrementSeedsFromExistingData() {
		SQLSchema schema = SQLSchema.create();
		schema.createTable(Strings.create("t"),
			new String[]{"ID", "VAL"},
			new ConvexColumnType[]{
				ConvexColumnType.of(ConvexType.INTEGER),
				ConvexColumnType.of(ConvexType.VARCHAR)},
			1, false, false);
		schema.insert("t", Vectors.of(CVMLong.create(1L), "a"));
		schema.insert("t", Vectors.of(CVMLong.create(2L), "b"));

		assertTrue(schema.convertToAutoIncrement("t"));
		schema.insert("t", Vectors.of(null, "c"));

		assertEquals("c", schema.selectByKey("t", CVMLong.create(3L)).get(1).toString());
	}

	@Test
	void collisionAdvancesToNearestFreeSlotRatherThanClobbering() {
		SQLSchema schema = autoIncrementTable("t");
		// Simulates a peer's write for id=1 having already merged in locally
		// (e.g. a cross-node race) before this node's own counter -- which
		// starts fresh at 0 -- ever got a chance to observe it.
		schema.insert("t", Vectors.of(CVMLong.create(1L), "from-peer"));

		schema.insert("t", Vectors.of(null, "local"));

		// The generated row must land on the next FREE slot, not silently
		// overwrite the peer's row at 1.
		assertEquals("from-peer", schema.selectByKey("t", CVMLong.create(1L)).get(1).toString());
		assertEquals("local", schema.selectByKey("t", CVMLong.create(2L)).get(1).toString());
	}

	@Test
	void neverReusesAGapLeftByADeletedRow() {
		SQLSchema schema = autoIncrementTable("t");
		schema.insert("t", Vectors.of(null, "one"));
		schema.insert("t", Vectors.of(null, "two"));
		schema.deleteByKey("t", CVMLong.create(2L));

		schema.insert("t", Vectors.of(null, "three"));

		// MySQL's own auto_increment never reuses a deleted row's id --
		// "three" must land on 3, not reclaim the now-empty slot 2.
		assertEquals("three", schema.selectByKey("t", CVMLong.create(3L)).get(1).toString());
	}

	@Test
	void createTableRejectsCompositePrimaryKeyForAutoIncrement() {
		SQLSchema schema = SQLSchema.create();
		assertThrows(UnsupportedOperationException.class, () ->
			schema.createTable(Strings.create("t"),
				new String[]{"a", "b"},
				new ConvexColumnType[]{
					ConvexColumnType.of(ConvexType.INTEGER),
					ConvexColumnType.of(ConvexType.INTEGER)},
				2, false, true));
	}

	@Test
	void createTableRejectsNonIntegerPrimaryKeyForAutoIncrement() {
		SQLSchema schema = SQLSchema.create();
		assertThrows(UnsupportedOperationException.class, () ->
			schema.createTable(Strings.create("t"),
				new String[]{"id", "val"},
				new ConvexColumnType[]{
					ConvexColumnType.of(ConvexType.VARCHAR),
					ConvexColumnType.of(ConvexType.VARCHAR)},
				1, false, true));
	}

	@Test
	void convertToAutoIncrementRejectsCompositePrimaryKeyTable() {
		SQLSchema schema = SQLSchema.create();
		schema.createTable(Strings.create("t"),
			new String[]{"a", "b"},
			new ConvexColumnType[]{
				ConvexColumnType.of(ConvexType.INTEGER),
				ConvexColumnType.of(ConvexType.INTEGER)},
			2, false, false);

		assertThrows(UnsupportedOperationException.class, () -> schema.convertToAutoIncrement("t"));
	}

	@Test
	void convertToAutoIncrementRejectsNonIntegerPrimaryKey() {
		SQLSchema schema = SQLSchema.create();
		schema.createTable("t", new String[]{"id", "val"}, false);
		assertThrows(UnsupportedOperationException.class, () -> schema.convertToAutoIncrement("t"));
	}

	@Test
	void convertToAutoIncrementIsANoOpWhenAlreadyAutoIncrement() {
		SQLSchema schema = autoIncrementTable("t");
		assertTrue(schema.convertToAutoIncrement("t"));
		schema.insert("t", Vectors.of(null, "still-works"));
		assertEquals("still-works", schema.selectByKey("t", CVMLong.create(1L)).get(1).toString());
	}

	@Test
	void convertToAutoIncrementReturnsFalseForAMissingTable() {
		SQLSchema schema = SQLSchema.create();
		assertFalse(schema.convertToAutoIncrement("nope"));
	}

	@Test
	void plainTableIsUnaffected() {
		SQLSchema schema = SQLSchema.create();
		schema.createTable("t", new String[]{"id", "val"}, false);
		// A NULL pk on a plain (non-auto-increment) table is just a NULL
		// primary key value -- not this feature's concern, unaffected.
		AVector<ACell> row = Vectors.of(CVMLong.create(1L), "x");
		schema.insert("t", row);
		assertEquals("x", schema.selectByKey("t", CVMLong.create(1L)).get(1).toString());
	}
}
