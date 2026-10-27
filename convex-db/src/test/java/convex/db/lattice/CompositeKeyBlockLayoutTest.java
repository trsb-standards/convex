package convex.db.lattice;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import convex.core.data.ABlob;
import convex.core.data.ACell;
import convex.core.data.AVector;
import convex.core.data.Index;
import convex.core.data.Vectors;
import convex.core.data.prim.CVMLong;
import convex.db.calcite.ConvexType;

/**
 * A composite-key table stores every row in its own block
 * ({@link SQLTable#blockKeyFor}). Under the old rule -- block key = first 8
 * bytes of the pk -- all rows of a table like {@code (NODEID, LOGID)} shared
 * ONE block, because a composite pk starts with the 4-byte length of its
 * first component: every insert rewrote the whole table (found live
 * 2026-10-05/06 on perf.querylog: 6-32 MB written per logged statement).
 * Also covers tables still holding rows in the old layout.
 */
class CompositeKeyBlockLayoutTest {

	private static final int N = 200;

	private static SQLSchema schemaWithLog() {
		SQLSchema schema = SQLSchema.create();
		schema.createTable("log", new String[]{"NODEID", "LOGID", "MSG"},
			new ConvexType[]{ConvexType.INTEGER, ConvexType.INTEGER, ConvexType.VARCHAR}, 2);
		return schema;
	}

	private static List<ACell> key(long nodeId, long logId) {
		return List.of(CVMLong.create(nodeId), CVMLong.create(logId));
	}

	private static void fill(SQLSchema schema, int from, int to) {
		for (int i = from; i <= to; i++) {
			schema.insert("log", Vectors.of(CVMLong.create(1), CVMLong.create(i), "row " + i));
		}
	}

	/** The rows Index of {@code state} regrouped the way the old rule stored it. */
	@SuppressWarnings("unchecked")
	private static AVector<ACell> toLegacyLayout(AVector<ACell> state) {
		Index<ABlob, ACell> rows = (Index<ABlob, ACell>) state.get(SQLTable.POS_ROWS);
		Index<ABlob, ACell>[] legacy = new Index[] { BlockTableLattice.INSTANCE.zero() };
		rows.forEach((bk, block) -> RowBlock.forEach(block, (pk, row) -> {
			ABlob old = RowBlock.blockKey(pk);
			legacy[0] = legacy[0].assoc(old, RowBlock.put(legacy[0].get(old), pk, row));
		}));
		return state.assoc(SQLTable.POS_ROWS, legacy[0]).assoc(SQLTable.POS_BLOCK_VEC, null);
	}

	private static int largestBlock(Index<ABlob, ACell> rows) {
		int[] max = new int[1];
		rows.forEach((bk, block) -> max[0] = Math.max(max[0], RowBlock.count(block)));
		return max[0];
	}

	@Test
	void everyRowOfACompositeKeyTableIsItsOwnBlock() {
		SQLSchema schema = schemaWithLog();
		fill(schema, 1, N);
		Index<ABlob, ACell> rows = schema.getTable("log").getRows();

		assertEquals(N, rows.count(), "one block per row -- the old rule gave 1 block for all rows");
		assertEquals(1, largestBlock(rows));
		assertEquals("row 77", schema.selectByKey("log", key(1, 77)).get(2).toString());
		assertNull(schema.selectByKey("log", key(1, N + 1)));
		assertEquals(N, schema.selectAll("log").count());

		assertTrue(schema.deleteByKey("log", key(1, 77)));
		assertNull(schema.selectByKey("log", key(1, 77)));
		assertEquals(N - 1, schema.selectAll("log").count());
	}

	@Test
	void aSingleColumnKeyTableKeepsItsLayout() {
		SQLSchema schema = SQLSchema.create();
		schema.createTable("names", new String[]{"NM", "V"}, new ConvexType[]{ConvexType.VARCHAR, ConvexType.INTEGER}, 1);
		schema.insert("names", Vectors.of("did:key:aaaa", CVMLong.create(1)));
		schema.insert("names", Vectors.of("did:key:bbbb", CVMLong.create(2)));
		SQLTable table = schema.getTable("names");

		assertEquals(1, table.getRows().count(), "same 8-byte prefix, still one block: single-column keys are unchanged");
		assertFalse(table.normalizeBlockLayout());
		assertEquals(2, schema.selectAll("names").count());
	}

	@Test
	void rowsInTheOldLayoutAreStillReadAndAreMovedByTheFirstWrite() {
		SQLSchema schema = schemaWithLog();
		fill(schema, 1, N);
		SQLTable table = schema.getTable("log");
		table.cursor().updateAndGet(CompositeKeyBlockLayoutTest::toLegacyLayout);
		assertEquals(1, table.getRows().count(), "the old layout: every row in one block");

		// reads work before anything is moved
		assertEquals("row 5", schema.selectByKey("log", key(1, 5)).get(2).toString());
		assertEquals(N, schema.selectAll("log").count());
		assertEquals(1, table.getRows().count());

		// the first write moves the whole table
		schema.insert("log", Vectors.of(CVMLong.create(1), CVMLong.create(N + 1), "new"));
		Index<ABlob, ACell> rows = table.getRows();
		assertEquals(N + 1, rows.count());
		assertEquals(1, largestBlock(rows));
		assertEquals(N + 1, schema.selectAll("log").count());
		assertEquals("row 5", schema.selectByKey("log", key(1, 5)).get(2).toString());
		assertEquals("new", schema.selectByKey("log", key(1, N + 1)).get(2).toString());

		// an overwrite and a delete of moved rows hit the right block
		schema.insert("log", Vectors.of(CVMLong.create(1), CVMLong.create(5), "changed"));
		assertEquals("changed", schema.selectByKey("log", key(1, 5)).get(2).toString());
		assertTrue(schema.deleteByKey("log", key(1, 6)));
		assertEquals(N, schema.selectAll("log").count());
	}

	@Test
	void aDeleteOnTheOldLayoutMovesTheTableAndKeepsTheTombstone() {
		SQLSchema schema = schemaWithLog();
		fill(schema, 1, 20);
		SQLTable table = schema.getTable("log");
		table.cursor().updateAndGet(CompositeKeyBlockLayoutTest::toLegacyLayout);

		assertTrue(schema.deleteByKey("log", key(1, 3)));
		assertNull(schema.selectByKey("log", key(1, 3)));
		assertEquals(19, schema.selectAll("log").count());
		assertEquals(20, table.getRows().count(), "19 live rows and one tombstone, each its own block");
		assertEquals(19, SQLTable.getLiveCount(table.getState()));
	}

	@Test
	void normalizeMovesAnOldLayoutTableOnceAndKeepsDeletedRowsDeleted() {
		SQLSchema schema = schemaWithLog();
		fill(schema, 1, 50);
		schema.deleteByKey("log", key(1, 10));
		SQLTable table = schema.getTable("log");
		table.cursor().updateAndGet(CompositeKeyBlockLayoutTest::toLegacyLayout);

		assertTrue(table.normalizeBlockLayout());
		assertFalse(table.normalizeBlockLayout(), "nothing left to move");
		assertEquals(50, table.getRows().count());
		assertEquals(49, schema.selectAll("log").count());
		assertEquals(49, SQLTable.getLiveCount(table.getState()));
		assertNull(schema.selectByKey("log", key(1, 10)), "the tombstone moved with the rows");
		assertNotNull(schema.selectByKey("log", key(1, 11)));
	}

	@Test
	@SuppressWarnings("unchecked")
	void mergingAnOldLayoutCopyWithANewLayoutCopyGivesEachRowOnce() {
		SQLSchema a = schemaWithLog();
		fill(a, 1, 30);
		AVector<ACell> shared = a.getTable("log").getState();

		// replica B: same table, still in the old layout, with one row changed later and one added
		SQLSchema b = schemaWithLog();
		SQLTable tb = b.getTable("log");
		tb.cursor().updateAndGet(s -> toLegacyLayout(shared));
		AVector<ACell> oldLayout = tb.getState();
		assertEquals(1, ((Index<ABlob, ACell>) oldLayout.get(SQLTable.POS_ROWS)).count());

		// replica A moves on in the new layout
		a.insert("log", Vectors.of(CVMLong.create(1), CVMLong.create(7), "changed on A"));
		a.insert("log", Vectors.of(CVMLong.create(1), CVMLong.create(31), "added on A"));
		AVector<ACell> newLayout = a.getTable("log").getState();

		for (AVector<ACell> merged : List.of(SQLTable.merge(newLayout, oldLayout), SQLTable.merge(oldLayout, newLayout))) {
			Index<ABlob, ACell> rows = (Index<ABlob, ACell>) merged.get(SQLTable.POS_ROWS);
			assertEquals(31, rows.count(), "31 rows, each once");
			assertEquals(1, largestBlock(rows));
			assertEquals(31, SQLTable.computeLiveCount(rows));
			assertEquals(31, ((CVMLong) merged.get(SQLTable.POS_LIVE_COUNT)).longValue());
			AVector<ACell> row7 = SQLTable.findEntry(merged, rows, a.toCompositeKey(Vectors.of(CVMLong.create(1), CVMLong.create(7)), 2));
			assertEquals("changed on A", SQLRow.getValues(row7).get(2).toString(), "the later write wins");
		}
	}
}
