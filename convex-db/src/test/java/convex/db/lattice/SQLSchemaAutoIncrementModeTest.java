package convex.db.lattice;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import convex.core.data.ACell;
import convex.core.data.AString;
import convex.core.data.Blob;
import convex.core.data.Strings;
import convex.core.data.Vectors;
import convex.core.data.prim.CVMLong;
import convex.db.ConvexDB;
import convex.db.calcite.ConvexColumnType;
import convex.db.calcite.ConvexType;

/**
 * The NODE and TIME key modes of AUTOINCREMENT (phase 3 of
 * PLAN_owner_tree_replication.md): keys carry the node number in their low
 * {@link AutoIncrementMode#NODE_BITS} bits, so nodes that cannot see each
 * other never generate the same key; TIME keys also sort by creation and
 * are always above every key the node has received.
 */
class SQLSchemaAutoIncrementModeTest {

	private static final long BITS = AutoIncrementMode.NODE_BITS;

	private final Long nodeBefore = AutoIncrementCounters.getNodeNumber();

	@AfterEach
	void restoreNodeNumber() {
		if (nodeBefore != null) AutoIncrementCounters.setNodeNumber(nodeBefore);
	}

	private static SQLSchema table(String name, AutoIncrementMode mode) {
		SQLSchema schema = SQLSchema.create();
		schema.createTable(Strings.create(name),
			new String[]{"ID", "VAL"},
			new ConvexColumnType[]{
				ConvexColumnType.of(ConvexType.INTEGER),
				ConvexColumnType.of(ConvexType.VARCHAR)},
			1, false, mode);
		return schema;
	}

	private static long insert(SQLSchema schema, String table, String val) {
		ACell key = schema.insertReturningKey(table, null, val);
		assertNotNull(key);
		return ((CVMLong) key).longValue();
	}

	private static Blob keyBlob(long v) {
		byte[] b = new byte[8];
		for (int i = 7; i >= 0; i--) { b[i] = (byte) v; v >>= 8; }
		return Blob.wrap(b);
	}

	@Test
	void nodeKeysCountUpWithTheNodeNumberInTheLowBits() {
		AutoIncrementCounters.setNodeNumber(5);
		SQLSchema schema = table("t", AutoIncrementMode.NODE);
		assertEquals((1L << BITS) | 5, insert(schema, "t", "a"));
		assertEquals((2L << BITS) | 5, insert(schema, "t", "b"));
		assertEquals((3L << BITS) | 5, insert(schema, "t", "c"));
	}

	@Test
	void nodeCounterIgnoresOtherNodesKeysButFollowsItsOwn() {
		AutoIncrementCounters.setNodeNumber(5);
		SQLSchema schema = table("t", AutoIncrementMode.NODE);
		// Another node's key, far ahead, does not move this node's counter
		schema.insert("t", Vectors.of(CVMLong.create((100L << BITS) | 9), "other node"));
		assertEquals((1L << BITS) | 5, insert(schema, "t", "first"));
		// An explicit key of this node does
		schema.insert("t", Vectors.of(CVMLong.create((10L << BITS) | 5), "explicit"));
		assertEquals((11L << BITS) | 5, insert(schema, "t", "after explicit"));
	}

	@Test
	void nodeCounterSeedsFromOwnKeysIncludingDeletedOnes() {
		AutoIncrementCounters.setNodeNumber(5);
		SQLSchema schema = table("t", AutoIncrementMode.NODE);
		insert(schema, "t", "a");
		long second = insert(schema, "t", "b");
		schema.deleteByKey(Strings.create("t"), CVMLong.create(second));
		// As after a restart: the counter is seeded again from the table
		AutoIncrementCounters.unmark(schema.getRegistryKey(), Strings.create("t"));
		assertEquals((3L << BITS) | 5, insert(schema, "t", "c"));
	}

	@Test
	void timeKeysFollowTheClockAndCarryTheNodeNumber() {
		AutoIncrementCounters.setNodeNumber(7);
		SQLSchema schema = table("t", AutoIncrementMode.TIME);
		long before = System.currentTimeMillis() - AutoIncrementMode.TIME_EPOCH_MS;
		List<Long> keys = new ArrayList<>();
		for (int i = 0; i < 50; i++) keys.add(insert(schema, "t", "row" + i));
		long after = System.currentTimeMillis() - AutoIncrementMode.TIME_EPOCH_MS;
		for (int i = 0; i < keys.size(); i++) {
			long k = keys.get(i);
			assertEquals(7, k & AutoIncrementMode.MAX_NODE);
			if (i > 0) assertTrue(k > keys.get(i - 1), "keys sort by creation");
		}
		long firstMs = keys.get(0) >>> BITS;
		assertTrue(firstMs >= before && firstMs <= after, "the first key holds the wall clock");
	}

	@Test
	void timeKeyIsAboveARowReceivedFromANodeWithAClockAhead() {
		AutoIncrementCounters.setNodeNumber(7);
		SQLSchema schema = table("t", AutoIncrementMode.TIME);
		insert(schema, "t", "own");
		// A row arriving from node 9, whose clock is an hour ahead -- written
		// straight into the table, as a merge from a peer does, so the
		// counter is not told about it
		long ahead = System.currentTimeMillis() - AutoIncrementMode.TIME_EPOCH_MS + 3_600_000L;
		long foreign = (ahead << BITS) | 9;
		schema.getLiveTable(Strings.create("t")).insertRow(keyBlob(foreign),
			Vectors.of(CVMLong.create(foreign), Strings.create("from node 9"), null), CVMLong.create(1));
		long next = insert(schema, "t", "after receiving");
		assertTrue(next > foreign, "a key is above every key the node has seen");
		assertEquals(7, next & AutoIncrementMode.MAX_NODE);
	}

	@Test
	void timeAndNodeTablesNeedANodeNumber() throws Exception {
		java.lang.reflect.Field f = AutoIncrementCounters.class.getDeclaredField("nodeNumber");
		f.setAccessible(true);
		f.set(null, null);
		SQLSchema schema = table("t", AutoIncrementMode.TIME);
		assertThrows(IllegalStateException.class, () -> insert(schema, "t", "x"));
		// Sequential keys need none
		SQLSchema seq = table("s", AutoIncrementMode.SEQUENTIAL);
		assertEquals(1L, insert(seq, "s", "x"));
	}

	@Test
	void nodeNumberMustFitInTheKey() {
		assertThrows(IllegalArgumentException.class, () -> AutoIncrementCounters.setNodeNumber(1L << BITS));
		assertThrows(IllegalArgumentException.class, () -> AutoIncrementCounters.setNodeNumber(-1));
	}

	@Test
	void keyModeIsFixedOnceSet() {
		AutoIncrementCounters.setNodeNumber(5);
		SQLSchema schema = table("t", AutoIncrementMode.NODE);
		AString t = Strings.create("t");
		assertTrue(schema.convertToAutoIncrement(t, AutoIncrementMode.NODE));
		assertThrows(UnsupportedOperationException.class,
			() -> schema.convertToAutoIncrement(t, AutoIncrementMode.TIME));
		assertThrows(UnsupportedOperationException.class, () -> schema.convertToAutoIncrement(t));
	}

	@Test
	void sqlCreateAndAlterTakeAKeyModeAndCombineWithVersioned() throws Exception {
		AutoIncrementCounters.setNodeNumber(3);
		ConvexDB cdb = ConvexDB.create();
		cdb.database("autoinc_mode_test").tables();
		cdb.register("autoinc_mode_test");
		AString reg = Strings.create("autoinc_mode_test");
		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=autoinc_mode_test");
				Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("CREATE TABLE a (id INTEGER, name VARCHAR(50)) VERSIONED AUTOINCREMENT TIME");
			stmt.executeUpdate("CREATE TABLE b (id INTEGER, name VARCHAR(50)) AUTOINCREMENT NODE VERSIONED");
			stmt.executeUpdate("CREATE TABLE c (id INTEGER, name VARCHAR(50))");
			stmt.executeUpdate("ALTER TABLE c AUTOINCREMENT NODE");
			assertEquals(AutoIncrementMode.TIME, AutoIncrementRegistry.getMode(reg, Strings.create("A")));
			assertEquals(AutoIncrementMode.NODE, AutoIncrementRegistry.getMode(reg, Strings.create("B")));
			assertEquals(AutoIncrementMode.NODE, AutoIncrementRegistry.getMode(reg, Strings.create("C")));
			assertTrue(TableVersionRegistry.isVersioned(reg, Strings.create("A")));
			assertTrue(TableVersionRegistry.isVersioned(reg, Strings.create("B")));

			stmt.executeUpdate("INSERT INTO a (name) VALUES ('x')");
			stmt.executeUpdate("INSERT INTO b (name) VALUES ('y')");
			try (ResultSet rs = stmt.executeQuery("SELECT id FROM a")) {
				assertTrue(rs.next());
				assertEquals(3, rs.getLong(1) & AutoIncrementMode.MAX_NODE);
			}
			try (ResultSet rs = stmt.executeQuery("SELECT id FROM b")) {
				assertTrue(rs.next());
				assertEquals((1L << BITS) | 3, rs.getLong(1));
			}
			try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM a_history")) {
				assertTrue(rs.next());
				assertEquals(1, rs.getInt(1));
			}
			assertThrows(Exception.class, () -> stmt.executeUpdate("ALTER TABLE c AUTOINCREMENT TIME"));
		} finally {
			cdb.unregister("autoinc_mode_test");
		}
	}
}
