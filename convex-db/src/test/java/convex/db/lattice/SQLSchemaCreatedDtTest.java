package convex.db.lattice;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;

import org.junit.jupiter.api.Test;

import convex.core.data.ACell;
import convex.core.data.AString;
import convex.core.data.AVector;
import convex.core.data.Strings;
import convex.core.data.Vectors;
import convex.core.data.prim.CVMLong;
import convex.db.ConvexDB;
import convex.db.calcite.ConvexColumnType;
import convex.db.calcite.ConvexType;

/**
 * The optional creation-time column {@link SQLSchema#CREATEDT_COLUMN}
 * (phase 3 of PLAN_owner_tree_replication.md): stamped once when a row is
 * new unless the INSERT gives it, kept by every later write, never changed by UPDATE.
 */
class SQLSchemaCreatedDtTest {

	private static final AString T = Strings.create("t");

	private static SQLSchema table(boolean autoIncrement) {
		SQLSchema schema = SQLSchema.create();
		schema.createTable(T, new String[]{"ID", "VAL"},
			new ConvexColumnType[]{ConvexColumnType.of(ConvexType.INTEGER), ConvexColumnType.of(ConvexType.VARCHAR)},
			1, false, autoIncrement);
		assertTrue(schema.addCreatedDt(T));
		return schema;
	}

	private static int col(SQLSchema schema) {
		return SQLSchema.createdDtIndex(schema.getLiveTable(T));
	}

	private static long createdDt(SQLSchema schema, long id) {
		AVector<ACell> row = schema.selectByKey(T, CVMLong.create(id));
		return ((CVMLong) row.get(col(schema))).longValue();
	}

	@Test
	void columnComesAfterOwnerIdAndOnlyOnce() {
		SQLSchema schema = table(false);
		String[] cols = schema.getColumnNames(T);
		assertEquals(SQLSchema.OWNERID_COLUMN, cols[cols.length - 2]);
		assertEquals(SQLSchema.CREATEDT_COLUMN, cols[cols.length - 1]);
		assertTrue(schema.addCreatedDt(T));
		assertEquals(cols.length, schema.getColumnNames(T).length);
	}

	@Test
	void aNewRowIsStampedAndAnUpsertKeepsTheTime() throws Exception {
		SQLSchema schema = table(false);
		long before = System.currentTimeMillis();
		schema.insert("t", CVMLong.create(1), "first");
		long stamped = createdDt(schema, 1);
		assertTrue(stamped >= before && stamped <= System.currentTimeMillis());

		Thread.sleep(5);
		// A rewrite of the row that leaves the column empty keeps the stored time
		schema.insert("t", CVMLong.create(1), "changed");
		assertEquals("changed", schema.selectByKey(T, CVMLong.create(1)).get(1).toString());
		assertEquals(stamped, createdDt(schema, 1));
	}

	@Test
	void aGivenTimeIsKept() {
		SQLSchema schema = table(false);
		schema.insert(T, Vectors.of(CVMLong.create(1), Strings.create("restored"), null, CVMLong.create(12345)));
		assertEquals(12345, createdDt(schema, 1));
	}

	@Test
	void generatedKeysAndBatchInsertsAreStamped() {
		SQLSchema schema = table(true);
		ACell key = schema.insertReturningKey("t", null, "generated");
		assertNotNull(schema.selectByKey(T, key).get(col(schema)));
		schema.insertAll(T, java.util.List.of(Vectors.of(CVMLong.create(100), Strings.create("batch"))));
		assertNotNull(schema.selectByKey(T, CVMLong.create(100)).get(col(schema)));
	}

	@Test
	void rowsOlderThanTheColumnKeepNull() {
		SQLSchema schema = SQLSchema.create();
		schema.createTable(T, new String[]{"ID", "VAL"},
			new ConvexColumnType[]{ConvexColumnType.of(ConvexType.INTEGER), ConvexColumnType.of(ConvexType.VARCHAR)});
		schema.insert("t", CVMLong.create(1), "old");
		schema.addCreatedDt(T);
		schema.insert("t", CVMLong.create(1), "updated later");
		assertNull(schema.selectByKey(T, CVMLong.create(1)).get(col(schema)));
	}

	@Test
	void sqlStampsItAndRefusesToSetOrChangeIt() throws Exception {
		ConvexDB cdb = ConvexDB.create();
		cdb.database("createddt_test").tables();
		cdb.register("createddt_test");
		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=createddt_test");
				Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("CREATE TABLE a (id INTEGER, name VARCHAR(20)) CREATEDT AUTOINCREMENT");
			stmt.executeUpdate("CREATE TABLE b (id INTEGER, name VARCHAR(20))");
			stmt.executeUpdate("INSERT INTO b VALUES (1, 'old')");
			stmt.executeUpdate("ALTER TABLE b CREATEDT");

			long before = System.currentTimeMillis();
			stmt.executeUpdate("INSERT INTO a (name) VALUES ('x')");
			stmt.executeUpdate("INSERT INTO b VALUES (2, 'bare values')");
			try (ResultSet rs = stmt.executeQuery("SELECT createdt FROM a")) {
				assertTrue(rs.next());
				Timestamp ts = rs.getTimestamp(1);
				assertTrue(ts.getTime() >= before);
			}
			try (ResultSet rs = stmt.executeQuery("SELECT id, createdt FROM b ORDER BY id")) {
				assertTrue(rs.next());
				assertNull(rs.getTimestamp(2), "a row older than the column has no creation time");
				assertTrue(rs.next());
				assertNotNull(rs.getTimestamp(2));
			}
			long stamped;
			try (ResultSet rs = stmt.executeQuery("SELECT createdt FROM b WHERE id = 2")) {
				rs.next();
				stamped = rs.getTimestamp(1).getTime();
			}
			stmt.executeUpdate("UPDATE b SET name = 'renamed' WHERE id = 2");
			try (ResultSet rs = stmt.executeQuery("SELECT createdt FROM b WHERE id = 2")) {
				rs.next();
				assertEquals(stamped, rs.getTimestamp(1).getTime());
			}

			// A given time is kept (a restore carries the real creation time)
			stmt.executeUpdate("INSERT INTO b (id, name, createdt) VALUES (3, 'restored', TIMESTAMP '2020-01-01 00:00:00')");
			try (ResultSet rs = stmt.executeQuery("SELECT createdt FROM b WHERE id = 3")) {
				rs.next();
				assertEquals(Timestamp.valueOf("2020-01-01 00:00:00").getTime(), rs.getTimestamp(1).getTime());
			}
			assertThrows(Exception.class,
				() -> stmt.executeUpdate("UPDATE b SET createdt = TIMESTAMP '2020-01-01 00:00:00' WHERE id = 2"));
			assertThrows(Exception.class,
				() -> stmt.executeUpdate("CREATE TABLE c (id INTEGER, createdt TIMESTAMP)"));
			assertThrows(Exception.class,
				() -> stmt.executeUpdate("ALTER TABLE b ADD COLUMN createdt TIMESTAMP"));
		} finally {
			cdb.unregister("createddt_test");
		}
	}
}
