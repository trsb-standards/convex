package convex.db.lattice;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

import convex.core.data.Vectors;
import convex.core.data.prim.CVMLong;
import convex.db.ConvexDB;
import convex.db.calcite.ConvexColumnType;
import convex.db.calcite.ConvexType;

/** ALTER TABLE ... ADD COLUMN: one nullable column at the end; existing rows read it as null. */
class AddColumnTest {

	@Test
	void addColumnAppendsANullableColumnAndKeepsTheRows() {
		SQLSchema s = SQLSchema.create();
		s.createTable("tool", new String[] {"TOOLID", "NM"}, new ConvexType[] {ConvexType.INTEGER, ConvexType.VARCHAR});
		s.insert("tool", Vectors.of(CVMLong.create(1), "myjobs"));

		assertEquals(true, s.addColumn("tool", "ISSTACKED", ConvexColumnType.of(ConvexType.INTEGER)));
		String[] cols = s.getColumnNames("tool");
		assertEquals("ISSTACKED", cols[cols.length - 1]);
		assertEquals("myjobs", s.selectByKey("tool", CVMLong.create(1)).get(1).toString(), "existing row kept");

		s.insert("tool", Vectors.of(CVMLong.create(2), "other", null, CVMLong.create(1)));
		assertEquals(1L, ((CVMLong) s.selectByKey("tool", CVMLong.create(2)).get(3)).longValue());

		assertThrows(IllegalArgumentException.class, () -> s.addColumn("tool", "isstacked", ConvexColumnType.of(ConvexType.INTEGER)));
		assertFalse(s.addColumn("no_such_table", "X", ConvexColumnType.of(ConvexType.INTEGER)));
	}

	@Test
	void alterTableAddColumnOverSql() throws Exception {
		String db = "addcol" + System.nanoTime();
		ConvexDB cdb = ConvexDB.create();
		cdb.database(db);
		cdb.register(db);
		try (Connection c = DriverManager.getConnection("jdbc:convex:database=" + db); Statement st = c.createStatement()) {
			st.execute("CREATE TABLE tool (TOOLID INTEGER, NM VARCHAR, PRIMARY KEY (TOOLID))");
			st.execute("INSERT INTO tool (TOOLID, NM) VALUES (1, 'myjobs')");
			st.execute("ALTER TABLE tool ADD COLUMN isstacked INTEGER");
			st.execute("ALTER TABLE tool ADD note VARCHAR(40)");
			st.execute("UPDATE tool SET ISSTACKED = 1 WHERE TOOLID = 1");
			try (ResultSet rs = st.executeQuery("SELECT TOOLID, NM, ISSTACKED, NOTE FROM tool")) {
				rs.next();
				assertEquals("myjobs", rs.getString("NM"));
				assertEquals(1, rs.getInt("ISSTACKED"));
				assertNull(rs.getString("NOTE"));
			}
			assertThrows(Exception.class, () -> st.execute("ALTER TABLE tool ADD COLUMN NOTE VARCHAR"), "already there");
			assertThrows(Exception.class, () -> st.execute("ALTER TABLE tool ADD COLUMN X BLOBBY"), "unknown type");
		} finally {
			cdb.unregister(db);
		}
	}
}
