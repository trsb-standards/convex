package convex.db.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import convex.db.ConvexDB;
import convex.db.lattice.SQLDatabase;

/**
 * A table with a two-column key must not look unique on its first key
 * column alone: the planner then dropped a GROUP BY that included that
 * column, giving one group per row (found 2026-10-25 on perf.querylog, key
 * NODEID, LOGID).
 */
class CompositeKeyGroupByTest {

    private ConvexDB cdb;
    private String dbName;

    @BeforeEach
    void setUp() {
        dbName = "ckgroupby_" + System.nanoTime();
        cdb = ConvexDB.create();
        SQLDatabase db = cdb.database(dbName);
        cdb.register(dbName);
        db.tables().createTable("log", new String[] { "node", "logid", "who" },
            new ConvexType[] { ConvexType.INTEGER, ConvexType.INTEGER, ConvexType.INTEGER }, 2);
        db.tables().insert("log", 1L, 100L, 7L);
        db.tables().insert("log", 1L, 101L, 7L);
        db.tables().insert("log", 1L, 102L, 6L);
        db.tables().insert("log", 2L, 100L, 7L);
    }

    @AfterEach
    void tearDown() {
        cdb.unregister(dbName);
    }

    @Test
    void groupingByTheFirstKeyColumnAndAnotherMergesEqualRows() throws Exception {
        Map<String, Long> groups = new HashMap<>();
        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT node, who, COUNT(*) AS n FROM log GROUP BY node, who")) {
            while (rs.next()) {
                groups.merge(rs.getLong(1) + "/" + rs.getLong(2), rs.getLong(3), Long::sum);
                assertEquals(null, groups.put(rs.getLong(1) + "/" + rs.getLong(2) + "#seen", 1L),
                    "group " + rs.getLong(1) + "/" + rs.getLong(2) + " came back more than once");
            }
        }
        assertEquals(2L, groups.get("1/7"));
        assertEquals(1L, groups.get("1/6"));
        assertEquals(1L, groups.get("2/7"));
    }

    @Test
    void groupingByTheFirstKeyColumnAloneCounts() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT node, COUNT(*) AS n FROM log GROUP BY node ORDER BY node")) {
            rs.next();
            assertEquals(3L, rs.getLong(2));
            rs.next();
            assertEquals(1L, rs.getLong(2));
        }
    }
}
