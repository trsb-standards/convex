package convex.db.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import convex.db.ConvexDB;

/**
 * CREATE TABLE's table-level {@code PRIMARY KEY (...)} clause: it used to be
 * skipped altogether, so every SQL-created table was keyed on its first
 * column alone and a declared composite key silently lost its other columns
 * (found via fw9.roletool / fw9.entityrole, 2026-10-05).
 */
public class ConvexDdlPrimaryKeyTest {

    private static int counter = 0;
    private String dbName;
    private ConvexDB cdb;

    @BeforeEach
    void setUp() {
        dbName = "ddlpk_" + (++counter) + "_" + System.currentTimeMillis();
        cdb = ConvexDB.create();
        cdb.database(dbName).tables();
        cdb.register(dbName);
    }

    @AfterEach
    void tearDown() {
        cdb.unregister(dbName);
    }

    private Connection connect() throws Exception {
        return DriverManager.getConnection("jdbc:convex:database=" + dbName);
    }

    private static int count(Statement stmt, String table) throws Exception {
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    @Test
    void compositePrimaryKeyKeysOnAllItsColumns() throws Exception {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE roletool (ROLEID INTEGER, TOOLID INTEGER, PRIMARY KEY (ROLEID, TOOLID))");
            assertEquals(2, cdb.database(dbName).tables().getLiveTable("ROLETOOL").getPkCount());

            stmt.executeUpdate("INSERT INTO roletool (ROLEID, TOOLID) VALUES (1, 10)");
            stmt.executeUpdate("INSERT INTO roletool (ROLEID, TOOLID) VALUES (1, 11)");
            stmt.executeUpdate("INSERT INTO roletool (ROLEID, TOOLID) VALUES (2, 10)");
            assertEquals(3, count(stmt, "roletool"));
        }
    }

    @Test
    void singleColumnPrimaryKeyIsUnchanged() throws Exception {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE tool (TOOLID INTEGER, NM VARCHAR, PRIMARY KEY (TOOLID))");
            assertEquals(1, cdb.database(dbName).tables().getLiveTable("TOOL").getPkCount());
            stmt.executeUpdate("CREATE TABLE nokey (A INTEGER, B INTEGER)");
            assertEquals(1, cdb.database(dbName).tables().getLiveTable("NOKEY").getPkCount());
        }
    }

    @Test
    void primaryKeyMustBeTheLeadingColumns() throws Exception {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            Exception e = assertThrows(Exception.class, () ->
                stmt.executeUpdate("CREATE TABLE bad (A INTEGER, B INTEGER, C INTEGER, PRIMARY KEY (B))"));
            assertTrue(String.valueOf(e.getMessage()).contains("leading columns")
                    || String.valueOf(e.getCause()).contains("leading columns"), String.valueOf(e));
            assertThrows(Exception.class, () ->
                stmt.executeUpdate("CREATE TABLE bad2 (A INTEGER, B INTEGER, PRIMARY KEY (B, A))"));
        }
    }
}
