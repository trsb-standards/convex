package convex.db.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import convex.db.ConvexDB;

/**
 * Covers {@link ConvexDdlExecutor#onDdlExecuted} — the hook a caller (e.g.
 * dbase.DbaseServer) registers to refresh its own metadata catalog and
 * re-announce to peers after a live DDL statement, since neither happens on
 * its own. CREATE TABLE/CREATE SCHEMA/DROP TABLE fire it directly from
 * ConvexDdlExecutor; CREATE INDEX/DROP INDEX fire it via a separate call from
 * {@code ConvexMeta} (they're intercepted by regex before ever reaching
 * ConvexDdlExecutor — Calcite's DDL parser has no native CREATE INDEX
 * grammar here) — both paths are covered below.
 */
public class ConvexDdlExecutorHookTest {

    private static int counter = 0;
    private String dbName;
    private List<ConvexDB> firedWith;

    @BeforeEach
    void setUp() {
        dbName = "ddlhook_" + (++counter) + "_" + System.currentTimeMillis();
        firedWith = new ArrayList<>();
        ConvexDdlExecutor.onDdlExecuted = firedWith::add;
    }

    @AfterEach
    void tearDown() {
        // onDdlExecuted is a process-wide static field — must be cleared so
        // this test's callback doesn't leak into unrelated tests running
        // later in the same JVM.
        ConvexDdlExecutor.onDdlExecuted = null;
        ConvexDB.lookup(dbName); // no-op if never registered; harmless
    }

    @Test
    void firesForCreateSchema() throws Exception {
        ConvexDB cdb = ConvexDB.create();
        cdb.database(dbName + "_base").tables();
        cdb.register(dbName + "_base");

        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName + "_base");
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE SCHEMA " + dbName + "_new");
        } finally {
            cdb.unregister(dbName + "_base");
            cdb.unregister(dbName + "_new");
        }

        assertEquals(1, firedWith.size());
        assertSame(cdb, firedWith.get(0));
    }

    @Test
    void firesForCreateTable() throws Exception {
        ConvexDB cdb = ConvexDB.create();
        cdb.database(dbName).tables();
        cdb.register(dbName);

        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE widgets (id INTEGER, name VARCHAR)");
        } finally {
            cdb.unregister(dbName);
        }

        assertEquals(1, firedWith.size());
        assertSame(cdb, firedWith.get(0));
    }

    @Test
    void firesForDropTable() throws Exception {
        ConvexDB cdb = ConvexDB.create();
        cdb.database(dbName).tables().createTable("widgets",
                new String[]{"id"}, new ConvexType[]{ConvexType.INTEGER});
        cdb.register(dbName);

        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             Statement stmt = conn.createStatement()) {
            // CREATE TABLE already happened natively above (not via SQL), so
            // the only fire expected here is from DROP TABLE itself.
            stmt.executeUpdate("DROP TABLE widgets");
        } finally {
            cdb.unregister(dbName);
        }

        assertEquals(1, firedWith.size());
        assertSame(cdb, firedWith.get(0));
    }

    @Test
    void firesForCreateIndexAndDropIndex() throws Exception {
        // CREATE INDEX/DROP INDEX never go through ConvexDdlExecutor at all —
        // they're intercepted via regex in ConvexMeta, which must fire the
        // same hook itself. This is the one path that would NOT be covered
        // by ConvexDdlExecutor's own three execute(...) overloads.
        ConvexDB cdb = ConvexDB.create();
        cdb.database(dbName).tables().createTable("widgets",
                new String[]{"id", "status"},
                new ConvexType[]{ConvexType.INTEGER, ConvexType.VARCHAR});
        cdb.register(dbName);

        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE INDEX widgets_status_idx ON widgets (status)");
            assertEquals(1, firedWith.size());
            assertSame(cdb, firedWith.get(0));

            stmt.execute("DROP INDEX widgets_status_idx");
            assertEquals(2, firedWith.size());
            assertSame(cdb, firedWith.get(1));
        } finally {
            cdb.unregister(dbName);
        }
    }

    @Test
    void firesForCreateIndexWithASchemaQualifiedTableName() throws Exception {
        // Regression coverage for the qualified-name regex fix: "ON schema.table"
        // previously failed to match at all and fell through to Calcite's real
        // parser (no CREATE INDEX grammar there), producing a confusing parse
        // error instead of resolving against the named schema.
        ConvexDB cdb = ConvexDB.create();
        cdb.database(dbName + "_target").tables().createTable("widgets",
                new String[]{"id", "status"},
                new ConvexType[]{ConvexType.INTEGER, ConvexType.VARCHAR});
        cdb.register(dbName + "_target");
        cdb.database(dbName + "_conn").tables();
        cdb.register(dbName + "_conn");

        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName + "_conn");
             Statement stmt = conn.createStatement()) {
            // Connected to "_conn" (which has no "widgets" table at all), but
            // qualifying the target lets it resolve correctly anyway.
            stmt.execute("CREATE INDEX widgets_status_idx ON " + dbName + "_target.widgets (status)");
        } finally {
            cdb.unregister(dbName + "_target");
            cdb.unregister(dbName + "_conn");
        }

        assertEquals(1, firedWith.size());
        assertSame(cdb, firedWith.get(0));
        assertTrue(cdb.database(dbName + "_target").tables().hasIndex("widgets", "status"));
    }
}
