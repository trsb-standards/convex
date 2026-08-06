package convex.db.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import convex.db.ConvexDB;

/**
 * Covers the {@code REPLICATE DB "name"} statement — regex-intercepted in
 * {@code ConvexMeta} (same trick as CREATE/DROP INDEX, since Calcite has no
 * native grammar for it) and dispatched to
 * {@link ConvexDdlExecutor#onReplicateDb} via {@link ConvexDdlExecutor#fireReplicateDb}.
 *
 * <p>Unlike {@code onDdlExecuted}, a failing handler here must NOT be
 * swallowed — REPLICATE DB IS the operation, so its failure must reach the
 * client as a real query error.
 */
public class ReplicateDbHookTest {

    private static int counter = 0;
    private String dbName;
    private List<String> firedWith;

    @BeforeEach
    void setUp() {
        dbName = "replicatehook_" + (++counter) + "_" + System.currentTimeMillis();
        firedWith = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        // onReplicateDb is a process-wide static field — must be cleared so
        // this test's callback doesn't leak into unrelated tests running
        // later in the same JVM.
        ConvexDdlExecutor.onReplicateDb = null;
    }

    @Test
    void firesWithTheQuotedDbName() throws Exception {
        ConvexDdlExecutor.onReplicateDb = firedWith::add;

        ConvexDB cdb = ConvexDB.create();
        cdb.database(dbName).tables();
        cdb.register(dbName);

        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             Statement stmt = conn.createStatement()) {
            stmt.execute("REPLICATE DB \"otherdb\"");
        } finally {
            cdb.unregister(dbName);
        }

        assertEquals(1, firedWith.size());
        assertEquals("otherdb", firedWith.get(0));
    }

    @Test
    void firesWithAnUnquotedDbName() throws Exception {
        ConvexDdlExecutor.onReplicateDb = firedWith::add;

        ConvexDB cdb = ConvexDB.create();
        cdb.database(dbName).tables();
        cdb.register(dbName);

        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             Statement stmt = conn.createStatement()) {
            stmt.execute("REPLICATE DB otherdb");
        } finally {
            cdb.unregister(dbName);
        }

        assertEquals(1, firedWith.size());
        assertEquals("otherdb", firedWith.get(0));
    }

    @Test
    void throwsAClearErrorWhenNoHandlerIsRegistered() throws Exception {
        ConvexDB cdb = ConvexDB.create();
        cdb.database(dbName).tables();
        cdb.register(dbName);

        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             Statement stmt = conn.createStatement()) {
            assertThrows(SQLException.class, () -> stmt.execute("REPLICATE DB otherdb"));
        } finally {
            cdb.unregister(dbName);
        }
    }

    @Test
    void handlerFailurePropagatesToTheClientRatherThanBeingSwallowed() throws Exception {
        ConvexDdlExecutor.onReplicateDb = name -> {
            throw new IllegalArgumentException("No such db: " + name);
        };

        ConvexDB cdb = ConvexDB.create();
        cdb.database(dbName).tables();
        cdb.register(dbName);

        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             Statement stmt = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () -> stmt.execute("REPLICATE DB nosuchdb"));
            assertEquals(true, ex.getMessage().contains("No such db: nosuchdb"));
        } finally {
            cdb.unregister(dbName);
        }
    }
}
