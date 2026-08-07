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
import convex.db.calcite.ConvexDdlExecutor.SchemaReplication;

/**
 * Covers the {@code REPLICATE SCHEMA "<db>.<schema>"} statement —
 * regex-intercepted in {@code ConvexMeta} (same trick as {@code REPLICATE
 * DB}/CREATE/DROP INDEX, since Calcite has no native grammar for it) and
 * dispatched to {@link ConvexDdlExecutor#onReplicateSchema} via
 * {@link ConvexDdlExecutor#fireReplicateSchema}.
 *
 * <p>Exists to replicate a single schema within a db, rather than every
 * schema that db holds (unlike {@code REPLICATE DB}). See
 * {@code dbase.DbaseServer.onReplicateSchema} for the real caller and its
 * "only has effect when REPLICATE DB wasn't already issued" guard.
 */
public class ReplicateSchemaHookTest {

    private static int counter = 0;
    private String dbName;
    private List<SchemaReplication> firedWith;

    @BeforeEach
    void setUp() {
        dbName = "replicateschemahook_" + (++counter) + "_" + System.currentTimeMillis();
        firedWith = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        // onReplicateSchema is a process-wide static field -- must be cleared
        // so this test's callback doesn't leak into unrelated tests running
        // later in the same JVM.
        ConvexDdlExecutor.onReplicateSchema = null;
    }

    @Test
    void firesWithQuotedDbAndSchema() throws Exception {
        ConvexDdlExecutor.onReplicateSchema = firedWith::add;

        ConvexDB cdb = ConvexDB.create();
        cdb.database(dbName).tables();
        cdb.register(dbName);

        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             Statement stmt = conn.createStatement()) {
            stmt.execute("REPLICATE SCHEMA 'default.ose'");
        } finally {
            cdb.unregister(dbName);
        }

        assertEquals(1, firedWith.size());
        assertEquals("default", firedWith.get(0).dbName());
        assertEquals("ose", firedWith.get(0).schemaName());
    }

    @Test
    void firesWithUnquotedDbAndSchema() throws Exception {
        ConvexDdlExecutor.onReplicateSchema = firedWith::add;

        ConvexDB cdb = ConvexDB.create();
        cdb.database(dbName).tables();
        cdb.register(dbName);

        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             Statement stmt = conn.createStatement()) {
            stmt.execute("REPLICATE SCHEMA default.ose");
        } finally {
            cdb.unregister(dbName);
        }

        assertEquals(1, firedWith.size());
        assertEquals("default", firedWith.get(0).dbName());
        assertEquals("ose", firedWith.get(0).schemaName());
    }

    @Test
    void throwsAClearErrorWhenNoHandlerIsRegistered() throws Exception {
        ConvexDB cdb = ConvexDB.create();
        cdb.database(dbName).tables();
        cdb.register(dbName);

        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             Statement stmt = conn.createStatement()) {
            assertThrows(SQLException.class, () -> stmt.execute("REPLICATE SCHEMA default.ose"));
        } finally {
            cdb.unregister(dbName);
        }
    }

    @Test
    void handlerFailurePropagatesToTheClientRatherThanBeingSwallowed() throws Exception {
        ConvexDdlExecutor.onReplicateSchema = registration -> {
            throw new IllegalArgumentException("No such db: \"" + registration.dbName() + "\"");
        };

        ConvexDB cdb = ConvexDB.create();
        cdb.database(dbName).tables();
        cdb.register(dbName);

        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             Statement stmt = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class,
                () -> stmt.execute("REPLICATE SCHEMA nosuchdb.ose"));
            assertEquals(true, ex.getMessage().contains("No such db: \"nosuchdb\""));
        } finally {
            cdb.unregister(dbName);
        }
    }
}
