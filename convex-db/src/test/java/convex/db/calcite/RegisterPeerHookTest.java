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
import convex.db.calcite.ConvexDdlExecutor.PeerRegistration;

/**
 * Covers the {@code REGISTER PEER '<host>' <port> '<keyHex>'} statement —
 * regex-intercepted in {@code ConvexMeta} (same trick as {@code REPLICATE
 * DB}/CREATE/DROP INDEX, since Calcite has no native grammar for it) and
 * dispatched to {@link ConvexDdlExecutor#onRegisterPeer} via
 * {@link ConvexDdlExecutor#fireRegisterPeer}.
 *
 * <p>Exists to close the "ongoing gossip is one-directional" gap: a node
 * that pulls from a peer used to have no way to tell that peer to push its
 * own future writes back. See {@code dbase.DbaseServer.registerAsPeerWith}
 * for the real caller.
 */
public class RegisterPeerHookTest {

    private static int counter = 0;
    private String dbName;
    private List<PeerRegistration> firedWith;

    @BeforeEach
    void setUp() {
        dbName = "registerpeerhook_" + (++counter) + "_" + System.currentTimeMillis();
        firedWith = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        // onRegisterPeer is a process-wide static field -- must be cleared
        // so this test's callback doesn't leak into unrelated tests running
        // later in the same JVM.
        ConvexDdlExecutor.onRegisterPeer = null;
    }

    @Test
    void firesWithQuotedHostAndKey() throws Exception {
        ConvexDdlExecutor.onRegisterPeer = firedWith::add;

        ConvexDB cdb = ConvexDB.create();
        cdb.database(dbName).tables();
        cdb.register(dbName);

        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             Statement stmt = conn.createStatement()) {
            stmt.execute("REGISTER PEER 'otherhost' 19651 'deadbeef01'");
        } finally {
            cdb.unregister(dbName);
        }

        assertEquals(1, firedWith.size());
        assertEquals("otherhost", firedWith.get(0).host());
        assertEquals(19651, firedWith.get(0).port());
        assertEquals("deadbeef01", firedWith.get(0).accountKeyHex());
    }

    @Test
    void firesWithUnquotedHostAndKey() throws Exception {
        ConvexDdlExecutor.onRegisterPeer = firedWith::add;

        ConvexDB cdb = ConvexDB.create();
        cdb.database(dbName).tables();
        cdb.register(dbName);

        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             Statement stmt = conn.createStatement()) {
            stmt.execute("REGISTER PEER otherhost 19651 deadbeef01");
        } finally {
            cdb.unregister(dbName);
        }

        assertEquals(1, firedWith.size());
        assertEquals("otherhost", firedWith.get(0).host());
        assertEquals(19651, firedWith.get(0).port());
        assertEquals("deadbeef01", firedWith.get(0).accountKeyHex());
    }

    @Test
    void firesWithADottedHostname() throws Exception {
        ConvexDdlExecutor.onRegisterPeer = firedWith::add;

        ConvexDB cdb = ConvexDB.create();
        cdb.database(dbName).tables();
        cdb.register(dbName);

        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             Statement stmt = conn.createStatement()) {
            stmt.execute("REGISTER PEER 'node-2.internal.example.com' 19651 'deadbeef01'");
        } finally {
            cdb.unregister(dbName);
        }

        assertEquals(1, firedWith.size());
        assertEquals("node-2.internal.example.com", firedWith.get(0).host());
    }

    @Test
    void throwsAClearErrorWhenNoHandlerIsRegistered() throws Exception {
        ConvexDB cdb = ConvexDB.create();
        cdb.database(dbName).tables();
        cdb.register(dbName);

        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             Statement stmt = conn.createStatement()) {
            assertThrows(SQLException.class, () -> stmt.execute("REGISTER PEER otherhost 19651 deadbeef01"));
        } finally {
            cdb.unregister(dbName);
        }
    }

    @Test
    void handlerFailurePropagatesToTheClientRatherThanBeingSwallowed() throws Exception {
        ConvexDdlExecutor.onRegisterPeer = registration -> {
            throw new IllegalStateException("Could not reach " + registration.host());
        };

        ConvexDB cdb = ConvexDB.create();
        cdb.database(dbName).tables();
        cdb.register(dbName);

        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             Statement stmt = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class,
                () -> stmt.execute("REGISTER PEER unreachablehost 19651 deadbeef01"));
            assertEquals(true, ex.getMessage().contains("Could not reach unreachablehost"));
        } finally {
            cdb.unregister(dbName);
        }
    }
}
