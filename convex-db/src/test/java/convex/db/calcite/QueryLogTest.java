package convex.db.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import convex.db.ConvexDB;
import convex.db.calcite.ConvexColumnType;
import convex.db.calcite.ConvexType;
import convex.db.lattice.SQLDatabase;
import convex.db.psql.PgServer;

/**
 * Covers QueryLog.onQueryExecuted, fired from ConvexMeta.prepareAndExecute/
 * execute — Avatica's own dispatch point for every jdbc:convex: statement,
 * regardless of caller. Specifically exercises the two gaps found live
 * 2026-08-07 with an earlier PgProtocolHandler-level attempt at this: direct
 * (non-pgwire) JDBC callers, and the regex-intercepted admin statements
 * (REPLICATE DB/REGISTER PEER/REPLICATE SCHEMA/CREATE INDEX/DROP INDEX) that
 * return before ever reaching Calcite's own execution path.
 */
class QueryLogTest {

    private static final AtomicInteger DB_COUNTER = new AtomicInteger();
    private ConvexDB cdb;
    private SQLDatabase db;
    private String dbName;
    private final List<QueryLog.Event> events = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        dbName = "querylogtest_" + DB_COUNTER.incrementAndGet();
        cdb = ConvexDB.create();
        db = cdb.database(dbName);
        cdb.register(dbName);

        ConvexColumnType[] userTypes = {
            ConvexColumnType.of(ConvexType.INTEGER),
            ConvexColumnType.varchar(50),
        };
        db.tables().createTable("users", new String[]{"id", "name"}, userTypes);
        db.tables().insert("users", 1L, "Alice");

        QueryLog.onQueryExecuted = events::add;
    }

    @AfterEach
    void tearDown() {
        QueryLog.onQueryExecuted = null;
        if (cdb != null) cdb.unregister(dbName);
    }

    @Test
    void firesForADirectJdbcCallerNotJustPgwire() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT * FROM users WHERE id = 1");
        }

        assertEquals(1, events.size());
        QueryLog.Event event = events.get(0);
        assertEquals("SELECT", event.queryType());
        assertNull(event.errorMessage());
        assertNotNull(event.connectionId());
        assertEquals("native", event.source());
    }

    @Test
    void firesForAnInsert() throws Exception {
        // Not asserting on rowCount here -- see rowCountOf's own javadoc:
        // this implementation fetches row/update data lazily, after
        // ConvexMeta.prepareAndExecute/execute return, so it's genuinely
        // unavailable (null) at this hook point for ordinary DML/SELECT.
        // Type/error/duration remain reliable regardless.
        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')");
        }

        assertEquals(1, events.size());
        QueryLog.Event event = events.get(0);
        assertEquals("INSERT", event.queryType());
        assertNull(event.errorMessage());
    }

    @Test
    void firesOnAFailedQueryWithNoRowCount() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             Statement stmt = conn.createStatement()) {
            assertThrows(SQLException.class, () -> stmt.execute("SELECT * FROM no_such_table"));
        }

        assertEquals(1, events.size());
        QueryLog.Event event = events.get(0);
        assertNull(event.rowCount());
        assertNotNull(event.errorMessage());
    }

    @Test
    void firesForRegexInterceptedAdminStatementsNotJustOrdinarySql() throws Exception {
        // REPLICATE DB is regex-intercepted in ConvexMeta and returns before
        // ever reaching Calcite's own execution path -- exactly the second
        // gap found live 2026-08-07.
        ConvexDdlExecutor.onReplicateDb = name -> {};
        try {
            try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("REPLICATE DB otherdb");
            }
        } finally {
            ConvexDdlExecutor.onReplicateDb = null;
        }

        assertEquals(1, events.size());
        QueryLog.Event event = events.get(0);
        assertEquals("ADMIN", event.queryType());
        assertEquals(Long.valueOf(0), event.rowCount());
        assertNull(event.errorMessage());
    }

    @Test
    void firesForABoundPreparedStatementWithTheOriginalSqlText() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
             PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM users WHERE id = ?")) {
            pstmt.setLong(1, 1);
            pstmt.execute();
        }

        assertEquals(1, events.size());
        QueryLog.Event event = events.get(0);
        assertTrue(event.sql().contains("SELECT * FROM users WHERE id"), "expected the original SQL, got: " + event.sql());
        assertEquals("SELECT", event.queryType());
    }

    @Test
    void unmarkPgwireConnectionStopsLabellingThatIdAsPgwire() {
        // Direct unit test of the mark/unmark pair PgProtocolHandler relies
        // on -- if unmarkPgwireConnection (called from closeConnection, on
        // every session end) didn't actually work, a connection id could
        // stay mislabelled "pgwire" forever, or (worse, if some later,
        // unrelated connection ever reused the same id) mislabel a genuine
        // native caller.
        QueryLog.markPgwireConnection("conn-x");
        QueryLog.fire("SELECT 1", 1, 1L, null, "conn-x");
        assertEquals("pgwire", events.get(0).source());

        events.clear();
        QueryLog.unmarkPgwireConnection("conn-x");
        QueryLog.fire("SELECT 1", 1, 1L, null, "conn-x");
        assertEquals("native", events.get(0).source());
    }

    @Test
    void aQueryOverPgwireFiresExactlyOnceNotTwice() throws Exception {
        // Confirms the earlier PgProtocolHandler-level hook removal didn't
        // leave a gap -- PgServer's own connection supplier is itself an
        // ordinary jdbc:convex: caller, so this must still fire, exactly
        // once (not doubled by any leftover pgwire-level hook).
        PgServer server = PgServer.builder().port(0).database(dbName).build();
        server.start();
        try (Socket socket = new Socket("localhost", server.getPort())) {
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());
            sendStartupMessage(out, dbName, "testuser");
            skipToReadyForQuery(in);
            sendQuery(out, "SELECT * FROM users WHERE id = 1");
            skipToReadyForQuery(in);
        } finally {
            server.stop();
        }

        assertEquals(1, events.size());
        assertEquals("SELECT", events.get(0).queryType());
        assertEquals("pgwire", events.get(0).source());
    }

    private void sendStartupMessage(DataOutputStream out, String database, String user) throws Exception {
        byte[] dbBytes = database.getBytes(StandardCharsets.UTF_8);
        byte[] userBytes = user.getBytes(StandardCharsets.UTF_8);
        int length = 4 + 4 + 5 + userBytes.length + 1 + 9 + dbBytes.length + 1 + 1;
        out.writeInt(length);
        out.writeInt(196608);
        out.writeBytes("user");
        out.writeByte(0);
        out.write(userBytes);
        out.writeByte(0);
        out.writeBytes("database");
        out.writeByte(0);
        out.write(dbBytes);
        out.writeByte(0);
        out.writeByte(0);
        out.flush();
    }

    private void sendQuery(DataOutputStream out, String query) throws Exception {
        byte[] queryBytes = query.getBytes(StandardCharsets.UTF_8);
        int length = 4 + queryBytes.length + 1;
        out.writeByte('Q');
        out.writeInt(length);
        out.write(queryBytes);
        out.writeByte(0);
        out.flush();
    }

    private void skipToReadyForQuery(DataInputStream in) throws Exception {
        while (true) {
            byte type = in.readByte();
            int length = in.readInt();
            if (type == 'Z') {
                in.readByte();
                break;
            } else {
                byte[] content = new byte[length - 4];
                in.readFully(content);
            }
        }
    }
}
