package convex.db.psql;

import static org.junit.jupiter.api.Assertions.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import convex.db.ConvexDB;
import convex.db.calcite.ConvexColumnType;

import convex.db.calcite.ConvexType;
import convex.db.lattice.SQLDatabase;

/**
 * Tests for the PostgreSQL wire protocol server.
 */
public class PgServerTest {

	private static final AtomicInteger DB_COUNTER = new AtomicInteger(0);
	private PgServer server;
	private ConvexDB cdb;
	private SQLDatabase db;
	private String dbName;

	@BeforeEach
	public void setUp() throws Exception {
		// Get unique database name
		dbName = "pgtest_" + DB_COUNTER.getAndIncrement();

		// Create and register a test database
		cdb = ConvexDB.create();
		db = cdb.database(dbName);
		cdb.register(dbName);

		// Create users table using SQLSchema API
		ConvexColumnType[] userTypes = {
			ConvexColumnType.of(ConvexType.INTEGER),   // id
			ConvexColumnType.varchar(50),              // name
			ConvexColumnType.varchar(100)              // email
		};
		db.tables().createTable("users", new String[]{"id", "name", "email"}, userTypes);
		db.tables().insert("users", 1L, "Alice", "alice@example.com");
		db.tables().insert("users", 2L, "Bob", "bob@example.com");

		// Start server on dynamic port (port 0 lets OS assign)
		server = PgServer.builder()
			.port(0)
			.database(dbName)
			.build();
		server.start();
	}

	@AfterEach
	public void tearDown() {
		if (server != null && server.isRunning()) {
			server.stop();
		}
		if (cdb != null) {
			cdb.unregister(dbName);
		}
	}

	@Test
	public void testServerStartAndStop() {
		assertTrue(server.isRunning());
		assertEquals(server.getPort(), server.getPort());

		server.stop();
		assertFalse(server.isRunning());
	}

	@Test
	public void testSSLRequest() throws IOException {
		try (Socket socket = new Socket("localhost", server.getPort())) {
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());

			// Send SSL request
			out.writeInt(8); // length
			out.writeInt(80877103); // SSL request code
			out.flush();

			// Should respond with 'N' (no SSL)
			int response = in.read();
			assertEquals('N', response);
		}
	}

	@Test
	public void testStartupAndAuthentication() throws IOException {
		try (Socket socket = new Socket("localhost", server.getPort())) {
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());

			// Send startup message
			sendStartupMessage(out, dbName, "testuser");

			// Read AuthenticationOk
			byte type = in.readByte();
			assertEquals('R', type); // AuthenticationRequest

			int length = in.readInt();
			int authType = in.readInt();
			assertEquals(0, authType); // AuthenticationOk

			// Read parameter status messages until ReadyForQuery
			while (true) {
				type = in.readByte();
				length = in.readInt();
				if (type == 'Z') { // ReadyForQuery
					byte status = in.readByte();
					assertEquals('I', status); // Idle
					break;
				} else {
					// Skip the message content
					byte[] content = new byte[length - 4];
					in.readFully(content);
				}
			}
		}
	}

	@Test
	public void testSimpleQuery() throws IOException {
		try (Socket socket = new Socket("localhost", server.getPort())) {
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());

			// Authenticate
			sendStartupMessage(out, dbName, "testuser");
			skipToReadyForQuery(in);

			// Query existing data
			sendQuery(out, "SELECT * FROM users WHERE id = 1");

			// Should receive RowDescription, DataRow, CommandComplete, ReadyForQuery
			byte type = in.readByte();
			assertEquals('T', type); // RowDescription

			int length = in.readInt();
			byte[] rowDescData = new byte[length - 4];
			in.readFully(rowDescData);

			// DataRow
			type = in.readByte();
			assertEquals('D', type);
			length = in.readInt();
			byte[] dataRowData = new byte[length - 4];
			in.readFully(dataRowData);

			// CommandComplete
			type = in.readByte();
			assertEquals('C', type);
			length = in.readInt();
			byte[] completeData = new byte[length - 4];
			in.readFully(completeData);
			String completeMsg = new String(completeData, StandardCharsets.UTF_8);
			assertTrue(completeMsg.startsWith("SELECT"));

			// NoticeResponse: server-measured query timing ("1 row in set (X.XXX sec)")
			type = in.readByte();
			assertEquals('N', type);
			length = in.readInt();
			byte[] noticeData = new byte[length - 4];
			in.readFully(noticeData);
			String noticeMsg = new String(noticeData, StandardCharsets.UTF_8);
			assertTrue(noticeMsg.contains("1 row in set ("), "expected timing notice, got: " + noticeMsg);
			assertTrue(noticeMsg.contains("sec)"), "expected timing notice, got: " + noticeMsg);

			// ReadyForQuery
			type = in.readByte();
			assertEquals('Z', type);
		}
	}

	@Test
	public void testInsertTimingNotice() throws IOException {
		try (Socket socket = new Socket("localhost", server.getPort())) {
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());

			sendStartupMessage(out, dbName, "testuser");
			skipToReadyForQuery(in);

			sendQuery(out, "INSERT INTO users (id, name, email) VALUES (3, 'Carol', 'carol@example.com')");

			// CommandComplete
			byte type = in.readByte();
			assertEquals('C', type);
			int length = in.readInt();
			byte[] completeData = new byte[length - 4];
			in.readFully(completeData);
			assertTrue(new String(completeData, StandardCharsets.UTF_8).startsWith("INSERT"));

			// NoticeResponse: "Query OK, 1 row affected (X.XXX sec)"
			type = in.readByte();
			assertEquals('N', type);
			length = in.readInt();
			byte[] noticeData = new byte[length - 4];
			in.readFully(noticeData);
			String noticeMsg = new String(noticeData, StandardCharsets.UTF_8);
			assertTrue(noticeMsg.contains("Query OK, 1 row affected ("), "expected timing notice, got: " + noticeMsg);

			// ReadyForQuery
			type = in.readByte();
			assertEquals('Z', type);
		}
	}

	/**
	 * The timing {@code NoticeResponse} footer exists for a human reading a
	 * terminal ({@link #testInsertTimingNotice} above, the simple-query
	 * path) -- a bound {@code PreparedStatement} is always a program, never
	 * a person at psql, so it should never receive one. pgjdbc surfaces a
	 * received {@code NoticeResponse} as a {@code SQLWarning} on the
	 * statement, so its absence is directly assertable via {@code
	 * getWarnings()} without needing to hand-decode the wire protocol.
	 */
	@Test
	public void testExtendedProtocolInsertSendsNoTimingNotice() throws Exception {
		String url = "jdbc:postgresql://localhost:" + server.getPort() + "/" + dbName + "?user=testuser";
		try (Connection conn = DriverManager.getConnection(url);
			 java.sql.PreparedStatement ps = conn.prepareStatement(
				 "INSERT INTO users (id, name, email) VALUES (?, ?, ?)")) {
			ps.setInt(1, 4);
			ps.setString(2, "Dan");
			ps.setString(3, "dan@example.com");
			ps.executeUpdate();
			assertNull(ps.getWarnings(), "extended-protocol INSERT should not receive a timing NoticeResponse");
		}
	}

	@Test
	public void testPasswordAuthentication() throws Exception {
		// Stop the trust-auth server
		server.stop();

		// Start a password-protected server on dynamic port
		PgServer pwdServer = PgServer.builder()
			.port(0)
			.database(dbName)
			.password("secret123")
			.build();
		pwdServer.start();

		try (Socket socket = new Socket("localhost", pwdServer.getPort())) {
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());

			// Send startup
			sendStartupMessage(out, dbName, "testuser");

			// Should receive AuthenticationCleartextPassword
			byte type = in.readByte();
			assertEquals('R', type);
			int length = in.readInt();
			int authType = in.readInt();
			assertEquals(3, authType); // CleartextPassword

			// Send password
			sendPassword(out, "secret123");

			// Should receive AuthenticationOk
			type = in.readByte();
			assertEquals('R', type);
			length = in.readInt();
			authType = in.readInt();
			assertEquals(0, authType); // Ok

			skipToReadyForQuery(in);
		} finally {
			pwdServer.stop();
		}
	}

	@Test
	public void testWrongPassword() throws Exception {
		// Stop the trust-auth server
		server.stop();

		// Start a password-protected server on dynamic port
		PgServer pwdServer = PgServer.builder()
			.port(0)
			.database(dbName)
			.password("secret123")
			.build();
		pwdServer.start();

		try (Socket socket = new Socket("localhost", pwdServer.getPort())) {
			socket.setSoTimeout(5000); // 5 second timeout
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());

			// Send startup
			sendStartupMessage(out, dbName, "testuser");

			// Should receive AuthenticationCleartextPassword
			byte type = in.readByte();
			assertEquals('R', type);
			in.readInt();
			in.readInt();

			// Send wrong password
			sendPassword(out, "wrongpassword");

			// Should receive ErrorResponse
			type = in.readByte();
			assertEquals('E', type); // ErrorResponse
		} finally {
			pwdServer.stop();
		}
	}

	@Test
	public void testEmptyQuery() throws IOException {
		try (Socket socket = new Socket("localhost", server.getPort())) {
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());

			sendStartupMessage(out, dbName, "testuser");
			skipToReadyForQuery(in);

			// Send empty query
			sendQuery(out, "");

			// Should receive EmptyQueryResponse (type 'I', length 4)
			byte type = in.readByte();
			assertEquals('I', type); // EmptyQueryResponse
			int length = in.readInt();
			assertEquals(4, length);

			// Should receive ReadyForQuery
			type = in.readByte();
			assertEquals('Z', type);
		}
	}

	@Test
	public void testTerminate() throws IOException {
		try (Socket socket = new Socket("localhost", server.getPort())) {
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());

			sendStartupMessage(out, dbName, "testuser");
			skipToReadyForQuery(in);

			// Send terminate
			out.writeByte('X');
			out.writeInt(4); // length
			out.flush();

			// Connection should close
			socket.setSoTimeout(1000);
			int result = in.read();
			assertEquals(-1, result); // EOF
		}
	}

	/**
	 * Tests connectivity using the PostgreSQL JDBC driver.
	 * Disabled: Requires extended query protocol support (Parse/Bind/Execute).
	 */
	@org.junit.jupiter.api.Disabled("Requires extended query protocol")
	@Test
	public void testPostgresJDBCDriver() throws Exception {
		// Connect using PostgreSQL JDBC driver
		String url = "jdbc:postgresql://localhost:" + server.getPort() + "/" + dbName + "?user=testuser";
		try (Connection conn = DriverManager.getConnection(url)) {
			assertNotNull(conn);
			assertFalse(conn.isClosed());

			// Execute query
			try (Statement stmt = conn.createStatement();
				 ResultSet rs = stmt.executeQuery("SELECT name, email FROM users ORDER BY id")) {

				assertTrue(rs.next());
				assertEquals("Alice", rs.getString("name"));
				assertEquals("alice@example.com", rs.getString("email"));

				assertTrue(rs.next());
				assertEquals("Bob", rs.getString("name"));
				assertEquals("bob@example.com", rs.getString("email"));

				assertFalse(rs.next());
			}
		}
	}

	/**
	 * {@code BEGIN}/{@code COMMIT} arrive as literal wire-text SQL (PostgreSQL's
	 * wire protocol has no dedicated transaction-control message — pgjdbc's
	 * {@code setAutoCommit(false)}/{@code commit()} just send these two
	 * strings via the ordinary simple-query path). Confirms {@link
	 * PgProtocolHandler#handleQuery} intercepts them and drives the backing
	 * connection's real {@code setAutoCommit}/{@code commit} (which {@code
	 * ConvexMeta} already implements via its fork/sync transaction model) —
	 * a second, unrelated connection must see the committed row.
	 */
	@Test
	public void testExplicitTransactionCommitPersistsAcrossConnections() throws Exception {
		String url = "jdbc:postgresql://localhost:" + server.getPort() + "/" + dbName + "?user=testuser";
		try (Connection conn = DriverManager.getConnection(url)) {
			conn.setAutoCommit(false);
			try (Statement stmt = conn.createStatement()) {
				stmt.execute("INSERT INTO users (id, name, email) VALUES (10, 'Dave', 'dave@example.com')");
			}
			conn.commit();
		}

		try (Connection conn = DriverManager.getConnection(url);
			 Statement stmt = conn.createStatement();
			 ResultSet rs = stmt.executeQuery("SELECT name FROM users WHERE id = 10")) {
			assertTrue(rs.next());
			assertEquals("Dave", rs.getString("name"));
		}
	}

	/**
	 * Same wire-text interception as {@link
	 * #testExplicitTransactionCommitPersistsAcrossConnections}, but for
	 * {@code ROLLBACK} — the inserted row must never become visible.
	 */
	@Test
	public void testExplicitTransactionRollbackDiscardsTheWrite() throws Exception {
		String url = "jdbc:postgresql://localhost:" + server.getPort() + "/" + dbName + "?user=testuser";
		try (Connection conn = DriverManager.getConnection(url)) {
			conn.setAutoCommit(false);
			try (Statement stmt = conn.createStatement()) {
				stmt.execute("INSERT INTO users (id, name, email) VALUES (11, 'Erin', 'erin@example.com')");
			}
			conn.rollback();
		}

		try (Connection conn = DriverManager.getConnection(url);
			 Statement stmt = conn.createStatement();
			 ResultSet rs = stmt.executeQuery("SELECT name FROM users WHERE id = 11")) {
			assertFalse(rs.next());
		}
	}

	/**
	 * A single client-side {@code PreparedStatement}, executed repeatedly
	 * with different bound values, reuses one server-side Parse (same
	 * statement name) across many Bind+Execute cycles — exactly the
	 * scenario {@code PgProtocolHandler}'s {@code preparedStatementCache}
	 * exists for (see its own doc: {@code executeWithParameters} used to
	 * re-{@code prepareStatement} from scratch on every single execute,
	 * throwing the real statement away right after). This doesn't assert on
	 * timing (too flaky for CI) — it asserts every row lands with its own
	 * correct, distinct values, which a caching bug (e.g. stale bound
	 * parameters leaking from a reused-but-not-cleared statement) would
	 * corrupt.
	 */
	@Test
	public void testPreparedStatementReusedAcrossManyExecutionsWithDifferentValues() throws Exception {
		ConvexColumnType[] types = {
			ConvexColumnType.of(ConvexType.INTEGER), // id
			ConvexColumnType.varchar(50),            // name
		};
		db.tables().createTable("reused", new String[]{"id", "name"}, types);

		String url = "jdbc:postgresql://localhost:" + server.getPort() + "/" + dbName + "?user=testuser";
		int rowCount = 50;
		try (Connection conn = DriverManager.getConnection(url);
			 java.sql.PreparedStatement ps = conn.prepareStatement("INSERT INTO reused (id, name) VALUES (?, ?)")) {
			for (int i = 0; i < rowCount; i++) {
				ps.setInt(1, i);
				ps.setString(2, "row-" + i);
				assertEquals(1, ps.executeUpdate());
			}
		}

		try (Connection conn = DriverManager.getConnection(url);
			 Statement stmt = conn.createStatement();
			 ResultSet rs = stmt.executeQuery("SELECT id, name FROM reused ORDER BY id")) {
			for (int i = 0; i < rowCount; i++) {
				assertTrue(rs.next());
				assertEquals(i, rs.getInt("id"));
				assertEquals("row-" + i, rs.getString("name"));
			}
			assertFalse(rs.next());
		}
	}

	/**
	 * Tests aggregation via PostgreSQL JDBC driver.
	 * Disabled: Requires extended query protocol support (Parse/Bind/Execute).
	 */
	@org.junit.jupiter.api.Disabled("Requires extended query protocol")
	@Test
	public void testPostgresJDBCAggregation() throws Exception {
		String url = "jdbc:postgresql://localhost:" + server.getPort() + "/" + dbName + "?user=testuser";
		try (Connection conn = DriverManager.getConnection(url);
			 Statement stmt = conn.createStatement();
			 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM users")) {

			assertTrue(rs.next());
			assertEquals(2, rs.getInt("cnt"));
		}
	}

	/**
	 * A bound null parameter for a non-VARCHAR column (e.g. INTEGER) must
	 * bind using that column's real SQL type, not the generic
	 * java.sql.Types.NULL PgProtocolHandler.executeWithParameters used to
	 * pass unconditionally — that generic-null path decodes as an Avatica
	 * ByteString downstream, which then fails to cast to Number once the
	 * INSERT actually executes against an INTEGER column.
	 */
	@Test
	public void testPreparedStatementBindsNullForNonVarcharColumn() throws Exception {
		ConvexColumnType[] types = {
			ConvexColumnType.of(ConvexType.INTEGER), // id
			ConvexColumnType.of(ConvexType.INTEGER), // score -- nullable
		};
		db.tables().createTable("scores", new String[]{"id", "score"}, types);

		String url = "jdbc:postgresql://localhost:" + server.getPort() + "/" + dbName + "?user=testuser";
		try (Connection conn = DriverManager.getConnection(url);
			 java.sql.PreparedStatement ps = conn.prepareStatement("INSERT INTO scores (id, score) VALUES (?, ?)")) {
			ps.setInt(1, 1);
			ps.setNull(2, java.sql.Types.INTEGER);
			ps.executeUpdate();
		}

		try (Connection conn = DriverManager.getConnection(url);
			 Statement stmt = conn.createStatement();
			 ResultSet rs = stmt.executeQuery("SELECT id, score FROM scores WHERE id = 1")) {
			assertTrue(rs.next());
			assertEquals(1, rs.getInt("id"));
			rs.getInt("score");
			assertTrue(rs.wasNull(), "score should have round-tripped as SQL NULL");
		}
	}

	/**
	 * The more consequential half of the same bug: text-format wire values
	 * were always bound via setString() regardless of target column type, so
	 * a genuinely non-null INTEGER parameter over pgwire failed too (String
	 * cannot be cast to Number in ConvexTable.executeInsert) — not just the
	 * null case above.
	 */
	@Test
	public void testPreparedStatementBindsNonNullIntegerValueForIntegerColumn() throws Exception {
		ConvexColumnType[] types = {
			ConvexColumnType.of(ConvexType.INTEGER), // id
			ConvexColumnType.of(ConvexType.INTEGER), // score
		};
		db.tables().createTable("scores2", new String[]{"id", "score"}, types);

		String url = "jdbc:postgresql://localhost:" + server.getPort() + "/" + dbName + "?user=testuser";
		try (Connection conn = DriverManager.getConnection(url);
			 java.sql.PreparedStatement ps = conn.prepareStatement("INSERT INTO scores2 (id, score) VALUES (?, ?)")) {
			ps.setInt(1, 1);
			ps.setInt(2, 42);
			ps.executeUpdate();
		}

		try (Connection conn = DriverManager.getConnection(url);
			 Statement stmt = conn.createStatement();
			 ResultSet rs = stmt.executeQuery("SELECT id, score FROM scores2 WHERE id = 1")) {
			assertTrue(rs.next());
			assertEquals(1, rs.getInt("id"));
			assertEquals(42, rs.getInt("score"));
		}
	}

	// Helper methods

	private void sendStartupMessage(DataOutputStream out, String database, String user) throws IOException {
		byte[] dbBytes = database.getBytes(StandardCharsets.UTF_8);
		byte[] userBytes = user.getBytes(StandardCharsets.UTF_8);

		// Calculate length: 4 (length) + 4 (version) + "user\0" + user + "\0" + "database\0" + database + "\0" + "\0"
		int length = 4 + 4 + 5 + userBytes.length + 1 + 9 + dbBytes.length + 1 + 1;

		out.writeInt(length);
		out.writeInt(196608); // Version 3.0
		out.writeBytes("user");
		out.writeByte(0);
		out.write(userBytes);
		out.writeByte(0);
		out.writeBytes("database");
		out.writeByte(0);
		out.write(dbBytes);
		out.writeByte(0);
		out.writeByte(0); // terminator
		out.flush();
	}

	private void sendQuery(DataOutputStream out, String query) throws IOException {
		byte[] queryBytes = query.getBytes(StandardCharsets.UTF_8);
		int length = 4 + queryBytes.length + 1;

		out.writeByte('Q');
		out.writeInt(length);
		out.write(queryBytes);
		out.writeByte(0);
		out.flush();
	}

	private void sendPassword(DataOutputStream out, String password) throws IOException {
		byte[] pwdBytes = password.getBytes(StandardCharsets.UTF_8);
		int length = 4 + pwdBytes.length + 1;

		out.writeByte('p');
		out.writeInt(length);
		out.write(pwdBytes);
		out.writeByte(0);
		out.flush();
	}

	private void skipToReadyForQuery(DataInputStream in) throws IOException {
		while (true) {
			byte type = in.readByte();
			int length = in.readInt();
			if (type == 'Z') { // ReadyForQuery
				in.readByte(); // status
				break;
			} else {
				byte[] content = new byte[length - 4];
				in.readFully(content);
			}
		}
	}
}
