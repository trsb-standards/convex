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
import org.junit.jupiter.api.io.TempDir;

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
	public void testCastSyntax() throws IOException {
		try (Socket socket = new Socket("localhost", server.getPort())) {
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());

			sendStartupMessage(out, dbName, "testuser");
			skipToReadyForQuery(in);

			// ::int4 must strip as a whole cast, not leave a stray '4' on the column name
			sendQuery(out, "SELECT id::int4 FROM users WHERE id = 1");
			// TDC + our fork's own timing NoticeResponse ('N', added 2026-08-05 --
			// upstream has no equivalent) + ReadyForQuery
			assertEquals("TDCNZ", readMessageTypes(in));
		}
	}

	@Test
	public void testTildeInLiteral() throws IOException {
		try (Socket socket = new Socket("localhost", server.getPort())) {
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());

			sendStartupMessage(out, dbName, "testuser");
			skipToReadyForQuery(in);

			// A '~' inside a string literal is data, not a regex operator
			sendQuery(out, "SELECT name FROM users WHERE email <> '~/nobody'");
			// TDDC + our fork's own timing NoticeResponse ('N') + ReadyForQuery
			assertEquals("TDDCNZ", readMessageTypes(in));
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
	public void testUcanAuthAcceptsAValidTokenForABoundDid() throws Exception {
		server.stop();
		convex.core.crypto.AKeyPair kp = convex.core.crypto.AKeyPair.generate();
		String did = convex.auth.did.DID.forKey(kp.getAccountKey()).toString();
		convex.core.data.AString jwt = convex.auth.ucan.UCAN.createJWT(kp, kp.getAccountKey(),
			System.currentTimeMillis() / 1000L + 3600, convex.core.data.Vectors.empty(), null);

		PgServer ucanServer = PgServer.builder()
			.port(0).database(dbName)
			.ucanEntityResolver(d -> d.equals(did) ? 42L : null)
			.build();
		ucanServer.start();
		try (Socket socket = new Socket("localhost", ucanServer.getPort())) {
			socket.setSoTimeout(5000);
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());
			sendStartupMessage(out, dbName, "testuser");
			assertEquals('R', in.readByte());
			in.readInt();
			assertEquals(3, in.readInt()); // CleartextPassword requested
			sendPassword(out, jwt.toString());
			assertEquals('R', in.readByte());
			in.readInt();
			assertEquals(0, in.readInt()); // AuthenticationOk
			skipToReadyForQuery(in);
		} finally {
			ucanServer.stop();
		}
	}

	@Test
	public void testUcanAuthRejectsAnUnboundDidAndAGarbageToken() throws Exception {
		server.stop();
		convex.core.crypto.AKeyPair kp = convex.core.crypto.AKeyPair.generate();
		convex.core.data.AString jwt = convex.auth.ucan.UCAN.createJWT(kp, kp.getAccountKey(),
			System.currentTimeMillis() / 1000L + 3600, convex.core.data.Vectors.empty(), null);

		PgServer ucanServer = PgServer.builder()
			.port(0).database(dbName)
			.ucanEntityResolver(d -> null) // nothing is bound
			.build();
		ucanServer.start();
		try {
			for (String secret : new String[]{ jwt.toString(), "not-a-jwt" }) {
				try (Socket socket = new Socket("localhost", ucanServer.getPort())) {
					socket.setSoTimeout(5000);
					DataOutputStream out = new DataOutputStream(socket.getOutputStream());
					DataInputStream in = new DataInputStream(socket.getInputStream());
					sendStartupMessage(out, dbName, "testuser");
					assertEquals('R', in.readByte());
					in.readInt();
					assertEquals(3, in.readInt());
					sendPassword(out, secret);
					assertEquals('E', in.readByte(), "expected ErrorResponse for: " + secret);
				}
			}
		} finally {
			ucanServer.stop();
		}
	}

	@Test
	public void testSessionAuthorizationReportsSelfOnATrustConnection() throws Exception {
		try (Socket socket = new Socket("localhost", server.getPort())) {
			socket.setSoTimeout(5000);
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());
			sendStartupMessage(out, dbName, "testuser");
			assertEquals('R', in.readByte());
			in.readInt();
			assertEquals(0, in.readInt()); // trust: AuthenticationOk immediately, no password round trip
			java.util.Map<String, String> statuses = collectParameterStatuses(in);
			assertEquals("SELF", statuses.get("session_authorization"));
		}
	}

	@Test
	public void testSessionAuthorizationReportsTheResolvedEntityIdOverUcan() throws Exception {
		server.stop();
		convex.core.crypto.AKeyPair kp = convex.core.crypto.AKeyPair.generate();
		String did = convex.auth.did.DID.forKey(kp.getAccountKey()).toString();
		convex.core.data.AString jwt = convex.auth.ucan.UCAN.createJWT(kp, kp.getAccountKey(),
			System.currentTimeMillis() / 1000L + 3600, convex.core.data.Vectors.empty(), null);

		PgServer ucanServer = PgServer.builder()
			.port(0).database(dbName)
			.ucanEntityResolver(d -> d.equals(did) ? 42L : null)
			.build();
		ucanServer.start();
		try (Socket socket = new Socket("localhost", ucanServer.getPort())) {
			socket.setSoTimeout(5000);
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());
			sendStartupMessage(out, dbName, "testuser");
			assertEquals('R', in.readByte());
			in.readInt();
			assertEquals(3, in.readInt()); // CleartextPassword requested
			sendPassword(out, jwt.toString());
			assertEquals('R', in.readByte());
			in.readInt();
			assertEquals(0, in.readInt()); // AuthenticationOk
			java.util.Map<String, String> statuses = collectParameterStatuses(in);
			assertEquals("Entity 42", statuses.get("session_authorization"));
		} finally {
			ucanServer.stop();
		}
	}

	@Test
	public void testEntitySessionLabelResolverOverridesTheBareEntityIdLabel() throws Exception {
		server.stop();
		convex.core.crypto.AKeyPair personKp = convex.core.crypto.AKeyPair.generate();
		String personDid = convex.auth.did.DID.forKey(personKp.getAccountKey()).toString();
		convex.core.crypto.AKeyPair companyKp = convex.core.crypto.AKeyPair.generate();
		String companyDid = convex.auth.did.DID.forKey(companyKp.getAccountKey()).toString();

		// A resolver mirroring dbase's real rule: a natural person (id 4) is
		// SELF; anything else (id 10, a company account) gets an enriched
		// label. An id with no mapping here (id 99) falls through to null,
		// proving the bare "Entity <id>" fallback still applies per-id, not
		// just when no resolver is configured at all.
		PgServer ucanServer = PgServer.builder()
			.port(0).database(dbName)
			.ucanEntityResolver(d -> {
				if (d.equals(personDid)) return 4L;
				if (d.equals(companyDid)) return 10L;
				return null;
			})
			.entitySessionLabelResolver(id -> switch (id.intValue()) {
				case 4 -> "SELF";
				case 10 -> "Entity 10 Acme Corp";
				default -> null;
			})
			.build();
		ucanServer.start();
		try {
			assertEquals("SELF", sessionAuthorizationFor(ucanServer, personKp, personDid));
			assertEquals("Entity 10 Acme Corp", sessionAuthorizationFor(ucanServer, companyKp, companyDid));
		} finally {
			ucanServer.stop();
		}
	}

	private String sessionAuthorizationFor(PgServer ucanServer, convex.core.crypto.AKeyPair kp, String did)
			throws Exception {
		convex.core.data.AString jwt = convex.auth.ucan.UCAN.createJWT(kp, kp.getAccountKey(),
			System.currentTimeMillis() / 1000L + 3600, convex.core.data.Vectors.empty(), null);
		try (Socket socket = new Socket("localhost", ucanServer.getPort())) {
			socket.setSoTimeout(5000);
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());
			sendStartupMessage(out, dbName, "testuser");
			assertEquals('R', in.readByte());
			in.readInt();
			assertEquals(3, in.readInt());
			sendPassword(out, jwt.toString());
			assertEquals('R', in.readByte());
			in.readInt();
			assertEquals(0, in.readInt());
			return collectParameterStatuses(in).get("session_authorization");
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
	 * Regression test for a live bug (found 2026-09-13, tracing a benchmark
	 * that looked hung): {@code executeWithParameters} calls a real internal
	 * {@code PreparedStatement#execute()} once per {@code Bind+Execute} —
	 * including every row of a JDBC {@code PreparedStatement#executeBatch()}
	 * — and {@code ConvexMeta} used to sync (announce+persist+broadcast)
	 * after every one of those, unconditionally, giving batching zero
	 * server-side throughput benefit (measured live: 100,000 rows at ~237
	 * rows/sec, indistinguishable from an unbatched loop). Fixed by
	 * deferring the sync from every {@code Execute} to the next {@code
	 * Sync} (see {@code BatchSyncDeferral}'s own doc) — a real {@code
	 * PreparedStatement#addBatch()}/{@code executeBatch()} pipelines N
	 * Binds+Executes then one Sync, so this proves the deferred sync still
	 * lands correctly (every row present, no drops/corruption) and is
	 * actually visible afterward with no further action needed on this
	 * connection.
	 */
	@Test
	public void testJdbcExecuteBatchAllRowsLandAndAreVisibleAfterTheBatch() throws Exception {
		ConvexColumnType[] types = {
			ConvexColumnType.of(ConvexType.INTEGER), // id
			ConvexColumnType.varchar(50),            // name
		};
		db.tables().createTable("batched", new String[]{"id", "name"}, types);

		String url = "jdbc:postgresql://localhost:" + server.getPort() + "/" + dbName + "?user=testuser";
		int rowCount = 200;
		try (Connection conn = DriverManager.getConnection(url);
			 java.sql.PreparedStatement ps = conn.prepareStatement("INSERT INTO batched (id, name) VALUES (?, ?)")) {
			for (int i = 0; i < rowCount; i++) {
				ps.setInt(1, i);
				ps.setString(2, "row-" + i);
				ps.addBatch();
			}
			int[] counts = ps.executeBatch();
			assertEquals(rowCount, counts.length);
			for (int c : counts) assertEquals(1, c);
		}

		// A fresh connection, with no other statement on it to incidentally
		// trigger a sync of its own -- proves the batch's own Sync already
		// made the writes visible, not something else.
		try (Connection conn = DriverManager.getConnection(url);
			 Statement stmt = conn.createStatement();
			 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM batched")) {
			assertTrue(rs.next());
			assertEquals(rowCount, rs.getInt(1));
		}
	}

	/**
	 * Found live 2026-08-25: a SELECT {@code PreparedStatement} reused across
	 * many executions crashed the real pgjdbc client with {@code
	 * java.util.NoSuchElementException} at {@code ArrayDeque.removeFirst} deep
	 * in {@code QueryExecutorImpl.processResults} — pgjdbc's own client-side
	 * response bookkeeping desynced. Root cause: pgjdbc only sends an explicit
	 * {@code Describe} message for a statement's first few executions (its own
	 * {@code prepareThreshold}, default 5); once it has the row shape cached
	 * client-side, later executions send Bind+Execute only. The server's old
	 * per-portal "did Describe just run" tracking (cleared on every Bind)
	 * then wrongly concluded "not described" on every one of those later
	 * executions and resent an unrequested {@code RowDescription} mid-stream.
	 * See {@code PgProtocolHandler.describedStatements}'s own doc for the
	 * fix (statement-scoped tracking, not portal-scoped) — this test proves
	 * it holds well past that threshold, with genuinely varying bound values
	 * each time (so a stale/reused result would also be caught, not just a
	 * protocol-level crash).
	 */
	@Test
	public void testSelectPreparedStatementReusedPastPgjdbcsDescribeThreshold() throws Exception {
		ConvexColumnType[] types = {
			ConvexColumnType.of(ConvexType.INTEGER), // id
			ConvexColumnType.varchar(50),            // name
		};
		db.tables().createTable("selreused", new String[]{"id", "name"}, types);

		String url = "jdbc:postgresql://localhost:" + server.getPort() + "/" + dbName + "?user=testuser";
		int rowCount = 40; // well past pgjdbc's default prepareThreshold of 5
		try (Connection conn = DriverManager.getConnection(url);
			 java.sql.PreparedStatement ins = conn.prepareStatement("INSERT INTO selreused (id, name) VALUES (?, ?)")) {
			for (int i = 0; i < rowCount; i++) {
				ins.setInt(1, i);
				ins.setString(2, "row-" + i);
				assertEquals(1, ins.executeUpdate());
			}
		}

		try (Connection conn = DriverManager.getConnection(url);
			 java.sql.PreparedStatement sel = conn.prepareStatement("SELECT id, name FROM selreused WHERE id = ?")) {
			for (int i = 0; i < rowCount; i++) {
				sel.setInt(1, i);
				try (ResultSet rs = sel.executeQuery()) {
					assertTrue(rs.next(), "row " + i + " must be found");
					assertEquals(i, rs.getInt("id"));
					assertEquals("row-" + i, rs.getString("name"));
					assertFalse(rs.next(), "exactly one row expected for id=" + i);
				}
			}
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

	/** Reads messages up to and including ReadyForQuery, returning their type bytes in order */
	private String readMessageTypes(DataInputStream in) throws IOException {
		StringBuilder types = new StringBuilder();
		while (true) {
			byte type = in.readByte();
			int length = in.readInt();
			in.readFully(new byte[length - 4]);
			types.append((char) type);
			if (type == 'Z') return types.toString();
		}
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

	/** Reads messages up to ReadyForQuery, collecting every ParameterStatus ('S') name/value pair seen. */
	private java.util.Map<String, String> collectParameterStatuses(DataInputStream in) throws IOException {
		java.util.Map<String, String> statuses = new java.util.HashMap<>();
		while (true) {
			byte type = in.readByte();
			int length = in.readInt();
			byte[] content = new byte[length - 4];
			in.readFully(content);
			if (type == 'Z') return statuses; // ReadyForQuery
			if (type == 'S') {
				String s = new String(content, StandardCharsets.UTF_8);
				int nul = s.indexOf('\0');
				statuses.put(s.substring(0, nul), s.substring(nul + 1, s.length() - 1));
			}
		}
	}

	/**
	 * Tests for TLS on PG-wire (default-on — see {@code dbase}'s
	 * {@code PLAN_access_rights.md}, "TLS on PG-wire"). Each test builds
	 * its own throwaway CA + leaf cert via {@link TlsSupport} in a
	 * {@code @TempDir}, rather than sharing the class's own trust-mode
	 * {@link #server}.
	 */
	@TempDir
	java.io.File tlsCertDir;

	@Test
	public void testTlsConnectionSucceedsWithValidCertificate() throws Exception {
		io.netty.handler.ssl.SslContext sslContext = TlsSupport.buildServerSslContext(
			new java.io.File(tlsCertDir, "ca.crt"), new java.io.File(tlsCertDir, "ca.key"),
			new java.io.File(tlsCertDir, "node.crt"), new java.io.File(tlsCertDir, "node.key"),
			"localhost");
		PgServer tlsServer = PgServer.builder()
			.port(0)
			.database(dbName)
			.sslContext(sslContext)
			.build();
		tlsServer.start();
		try {
			// sslmode=require negotiates TLS; NonValidatingFactory skips trust
			// verification of the throwaway self-signed-CA-issued cert -- the
			// point of this test is that the handshake and query succeed once
			// negotiated, not exercising real client-side trust (that's an
			// operator/app deployment concern, not this server's own logic).
			String url = "jdbc:postgresql://localhost:" + tlsServer.getPort() + "/" + dbName
				+ "?user=testuser&sslmode=require&sslfactory=org.postgresql.ssl.NonValidatingFactory";
			try (Connection conn = DriverManager.getConnection(url);
				 Statement stmt = conn.createStatement();
				 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM users")) {
				assertTrue(rs.next());
				assertEquals(2, rs.getInt(1));
			}
		} finally {
			tlsServer.stop();
		}
	}

	@Test
	public void testPlaintextRejectedWhenTlsRequiredByDefault() throws Exception {
		io.netty.handler.ssl.SslContext sslContext = TlsSupport.buildServerSslContext(
			new java.io.File(tlsCertDir, "ca.crt"), new java.io.File(tlsCertDir, "ca.key"),
			new java.io.File(tlsCertDir, "node.crt"), new java.io.File(tlsCertDir, "node.key"),
			"localhost");
		// allowPlaintext defaults to false -- a connection that skips
		// SSLRequest entirely must be refused, not silently accepted.
		PgServer tlsServer = PgServer.builder()
			.port(0)
			.database(dbName)
			.sslContext(sslContext)
			.build();
		tlsServer.start();
		try (Socket socket = new Socket("localhost", tlsServer.getPort())) {
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());

			// Go straight to StartupMessage, skipping SSLRequest entirely --
			// exactly what an old/misconfigured client that never negotiates
			// TLS at all would do.
			sendStartupMessage(out, dbName, "testuser");

			byte type = in.readByte();
			assertEquals('E', type, "expected ErrorResponse for a connection that skipped SSLRequest");
			int length = in.readInt();
			in.readFully(new byte[length - 4]);

			// The server closes the connection after the error -- further
			// reads hit end-of-stream rather than a ReadyForQuery.
			assertEquals(-1, in.read());
		} finally {
			tlsServer.stop();
		}
	}

	@Test
	public void testPlaintextAllowedWhenExplicitlyPermitted() throws Exception {
		io.netty.handler.ssl.SslContext sslContext = TlsSupport.buildServerSslContext(
			new java.io.File(tlsCertDir, "ca.crt"), new java.io.File(tlsCertDir, "ca.key"),
			new java.io.File(tlsCertDir, "node.crt"), new java.io.File(tlsCertDir, "node.key"),
			"localhost");
		PgServer tlsServer = PgServer.builder()
			.port(0)
			.database(dbName)
			.sslContext(sslContext)
			.allowPlaintext(true)
			.build();
		tlsServer.start();
		try (Socket socket = new Socket("localhost", tlsServer.getPort())) {
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());

			sendStartupMessage(out, dbName, "testuser");
			skipToReadyForQuery(in);
		} finally {
			tlsServer.stop();
		}
	}
}
