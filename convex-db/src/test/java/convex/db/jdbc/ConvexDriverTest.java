package convex.db.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import convex.db.ConvexDB;
import convex.db.calcite.ConvexType;
import convex.node.NodeServer;

/**
 * Tests for the ConvexDriver URL formats and connection model.
 */
public class ConvexDriverTest {

	@AfterEach
	public void tearDown() {
		ConvexDriver.closeAll();
	}

	// ========== URL Parsing ==========

	@Test
	public void testParseMemExplicit() {
		ConvexDriver driver = new ConvexDriver();
		var parsed = driver.parseURL("jdbc:convex:mem:mydb", new java.util.Properties());
		assertEquals(ConvexDriver.ParsedURL.Mode.MEM, parsed.mode);
		assertEquals("mydb", parsed.identifier);
		assertEquals("mydb", parsed.database);
	}

	@Test
	public void testParseMemShorthand() {
		ConvexDriver driver = new ConvexDriver();
		var parsed = driver.parseURL("jdbc:convex:mydb", new java.util.Properties());
		assertEquals(ConvexDriver.ParsedURL.Mode.MEM, parsed.mode);
		assertEquals("mydb", parsed.identifier);
		assertEquals("mydb", parsed.database);
	}

	@Test
	public void testParseFile() {
		ConvexDriver driver = new ConvexDriver();
		var parsed = driver.parseURL("jdbc:convex:file:/data/mydb.etch", new java.util.Properties());
		assertEquals(ConvexDriver.ParsedURL.Mode.FILE, parsed.mode);
		assertEquals("/data/mydb.etch", parsed.identifier);
		assertEquals("mydb", parsed.database); // stem of filename
	}

	@Test
	public void testParseFileWithDatabaseParam() {
		ConvexDriver driver = new ConvexDriver();
		var parsed = driver.parseURL("jdbc:convex:file:/data/store.etch;database=market", new java.util.Properties());
		assertEquals(ConvexDriver.ParsedURL.Mode.FILE, parsed.mode);
		assertEquals("/data/store.etch", parsed.identifier);
		assertEquals("market", parsed.database);
	}

	@Test
	public void testParseLegacy() {
		ConvexDriver driver = new ConvexDriver();
		var parsed = driver.parseURL("jdbc:convex:database=mydb", new java.util.Properties());
		assertEquals(ConvexDriver.ParsedURL.Mode.LEGACY, parsed.mode);
		assertEquals("mydb", parsed.identifier);
		assertEquals("mydb", parsed.database);
	}

	// ========== In-Memory Connections ==========

	@Test
	public void testMemConnection() throws Exception {
		try (Connection conn = DriverManager.getConnection("jdbc:convex:mem:driver_test")) {
			assertNotNull(conn);
			try (Statement stmt = conn.createStatement()) {
				stmt.executeUpdate("CREATE TABLE t (id INTEGER, name VARCHAR)");
				stmt.executeUpdate("INSERT INTO t VALUES (1, 'Alice')");
				ResultSet rs = stmt.executeQuery("SELECT name FROM t WHERE id = 1");
				assertTrue(rs.next());
				assertEquals("Alice", rs.getString(1));
			}
		}
	}

	@Test
	public void testMemShorthandConnection() throws Exception {
		try (Connection conn = DriverManager.getConnection("jdbc:convex:shorthand_test")) {
			assertNotNull(conn);
			try (Statement stmt = conn.createStatement()) {
				stmt.executeUpdate("CREATE TABLE t (id INTEGER, val VARCHAR)");
				stmt.executeUpdate("INSERT INTO t VALUES (1, 'hello')");
				ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM t");
				assertTrue(rs.next());
				assertEquals(1, rs.getLong(1));
			}
		}
	}

	@Test
	public void testMemSharedAcrossConnections() throws Exception {
		// First connection creates table and inserts
		try (Connection conn1 = DriverManager.getConnection("jdbc:convex:mem:shared_test")) {
			try (Statement stmt = conn1.createStatement()) {
				stmt.executeUpdate("CREATE TABLE t (id INTEGER, name VARCHAR)");
				stmt.executeUpdate("INSERT INTO t VALUES (1, 'Alice')");
			}
		}

		// Second connection to same name sees the data
		try (Connection conn2 = DriverManager.getConnection("jdbc:convex:mem:shared_test")) {
			try (Statement stmt = conn2.createStatement()) {
				ResultSet rs = stmt.executeQuery("SELECT name FROM t WHERE id = 1");
				assertTrue(rs.next());
				assertEquals("Alice", rs.getString(1));
			}
		}
	}

	/** Regression for #645: generated DML must retain its connection instance. */
	@Test
	public void testDmlIsolationAcrossManagedInstances() throws Exception {
		String a="jdbc:convex:mem:isolation_a";
		String b="jdbc:convex:mem:isolation_b";

		executeUpdate(a,"CREATE TABLE t (id INTEGER, name VARCHAR)");
		assertEquals(1,executeUpdate(a,"INSERT INTO t VALUES (1, 'FromA')"));
		executeUpdate(b,"CREATE TABLE t (id INTEGER, name VARCHAR)");
		assertEquals(1,executeUpdate(b,"INSERT INTO t VALUES (1, 'FromB')"));

		assertEquals(List.of("FromA"),queryStrings(a,"SELECT name FROM t"));
		assertEquals(List.of("FromB"),queryStrings(b,"SELECT name FROM t"));

		assertEquals(1,executeUpdate(b,"UPDATE t SET name = 'UpdatedB' WHERE id = 1"));
		assertEquals(List.of("FromA"),queryStrings(a,"SELECT name FROM t"));
		assertEquals(List.of("UpdatedB"),queryStrings(b,"SELECT name FROM t"));

		assertEquals(1,executeUpdate(b,"DELETE FROM t WHERE id = 1"));
		assertEquals(List.of("FromA"),queryStrings(a,"SELECT name FROM t"));
		assertTrue(queryStrings(b,"SELECT name FROM t").isEmpty());
	}

	/** Regression for #646: Calcite represents a one-column row as a scalar. */
	@Test
	public void testSingleColumnDml() throws Exception {
		String url="jdbc:convex:mem:single_column";
		executeUpdate(url,"CREATE TABLE s (k VARCHAR)");

		assertEquals(1,executeUpdate(url,"INSERT INTO s VALUES ('x')"));
		assertEquals(List.of("x"),queryStrings(url,"SELECT k FROM s"));

		assertEquals(1,executeUpdate(url,"UPDATE s SET k = 'y' WHERE k = 'x'"));
		assertEquals(List.of("y"),queryStrings(url,"SELECT k FROM s"));

		assertEquals(1,executeUpdate(url,"DELETE FROM s WHERE k = 'y'"));
		assertTrue(queryStrings(url,"SELECT k FROM s").isEmpty());
	}

	private static int executeUpdate(String url, String sql) throws Exception {
		try (Connection connection=DriverManager.getConnection(url);
				Statement statement=connection.createStatement()) {
			return statement.executeUpdate(sql);
		}
	}

	private static List<String> queryStrings(String url, String sql) throws Exception {
		ArrayList<String> result=new ArrayList<>();
		try (Connection connection=DriverManager.getConnection(url);
				Statement statement=connection.createStatement();
				ResultSet rs=statement.executeQuery(sql)) {
			while (rs.next()) result.add(rs.getString(1));
		}
		return result;
	}

	// ========== File-Backed Connections ==========

	@Test
	public void testFileConnection() throws Exception {
		File tempFile = File.createTempFile("convex-driver-test", ".etch");
		tempFile.delete(); // EtchStore.create will create it
		tempFile.deleteOnExit();

		String url = "jdbc:convex:file:" + tempFile.getAbsolutePath().replace('\\', '/');

		try (Connection conn = DriverManager.getConnection(url)) {
			assertNotNull(conn);
			try (Statement stmt = conn.createStatement()) {
				stmt.executeUpdate("CREATE TABLE t (id INTEGER, name VARCHAR)");
				stmt.executeUpdate("INSERT INTO t VALUES (1, 'Bob')");
				ResultSet rs = stmt.executeQuery("SELECT name FROM t WHERE id = 1");
				assertTrue(rs.next());
				assertEquals("Bob", rs.getString(1));
			}
		}

		assertTrue(tempFile.exists(), "Etch file should exist after connection");
	}

	// ========== Direct API ==========

	@Test
	public void testDirectGetConnection() throws Exception {
		ConvexDB cdb = ConvexDB.create();
		cdb.database("direct_test").tables().createTable("t",
				new String[]{"id", "name"},
				new ConvexType[]{ConvexType.INTEGER, ConvexType.VARCHAR});

		try (Connection conn = cdb.getConnection("direct_test")) {
			assertNotNull(conn);
			try (Statement stmt = conn.createStatement()) {
				stmt.executeUpdate("INSERT INTO t VALUES (1, 'Charlie')");
				ResultSet rs = stmt.executeQuery("SELECT name FROM t WHERE id = 1");
				assertTrue(rs.next());
				assertEquals("Charlie", rs.getString(1));
			}
		}
	}

	// ========== Legacy Format ==========

	@Test
	public void testLegacyFormatWithRegistry() throws Exception {
		ConvexDB cdb = ConvexDB.create();
		cdb.database("legacy_test").tables().createTable("t",
				new String[]{"id", "name"},
				new ConvexType[]{ConvexType.INTEGER, ConvexType.VARCHAR});
		cdb.register("legacy_test");

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=legacy_test")) {
			assertNotNull(conn);
			try (Statement stmt = conn.createStatement()) {
				stmt.executeUpdate("INSERT INTO t VALUES (1, 'Legacy')");
				ResultSet rs = stmt.executeQuery("SELECT name FROM t WHERE id = 1");
				assertTrue(rs.next());
				assertEquals("Legacy", rs.getString(1));
			}
		} finally {
			cdb.unregister("legacy_test");
		}
	}

	// ========== CREATE SCHEMA ==========

	@Test
	public void testCreateSchemaCreatesUsableSiblingDatabase() throws Exception {
		ConvexDB cdb = ConvexDB.create();
		cdb.database("createschema_base").tables().createTable("t",
				new String[]{"id"}, new ConvexType[]{ConvexType.INTEGER});
		cdb.register("createschema_base");

		try {
			try (Connection conn = DriverManager.getConnection("jdbc:convex:database=createschema_base");
					Statement stmt = conn.createStatement()) {
				stmt.executeUpdate("CREATE SCHEMA createschema_new");
			}

			// The new schema must be a real, separately registered Convex database —
			// not just a name mounted for this one connection — so other connections
			// (e.g. a second PgServer client, or a client typing "-d createschema_new"
			// on a fresh psql session) can reach it too. Registered lower-case:
			// Calcite upper-cases the unquoted identifier in the CREATE SCHEMA
			// statement itself, but a client reconnecting types the name as-is,
			// with no case folding applied by the driver.
			assertNotNull(ConvexDB.lookup("createschema_new"));
			assertEquals(cdb, ConvexDB.lookup("createschema_new"));

			// A fresh connection addressing it directly (as a real client would,
			// rather than staying on the connection that ran CREATE SCHEMA and
			// qualifying every reference) gets normal, unqualified table access.
			try (Connection conn2 = DriverManager.getConnection("jdbc:convex:database=createschema_new");
					Statement stmt2 = conn2.createStatement()) {
				stmt2.executeUpdate("CREATE TABLE widgets (id INTEGER, name VARCHAR)");
				stmt2.executeUpdate("INSERT INTO widgets VALUES (1, 'sprocket')");

				ResultSet rs = stmt2.executeQuery("SELECT name FROM widgets WHERE id = 1");
				assertTrue(rs.next());
				assertEquals("sprocket", rs.getString(1));
			}
		} finally {
			cdb.unregister("createschema_base");
			cdb.unregister("createschema_new");
		}
	}

	@Test
	public void testCreateSchemaSupportsQualifiedDdlAndDmlOnSameConnection() throws Exception {
		// A qualified reference like "newdb.widgets" must work regardless of
		// which database the connection is actually scoped to — including
		// right after CREATE SCHEMA, on the very connection that created it,
		// without needing to reconnect first.
		ConvexDB cdb = ConvexDB.create();
		cdb.database("samesession_anchor").tables();
		cdb.register("samesession_anchor");

		try {
			try (Connection conn = DriverManager.getConnection("jdbc:convex:database=samesession_anchor");
					Statement stmt = conn.createStatement()) {
				stmt.executeUpdate("CREATE SCHEMA samesession_new");
				stmt.executeUpdate("CREATE TABLE samesession_new.widgets (id INTEGER, name VARCHAR)");

				try (PreparedStatement ps = conn.prepareStatement(
						"INSERT INTO samesession_new.widgets VALUES (?, ?)")) {
					ps.setInt(1, 1);
					ps.setString(2, "sprocket");
					ps.executeUpdate();
				}

				ResultSet rs = stmt.executeQuery("SELECT name FROM samesession_new.widgets WHERE id = 1");
				assertTrue(rs.next());
				assertEquals("sprocket", rs.getString(1));
			}
		} finally {
			cdb.unregister("samesession_anchor");
			cdb.unregister("samesession_new");
		}
	}

	@Test
	public void testCreateSchemaIfNotExistsIsNoOpWhenSchemaAlreadyExists() throws Exception {
		ConvexDB cdb = ConvexDB.create();
		cdb.database("createschema_idempotent").tables();
		cdb.register("createschema_idempotent");

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=createschema_idempotent")) {
			try (Statement stmt = conn.createStatement()) {
				stmt.executeUpdate("CREATE SCHEMA sibling_a");
				// Must not throw the second time.
				stmt.executeUpdate("CREATE SCHEMA IF NOT EXISTS sibling_a");
			}
		} finally {
			cdb.unregister("createschema_idempotent");
			cdb.unregister("sibling_a");
		}
	}

	// ========== Re-registration after a restart ==========

	@Test
	public void testUnregisteredExistingDatabaseSilentlyFallsBackToEmptyInstance() throws Exception {
		// Documents a real footgun: ConvexDB.registry is in-memory only and
		// does NOT persist across a process restart, unlike the underlying
		// data. If a database that already has persisted data is no longer
		// registered (e.g. right after a fresh restart, before anything
		// re-registers it), connecting to it by name does NOT error — it
		// silently falls back to a brand-new, empty, disconnected ConvexDB
		// (LEGACY mode's fallthrough to resolveMemInstance()). This is why
		// dbase.DbaseServer.main() re-registers every existing database at
		// startup rather than relying on ad-hoc re-registration.
		ConvexDB cdb = ConvexDB.create();
		cdb.database("unregistered_existing").tables().createTable("t",
				new String[]{"id"}, new ConvexType[]{ConvexType.INTEGER});
		cdb.register("unregistered_existing");

		// Simulates what a fresh process's empty in-memory registry looks
		// like, even though "unregistered_existing"'s data still exists.
		cdb.unregister("unregistered_existing");

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=unregistered_existing");
				Statement stmt = conn.createStatement()) {
			// Table "t" does not exist in the fallback empty instance —
			// creating it here does NOT touch the original database's data.
			stmt.executeUpdate("CREATE TABLE t (id INTEGER)");
		}

		// The real data is untouched and still reachable via the native API,
		// proving the JDBC connection above talked to a different instance.
		assertEquals(0L, cdb.database("unregistered_existing").tables().selectAll("t").count());
	}

	@Test
	public void testReRegisteringExistingDatabasesRestoresRealAccess() throws Exception {
		// The fix pattern DbaseServer.main() now applies at startup: iterate
		// every database that already has state and re-register each one,
		// rather than leaving them to be silently shadowed by the
		// empty-fallback behaviour above.
		ConvexDB cdb = ConvexDB.create();
		cdb.database("reregister_existing").tables().createTable("t",
				new String[]{"id"}, new ConvexType[]{ConvexType.INTEGER});
		cdb.database("reregister_existing").tables().insert("t", 1);
		cdb.register("reregister_existing");

		cdb.unregister("reregister_existing"); // simulate a fresh restart's empty registry

		for (String existingDb : cdb.getDatabaseNames()) {
			cdb.register(existingDb);
		}

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=reregister_existing");
				Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT id FROM t")) {
			assertTrue(rs.next(), "re-registering must restore access to the real, persisted data");
			assertEquals(1, rs.getInt("id"));
		} finally {
			cdb.unregister("reregister_existing");
		}
	}

	// ========== DROP TABLE on a natively-created table ==========

	@Test
	public void testDropTableFindsTableCreatedViaNativeApiDespiteCasing() throws Exception {
		// Regression test: a table created via the native SQLSchema API keeps
		// whatever literal name the caller used (e.g. lower-case "widgets",
		// as dbase-meta's schema classes do), but DROP TABLE's SQL identifier
		// gets upper-cased by Calcite's parser ("WIDGETS"). dropTable() does
		// an exact/case-sensitive lookup against Convex's native storage, so
		// this used to fail with "Object 'WIDGETS' not found" even though
		// the table plainly exists.
		ConvexDB cdb = ConvexDB.create();
		cdb.database("droptest").tables().createTable("widgets",
				new String[]{"id"}, new ConvexType[]{ConvexType.INTEGER});
		cdb.register("droptest");

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=droptest");
				Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("DROP TABLE widgets");
		} finally {
			cdb.unregister("droptest");
		}

		assertEquals(0, cdb.database("droptest").tables().getTableNames().length,
			"the natively-created table must actually be gone after DROP TABLE");
	}
}
