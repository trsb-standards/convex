package convex.db.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import convex.core.data.AVector;
import convex.core.data.ACell;
import convex.core.data.Strings;
import convex.core.data.prim.CVMLong;
import convex.db.ConvexDB;
import convex.db.calcite.ConvexType;
import convex.db.lattice.SQLSchema;
import convex.db.lattice.TableVersionRegistry;
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

	/**
	 * Isolates whether a bound null for a non-VARCHAR column is a pgwire-only
	 * bug (PgProtocolHandler mis-binding) or a deeper ConvexMeta/Avatica one
	 * — this goes through the native jdbc:convex: driver, no pgwire at all.
	 */
	@Test
	public void testPreparedStatementBindsNullForIntegerColumn() throws Exception {
		ConvexDB cdb = ConvexDB.create();
		cdb.database("nullbind_test").tables();
		cdb.register("nullbind_test");

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=nullbind_test");
				Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("CREATE TABLE scores (id INTEGER, score INTEGER)");

			try (PreparedStatement ps = conn.prepareStatement("INSERT INTO scores (id, score) VALUES (?, ?)")) {
				ps.setInt(1, 1);
				ps.setNull(2, java.sql.Types.INTEGER);
				ps.executeUpdate();
			}

			ResultSet rs = stmt.executeQuery("SELECT id, score FROM scores WHERE id = 1");
			assertTrue(rs.next());
			assertEquals(1, rs.getInt("id"));
			rs.getInt("score");
			assertTrue(rs.wasNull(), "score should have round-tripped as SQL NULL");
		} finally {
			cdb.unregister("nullbind_test");
		}
	}

	/**
	 * MAX(CASE WHEN cond THEN expr END) is a common pivot-query pattern
	 * (conditional aggregation). Calcite's own SqlToRelConverter lifts the
	 * CASE out of the aggregate argument and synthesizes an IS_TRUE(cond)
	 * guard elsewhere in the plan — this used to fail with
	 * "UnsupportedOperationException: Operator not supported: IS_TRUE" in
	 * ConvexExpressionEvaluator, which had no case for it.
	 */
	@Test
	public void testMaxOfCaseWhenSupportsConditionalAggregation() throws Exception {
		ConvexDB cdb = ConvexDB.create();
		cdb.database("pivot_test").tables();
		cdb.register("pivot_test");

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=pivot_test");
				Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("CREATE TABLE readings (software VARCHAR, reading INTEGER)");
			stmt.executeUpdate("INSERT INTO readings VALUES ('A', 10)");
			stmt.executeUpdate("INSERT INTO readings VALUES ('A', 30)");
			stmt.executeUpdate("INSERT INTO readings VALUES ('B', 5)");

			try (ResultSet rs = stmt.executeQuery(
					"SELECT MAX(CASE WHEN software = 'A' THEN reading END) AS a, "
					+ "MAX(CASE WHEN software = 'B' THEN reading END) AS b FROM readings")) {
				assertTrue(rs.next());
				assertEquals(30, rs.getInt("a"));
				assertEquals(5, rs.getInt("b"));
			}
		} finally {
			cdb.unregister("pivot_test");
		}
	}

	/**
	 * Calcite wraps a scalar subquery (e.g. {@code WHERE x = (SELECT ...)})
	 * in a SINGLE_VALUE aggregate to enforce the "at most one row" rule —
	 * this used to fail with "Aggregate not supported: SINGLE_VALUE" since
	 * ConvexAggregate had no case for it. Covers all three cardinalities:
	 * zero rows (NULL), one row (that value), and more than one row (must
	 * be a reported error, not a silently-picked row).
	 */
	@Test
	public void testScalarSubqueryHandlesAllCardinalities() throws Exception {
		ConvexDB cdb = ConvexDB.create();
		cdb.database("singlevalue_test").tables();
		cdb.register("singlevalue_test");

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=singlevalue_test");
				Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("CREATE TABLE t (id INTEGER, val INTEGER)");
			stmt.executeUpdate("INSERT INTO t VALUES (1, 100)");

			try (ResultSet rs = stmt.executeQuery(
					"SELECT (SELECT val FROM t WHERE id = 1) AS x")) {
				assertTrue(rs.next());
				assertEquals(100, rs.getInt("x"));
			}

			try (ResultSet rs = stmt.executeQuery(
					"SELECT (SELECT val FROM t WHERE id = 999) AS x")) {
				assertTrue(rs.next());
				rs.getInt("x");
				assertTrue(rs.wasNull(), "scalar subquery with zero matching rows must be NULL");
			}

			stmt.executeUpdate("INSERT INTO t VALUES (2, 200)");
			assertThrows(SQLException.class, () -> {
				try (ResultSet rs = stmt.executeQuery("SELECT (SELECT val FROM t) AS x")) {
					rs.next();
				}
			}, "scalar subquery returning more than one row must be reported as an error");
		} finally {
			cdb.unregister("singlevalue_test");
		}
	}

	/**
	 * Calcite's plain "standard" function library only defines a strictly
	 * binary CONCAT — this used to fail SQL *validation* (not execution)
	 * with "No match found for function signature CONCAT(...)" for 3+
	 * arguments, since no library adding the N-ary MySQL-style CONCAT was
	 * enabled on the connection.
	 */
	@Test
	public void testConcatSupportsMoreThanTwoArguments() throws Exception {
		ConvexDB cdb = ConvexDB.create();
		cdb.database("concat_test").tables();
		cdb.register("concat_test");

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=concat_test");
				Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("CREATE TABLE t (id INTEGER, a VARCHAR(20), b VARCHAR(20), c VARCHAR(20))");
			stmt.executeUpdate("INSERT INTO t VALUES (1, 'foo', 'bar', 'baz')");

			try (ResultSet rs = stmt.executeQuery("SELECT CONCAT(a, '-', b, '-', c) AS x FROM t")) {
				assertTrue(rs.next());
				assertEquals("foo-bar-baz", rs.getString("x"));
			}
		} finally {
			cdb.unregister("concat_test");
		}
	}

	/**
	 * ROUND(x, n) on a genuinely fractional expression must stay a double;
	 * ROUND(AVG(intCol), n) — where Calcite's own return-type inference for
	 * ROUND follows AVG's declared BIGINT/INTEGER type (same rule
	 * ConvexAggregate.computeAvg already matches) — must come back as a
	 * long instead, or the JDBC layer throws a ClassCastException picking
	 * an accessor from the column's declared type (Avatica's LongAccessor
	 * choking on a runtime Double).
	 */
	@Test
	public void testRoundMatchesCalcitesDeclaredReturnType() throws Exception {
		ConvexDB cdb = ConvexDB.create();
		cdb.database("round_test").tables();
		cdb.register("round_test");

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=round_test");
				Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("CREATE TABLE t (id INTEGER, amount INTEGER)");
			stmt.executeUpdate("INSERT INTO t VALUES (1, 100)");
			stmt.executeUpdate("INSERT INTO t VALUES (2, 201)");

			try (ResultSet rs = stmt.executeQuery("SELECT ROUND(amount / 3.0, 2) AS r FROM t WHERE id = 1")) {
				assertTrue(rs.next());
				assertEquals(33.33, rs.getDouble("r"), 0.001);
			}

			// AVG(INTEGER) truncates to a long before ROUND ever sees it —
			// pre-existing, documented behavior of computeAvg ("Match
			// Calcite's declared return type"), not something this fix
			// changes: (100+201)/2 = 150.5 truncates to 150, so
			// ROUND(150.0, 2) correctly comes back as 150, not a
			// standard-rounded 151.
			try (ResultSet rs = stmt.executeQuery("SELECT ROUND(AVG(amount), 2) AS r FROM t")) {
				assertTrue(rs.next());
				assertEquals(150, rs.getLong("r"));
			}
		} finally {
			cdb.unregister("round_test");
		}
	}

	/**
	 * {@code CREATE TABLE ... VERSIONED} via the real JDBC/ConvexMeta path —
	 * a sibling plain {@code CREATE TABLE} in the same schema stays plain,
	 * proving the two coexist correctly (the actual point of this feature).
	 * A SQL surface for history exists too ({@code SELECT * FROM t_HISTORY},
	 * see {@code testHistorySuffixTableExposesFullRowHistoryIncludingDeletes}
	 * below), but this test predates it and verification here still drops
	 * down to the Java API, same object the DDL just
	 * wrote through.
	 */
	@Test
	public void testCreateTableVersionedTracksHistoryAndSiblingPlainTableDoesNot() throws Exception {
		ConvexDB cdb = ConvexDB.create();
		cdb.database("versioned_ddl_test").tables();
		cdb.register("versioned_ddl_test");

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=versioned_ddl_test");
				Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("CREATE TABLE tracked (id INTEGER, name VARCHAR(50)) VERSIONED");
			stmt.executeUpdate("CREATE TABLE plain (id INTEGER, name VARCHAR(50))");

			stmt.executeUpdate("INSERT INTO tracked VALUES (1, 'alpha')");
			stmt.executeUpdate("UPDATE tracked SET name = 'beta' WHERE id = 1");

			stmt.executeUpdate("INSERT INTO plain VALUES (1, 'alpha')");
			stmt.executeUpdate("UPDATE plain SET name = 'beta' WHERE id = 1");

			SQLSchema tables = cdb.database("versioned_ddl_test").tables();

			// Calcite normalizes unquoted identifiers to uppercase
			// (caseSensitive=false is a ConvexDriver default) -- these direct
			// Java-API lookups need to match that, even though the SQL text
			// above was written lowercase.
			assertTrue(TableVersionRegistry.isVersioned(Strings.create("versioned_ddl_test"), Strings.create("TRACKED")));
			assertEquals(false, TableVersionRegistry.isVersioned(Strings.create("versioned_ddl_test"), Strings.create("PLAIN")));

			// 2: ConvexTable.executeUpdate only deleteByKey+insertRow's when
			// the pk itself changes (fixed live 2026-08-10 -- it used to do
			// that unconditionally, even for a plain non-pk column update
			// like this one) -- an ordinary UPDATE upserts in place, so this
			// is 1 insert entry + 1 genuine CT_UPDATE entry.
			List<AVector<ACell>> trackedHistory = tables.getHistory("TRACKED", CVMLong.create(1L));
			assertEquals(2, trackedHistory.size(), "insert (1 entry) + update (1 entry, in place)");

			assertTrue(tables.getHistory("PLAIN", CVMLong.create(1L)).isEmpty(),
				"a non-versioned table must report empty history, not an error");

			// Both tables' live data is correct regardless of versioned-ness.
			try (ResultSet rs = stmt.executeQuery("SELECT name FROM tracked WHERE id = 1")) {
				assertTrue(rs.next());
				assertEquals("beta", rs.getString("name"));
			}
			try (ResultSet rs = stmt.executeQuery("SELECT name FROM plain WHERE id = 1")) {
				assertTrue(rs.next());
				assertEquals("beta", rs.getString("name"));
			}
		} finally {
			cdb.unregister("versioned_ddl_test");
		}
	}

	/**
	 * {@code ALTER TABLE t VERSIONED} — converts an existing plain table
	 * (with pre-existing rows) to versioned in place via the real JDBC/
	 * ConvexMeta path. History must be empty immediately after conversion
	 * (nothing backfilled) and start populating only from the next write.
	 */
	@Test
	public void testAlterTableVersionedConvertsInPlaceWithNoBackfill() throws Exception {
		ConvexDB cdb = ConvexDB.create();
		cdb.database("alter_versioned_test").tables();
		cdb.register("alter_versioned_test");

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=alter_versioned_test");
				Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("CREATE TABLE t (id INTEGER, name VARCHAR(50))");
			stmt.executeUpdate("INSERT INTO t VALUES (1, 'alpha')");

			// Calcite normalizes unquoted identifiers to uppercase -- these
			// direct Java-API lookups need to match that.
			assertEquals(false, TableVersionRegistry.isVersioned(Strings.create("alter_versioned_test"), Strings.create("T")));

			stmt.executeUpdate("ALTER TABLE t VERSIONED");

			assertTrue(TableVersionRegistry.isVersioned(Strings.create("alter_versioned_test"), Strings.create("T")));

			SQLSchema tables = cdb.database("alter_versioned_test").tables();

			// Pre-existing row survives the conversion unchanged.
			try (ResultSet rs = stmt.executeQuery("SELECT name FROM t WHERE id = 1")) {
				assertTrue(rs.next());
				assertEquals("alpha", rs.getString("name"));
			}

			// No history backfilled for the pre-conversion insert.
			assertTrue(tables.getHistory("T", CVMLong.create(1L)).isEmpty());

			// A write from this point onward IS tracked -- a plain non-pk
			// UPDATE upserts in place (see the comment in the sibling
			// CREATE TABLE VERSIONED test above), so this is 1 entry.
			stmt.executeUpdate("UPDATE t SET name = 'alpha-v2' WHERE id = 1");
			assertEquals(1, tables.getHistory("T", CVMLong.create(1L)).size());
		} finally {
			cdb.unregister("alter_versioned_test");
		}
	}

	/**
	 * Regression test for the live bug reported 2026-08-09: a caller
	 * connected to the WRONG schema (e.g. "meta" when the table actually
	 * lives in "ose") ran {@code ALTER TABLE test VERSIONED} and got back
	 * "OK" even though nothing was converted -- {@code ConvexMeta} used to
	 * discard {@code convertToVersioned}'s boolean return value
	 * unconditionally. Must now throw instead of silently no-opping.
	 */
	@Test
	public void testAlterTableVersionedOnMissingTableThrowsInsteadOfSilentlySucceeding() throws Exception {
		ConvexDB cdb = ConvexDB.create();
		cdb.database("alter_versioned_missing_test").tables();
		cdb.register("alter_versioned_missing_test");

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=alter_versioned_missing_test");
				Statement stmt = conn.createStatement()) {
			SQLException ex = assertThrows(SQLException.class,
				() -> stmt.executeUpdate("ALTER TABLE nosuchtable VERSIONED"));
			assertTrue(ex.getMessage().contains("NOSUCHTABLE"));
			assertEquals(false, TableVersionRegistry.isVersioned(
				Strings.create("alter_versioned_missing_test"), Strings.create("NOSUCHTABLE")));
		} finally {
			cdb.unregister("alter_versioned_missing_test");
		}
	}

	/**
	 * {@code CREATE TABLE ... AUTOINCREMENT} via the real JDBC/ConvexMeta
	 * path — an omitted (NULL) primary key value generates sequential ids
	 * starting at 1, and an explicit value ahead of the counter pulls it
	 * forward past that value (mirrors MySQL/PostgreSQL auto-increment
	 * semantics; see {@code AutoIncrementCounters}'s own doc).
	 */
	@Test
	public void testCreateTableAutoIncrementGeneratesSequentialIdsAndHonorsExplicitValues() throws Exception {
		ConvexDB cdb = ConvexDB.create();
		cdb.database("autoincrement_ddl_test").tables();
		cdb.register("autoincrement_ddl_test");

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=autoincrement_ddl_test");
				Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("CREATE TABLE t (id INTEGER, name VARCHAR(50)) AUTOINCREMENT");

			assertTrue(convex.db.lattice.AutoIncrementRegistry.isAutoIncrement(
				Strings.create("autoincrement_ddl_test"), Strings.create("T")));

			stmt.executeUpdate("INSERT INTO t (name) VALUES ('alpha')");
			stmt.executeUpdate("INSERT INTO t (name) VALUES ('beta')");
			// An explicit value ahead of the counter must be honored, and pull
			// the counter forward past it for the next generated value.
			stmt.executeUpdate("INSERT INTO t (id, name) VALUES (50, 'explicit')");
			stmt.executeUpdate("INSERT INTO t (name) VALUES ('gamma')");

			try (ResultSet rs = stmt.executeQuery("SELECT id, name FROM t ORDER BY id")) {
				assertTrue(rs.next());
				assertEquals(1L, rs.getLong("id"));
				assertEquals("alpha", rs.getString("name"));
				assertTrue(rs.next());
				assertEquals(2L, rs.getLong("id"));
				assertEquals("beta", rs.getString("name"));
				assertTrue(rs.next());
				assertEquals(50L, rs.getLong("id"));
				assertEquals("explicit", rs.getString("name"));
				assertTrue(rs.next());
				assertEquals(51L, rs.getLong("id"));
				assertEquals("gamma", rs.getString("name"));
				assertFalse(rs.next());
			}
		} finally {
			cdb.unregister("autoincrement_ddl_test");
		}
	}

	/**
	 * {@code ALTER TABLE t AUTOINCREMENT} — converts an existing plain table
	 * with pre-existing rows, seeding the counter from that data's actual
	 * {@code MAX(id)} rather than starting back at 1 (the exact scenario the
	 * real MariaDB migration needs: historical rows already present).
	 */
	@Test
	public void testAlterTableAutoIncrementSeedsFromExistingData() throws Exception {
		ConvexDB cdb = ConvexDB.create();
		cdb.database("alter_autoincrement_test").tables();
		cdb.register("alter_autoincrement_test");

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=alter_autoincrement_test");
				Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("CREATE TABLE t (id INTEGER, name VARCHAR(50))");
			stmt.executeUpdate("INSERT INTO t (id, name) VALUES (5, 'five')");
			stmt.executeUpdate("INSERT INTO t (id, name) VALUES (196, 'one-ninety-six')");

			assertEquals(false, convex.db.lattice.AutoIncrementRegistry.isAutoIncrement(
				Strings.create("alter_autoincrement_test"), Strings.create("T")));

			stmt.executeUpdate("ALTER TABLE t AUTOINCREMENT");

			assertTrue(convex.db.lattice.AutoIncrementRegistry.isAutoIncrement(
				Strings.create("alter_autoincrement_test"), Strings.create("T")));

			stmt.executeUpdate("INSERT INTO t (name) VALUES ('next')");

			try (ResultSet rs = stmt.executeQuery("SELECT id, name FROM t WHERE id = 197")) {
				assertTrue(rs.next());
				assertEquals("next", rs.getString("name"));
			}
		} finally {
			cdb.unregister("alter_autoincrement_test");
		}
	}

	/**
	 * Regression-style coverage mirroring {@code
	 * testAlterTableVersionedOnMissingTableThrowsInsteadOfSilentlySucceeding}
	 * — {@code ALTER TABLE ... AUTOINCREMENT} against a table that doesn't
	 * exist in the connected schema must throw, not silently report success.
	 */
	@Test
	public void testAlterTableAutoIncrementOnMissingTableThrowsInsteadOfSilentlySucceeding() throws Exception {
		ConvexDB cdb = ConvexDB.create();
		cdb.database("alter_autoincrement_missing_test").tables();
		cdb.register("alter_autoincrement_missing_test");

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=alter_autoincrement_missing_test");
				Statement stmt = conn.createStatement()) {
			SQLException ex = assertThrows(SQLException.class,
				() -> stmt.executeUpdate("ALTER TABLE nosuchtable AUTOINCREMENT"));
			assertTrue(ex.getMessage().contains("NOSUCHTABLE"));
		} finally {
			cdb.unregister("alter_autoincrement_missing_test");
		}
	}

	/**
	 * Covers the SQL-level "see all records including updated/deleted ones"
	 * ask: {@code SELECT * FROM <table>_HISTORY} on a versioned table.
	 *
	 * <p>An ordinary (non-pk) {@code UPDATE} upserts in place and produces a
	 * genuine {@code CT_UPDATE} entry — {@code ConvexTable.executeUpdate}
	 * only decomposes into {@code deleteByKey} + {@code insertRow} when the
	 * pk column itself is actually changing (fixed live 2026-08-10; it used
	 * to do that unconditionally, so every update looked like a fake
	 * DELETE+INSERT pair and a real UPDATE change-type never appeared via
	 * SQL at all). A pk-changing update still legitimately decomposes into
	 * DELETE (old pk, no values — reconstructed from the history key) +
	 * INSERT (new pk) below, since that really is a move to a new key.
	 */
	@Test
	public void testHistorySuffixTableExposesFullRowHistoryIncludingDeletes() throws Exception {
		ConvexDB cdb = ConvexDB.create();
		cdb.database("history_suffix_test").tables();
		cdb.register("history_suffix_test");

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=history_suffix_test");
				Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("CREATE TABLE t (id INTEGER, name VARCHAR(50)) VERSIONED");
			stmt.executeUpdate("INSERT INTO t VALUES (1, 'alpha')");
			stmt.executeUpdate("UPDATE t SET name = 'alpha-v2' WHERE id = 1");
			stmt.executeUpdate("UPDATE t SET id = 2 WHERE id = 1"); // pk change: alpha-v2, now at id=2
			stmt.executeUpdate("DELETE FROM t WHERE id = 2");

			try (ResultSet rs = stmt.executeQuery("SELECT ID, NAME, CHANGETYPE FROM t_HISTORY ORDER BY WRITESEQ")) {
				assertTrue(rs.next());
				assertEquals(1, rs.getInt("ID"));
				assertEquals("alpha", rs.getString("NAME"));
				assertEquals("INSERT", rs.getString("CHANGETYPE"));

				// Ordinary (non-pk) UPDATE: genuine CT_UPDATE, in place.
				assertTrue(rs.next());
				assertEquals(1, rs.getInt("ID"));
				assertEquals("alpha-v2", rs.getString("NAME"));
				assertEquals("UPDATE", rs.getString("CHANGETYPE"));

				// pk-changing UPDATE's internal deleteByKey (old pk=1).
				assertTrue(rs.next());
				assertEquals(1, rs.getInt("ID"));
				assertNull(rs.getString("NAME"));
				assertEquals("DELETE", rs.getString("CHANGETYPE"));

				// pk-changing UPDATE's internal insert at the new pk=2.
				assertTrue(rs.next());
				assertEquals(2, rs.getInt("ID"));
				assertEquals("alpha-v2", rs.getString("NAME"));
				assertEquals("INSERT", rs.getString("CHANGETYPE"));

				// The explicit DELETE (pk=2).
				assertTrue(rs.next());
				assertEquals(2, rs.getInt("ID"));
				assertNull(rs.getString("NAME"));
				assertEquals("DELETE", rs.getString("CHANGETYPE"));

				assertFalse(rs.next());
			}
		} finally {
			cdb.unregister("history_suffix_test");
		}
	}

	/**
	 * Covers "how do I convert the ordering column to seconds ago" --
	 * {@code CHANGEDAT} (a TIMESTAMP derived from {@code WRITESEQ}, an
	 * HLC-style wall-clock-comparable value -- see {@code
	 * VersionedSQLTable.nextHistorySeq}'s own doc; there used to be a raw
	 * {@code System.nanoTime()}-based NANOTIME column instead, replaced live
	 * 2026-08-10 both for this reason and because nanoTime() is per-host and
	 * meaningless once two nodes' history is merged). Verifies it
	 * round-trips correctly through a real {@code TIMESTAMPDIFF} query, not
	 * just that the raw value looks right.
	 */
	@Test
	public void testHistoryChangedAtSupportsSecondsAgoQuery() throws Exception {
		ConvexDB cdb = ConvexDB.create();
		cdb.database("history_changedat_test").tables();
		cdb.register("history_changedat_test");

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=history_changedat_test");
				Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("CREATE TABLE t (id INTEGER, name VARCHAR(50)) VERSIONED");
			stmt.executeUpdate("INSERT INTO t VALUES (1, 'alpha')");

			try (ResultSet rs = stmt.executeQuery(
					"SELECT TIMESTAMPDIFF(SECOND, CHANGEDAT, CURRENT_TIMESTAMP) AS SECONDS_AGO FROM t_HISTORY")) {
				assertTrue(rs.next());
				long secondsAgo = rs.getLong("SECONDS_AGO");
				// Just-written row: comfortably under a minute old, never negative.
				assertTrue(secondsAgo >= 0 && secondsAgo < 60,
					"expected a small non-negative seconds-ago value, got " + secondsAgo);
			}
		} finally {
			cdb.unregister("history_changedat_test");
		}
	}

	/** A non-versioned table has no "_HISTORY" counterpart at all. */
	@Test
	public void testHistorySuffixTableDoesNotExistForPlainTable() throws Exception {
		ConvexDB cdb = ConvexDB.create();
		cdb.database("history_suffix_plain_test").tables();
		cdb.register("history_suffix_plain_test");

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=history_suffix_plain_test");
				Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("CREATE TABLE t (id INTEGER, name VARCHAR(50))");
			assertThrows(SQLException.class, () -> stmt.executeQuery("SELECT * FROM t_HISTORY"));
		} finally {
			cdb.unregister("history_suffix_plain_test");
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
