package convex.db.calcite;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import convex.db.ConvexDB;
import convex.db.CurrentEntity;
import convex.db.lattice.SQLDatabase;

/**
 * Round C, step 1 of `PLAN_access_rights.md` (`dbase` repo): a connection
 * resolved to a real, non-ROOT entity cannot write directly to a {@code
 * ConvexTable.PROTECTED_SCHEMAS} schema (currently just {@code auth}) —
 * catalog data there is meant to go through a purpose-built handler
 * statement instead. Trust (no UCAN token, {@link CurrentEntity#get()}
 * null) and ROOT (id 0) both remain admin overrides, mirroring the
 * identical override in {@code DbaseServer}'s {@code TRANSFER}
 * authorization check.
 */
public class ConvexTableProtectedSchemaTest {

	private static int testCounter = 0;

	private ConvexDB cdb;
	private String schemaName;

	private Connection connectToSchema(String name) throws Exception {
		schemaName = name;
		SQLDatabase db = cdb.database(name);
		cdb.register(name);
		db.tables().createTable("t",
			new String[]{"id", "name"},
			new convex.db.calcite.ConvexType[]{convex.db.calcite.ConvexType.INTEGER, convex.db.calcite.ConvexType.VARCHAR});
		return DriverManager.getConnection("jdbc:convex:database=" + name);
	}

	@BeforeEach
	void setUp() {
		cdb = ConvexDB.create();
	}

	@AfterEach
	void tearDown() {
		CurrentEntity.clear();
		if (cdb != null && schemaName != null) cdb.unregister(schemaName);
	}

	@Test
	void trustConnectionCanWriteToProtectedSchema() throws Exception {
		try (Connection conn = connectToSchema("auth"); Statement stmt = conn.createStatement()) {
			// CurrentEntity never set -- this is the trust/no-UCAN case.
			assertEquals(1, stmt.executeUpdate("INSERT INTO t VALUES (1, 'a')"));
			assertEquals(1, stmt.executeUpdate("UPDATE t SET name = 'b' WHERE id = 1"));
			assertEquals(1, stmt.executeUpdate("DELETE FROM t WHERE id = 1"));
		}
	}

	@Test
	void rootEntityCanWriteToProtectedSchema() throws Exception {
		try (Connection conn = connectToSchema("auth"); Statement stmt = conn.createStatement()) {
			CurrentEntity.set(0L); // ROOT_ENTITY_ID
			assertEquals(1, stmt.executeUpdate("INSERT INTO t VALUES (1, 'a')"));
			assertEquals(1, stmt.executeUpdate("UPDATE t SET name = 'b' WHERE id = 1"));
			assertEquals(1, stmt.executeUpdate("DELETE FROM t WHERE id = 1"));
		}
	}

	@Test
	void nonRootEntityCannotInsertIntoProtectedSchema() throws Exception {
		try (Connection conn = connectToSchema("auth"); Statement stmt = conn.createStatement()) {
			CurrentEntity.set(7L);
			SQLException e = assertThrows(SQLException.class,
				() -> stmt.executeUpdate("INSERT INTO t VALUES (1, 'a')"));
			assertTrue(e.getMessage().contains("protected schema"), "unexpected message: " + e.getMessage());
		}
	}

	@Test
	void nonRootEntityCannotUpdateOrDeleteInProtectedSchema() throws Exception {
		try (Connection conn = connectToSchema("auth"); Statement stmt = conn.createStatement()) {
			// Seed a row as trust first, so UPDATE/DELETE have something to target.
			stmt.executeUpdate("INSERT INTO t VALUES (1, 'a')");

			CurrentEntity.set(7L);
			assertThrows(SQLException.class, () -> stmt.executeUpdate("UPDATE t SET name = 'b' WHERE id = 1"));
			assertThrows(SQLException.class, () -> stmt.executeUpdate("DELETE FROM t WHERE id = 1"));
		}
	}

	@Test
	void nonRootEntityCanStillWriteToAnUnprotectedSchema() throws Exception {
		try (Connection conn = connectToSchema("plain_" + (++testCounter)); Statement stmt = conn.createStatement()) {
			CurrentEntity.set(7L);
			assertEquals(1, stmt.executeUpdate("INSERT INTO t VALUES (1, 'a')"));
			assertEquals(1, stmt.executeUpdate("UPDATE t SET name = 'b' WHERE id = 1"));
			assertEquals(1, stmt.executeUpdate("DELETE FROM t WHERE id = 1"));
		}
	}

	@Test
	void selectIsNeverBlockedRegardlessOfEntity() throws Exception {
		try (Connection conn = connectToSchema("auth"); Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("INSERT INTO t VALUES (1, 'a')"); // trust, seeds a row

			CurrentEntity.set(7L);
			try (var rs = stmt.executeQuery("SELECT * FROM t")) {
				assertTrue(rs.next());
			}
		}
	}
}
