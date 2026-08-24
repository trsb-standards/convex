package convex.db.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import convex.core.data.Strings;
import convex.db.ConvexDB;
import convex.db.jdbc.ConvexDriver;
import convex.db.lattice.BlobCAS;
import convex.etch.EtchStore;
import convex.node.NodeServer;

/**
 * End-to-end coverage for {@link ConvexFilesTable} — proves the point-lookup
 * SQL surface ({@code SELECT ... FROM files WHERE hash = '...'}) actually
 * works against a real, store-backed {@link BlobCAS}, not just the unit-level
 * {@code BlobCAS} round-trip already covered by {@code BlobCASTest}.
 *
 * <p>Deliberately uses a real {@link EtchStore} (not {@code jdbc:convex:mem:}),
 * since {@code mem:} connections have no backing {@code AStore} at all (see
 * {@code ConvexDriver.resolveMemInstance}) — {@code BlobCAS} needs a real
 * store to persist into.
 */
class ConvexFilesTableTest {

	private EtchStore store;
	private NodeServer<?> server;

	@AfterEach
	void tearDown() throws Exception {
		BlobCAS.shutdown();
		ConvexDriver.closeAll();
		if (server != null) server.close();
		if (store != null) store.close();
	}

	private Connection storeBackedConnection(File etchFile, String database) throws Exception {
		store = EtchStore.create(etchFile);
		server = ConvexDB.createNodeServer(store);
		server.launch();
		ConvexDB cdb = ConvexDB.connect(server.getCursor());
		BlobCAS.init(store);
		return ConvexDriver.connect(cdb, database);
	}

	private static String largeText(int size) {
		return "z".repeat(size);
	}

	/**
	 * Convex's content hash for an {@code AString} is a pure, deterministic
	 * function of its content -- the exact same computation {@code BlobCAS}
	 * itself performs when persisting a value (see {@code BlobCAS.persistAndRef}).
	 * Computing it independently here (rather than reading it back out of the
	 * row somehow) is legitimate, not circular: it's how any real client would
	 * know what hash to query {@code FILES} for.
	 */
	private static String expectedStringHash(String content) {
		return Strings.create(content).getHash().toHexString();
	}

	@Test
	void selectFromFilesByHashReturnsTheRealContent(@TempDir File tempDir) throws Exception {
		String content = largeText(5000);
		try (Connection conn = storeBackedConnection(new File(tempDir, "files1.etch"), "files_test1");
				Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("CREATE TABLE t (id INTEGER, body VARCHAR)");
			stmt.executeUpdate("INSERT INTO t VALUES (1, '" + content + "')");

			String hash = expectedStringHash(content);

			try (ResultSet rs = stmt.executeQuery(
					"SELECT kind, size, content FROM files WHERE hash = '" + hash + "'")) {
				assertTrue(rs.next());
				assertEquals("STRING", rs.getString(1));
				assertEquals(content.length(), rs.getInt(2));
				assertEquals(content, rs.getString(3));
			}
		}
	}

	@Test
	void selectFromFilesWithoutAHashPredicateReturnsEmptyNotAnError(@TempDir File tempDir) throws Exception {
		try (Connection conn = storeBackedConnection(new File(tempDir, "files2.etch"), "files_test2");
				Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("CREATE TABLE t (id INTEGER, body VARCHAR)");
			stmt.executeUpdate("INSERT INTO t VALUES (1, '" + largeText(5000) + "')");

			try (ResultSet rs = stmt.executeQuery("SELECT hash FROM files")) {
				assertFalse(rs.next(), "full enumeration is deliberately unsupported -- see ConvexFilesTable's own doc");
			}
		}
	}

	@Test
	void selectFromFilesWithAnUnknownHashReturnsEmpty(@TempDir File tempDir) throws Exception {
		try (Connection conn = storeBackedConnection(new File(tempDir, "files3.etch"), "files_test3");
				Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("CREATE TABLE t (id INTEGER, body VARCHAR)");

			try (ResultSet rs = stmt.executeQuery(
					"SELECT hash FROM files WHERE hash = '" + "00".repeat(32) + "'")) {
				assertFalse(rs.next());
			}
		}
	}
}
