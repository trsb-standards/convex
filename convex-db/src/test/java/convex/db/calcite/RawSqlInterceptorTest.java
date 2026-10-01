package convex.db.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import convex.db.ConvexDB;

/**
 * Covers {@link ConvexDdlExecutor#rawSqlInterceptor} — the generic
 * pre-interception seam an embedder (e.g. dbase) uses to fully own its own
 * SQL extensions, checked before any of convex-db's own regex-intercepted
 * statements. See dbase's own {@code PLAN_convexdb_dbase_split.md} for why
 * this exists.
 */
public class RawSqlInterceptorTest {

	private static int counter = 0;
	private String dbName;

	@BeforeEach
	void setUp() {
		dbName = "rawsqlhook_" + (++counter) + "_" + System.currentTimeMillis();
	}

	@AfterEach
	void tearDown() {
		// A process-wide static field -- must be cleared so this test's
		// interceptor doesn't leak into unrelated tests running later in
		// the same JVM.
		ConvexDdlExecutor.rawSqlInterceptor = null;
	}

	@Test
	void nullReturnFallsThroughToOrdinaryHandlingUnchanged() throws Exception {
		AtomicReference<String> seen = new AtomicReference<>();
		ConvexDdlExecutor.rawSqlInterceptor = req -> {
			seen.set(req.sql());
			return null;
		};

		ConvexDB cdb = ConvexDB.create();
		cdb.database(dbName).tables();
		cdb.register(dbName);

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
				Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT 1")) {
			assertTrue(rs.next());
			assertEquals(1, rs.getInt(1));
		} finally {
			cdb.unregister(dbName);
		}

		assertEquals("SELECT 1", seen.get());
	}

	@Test
	void rewriteActionSubstitutesTheSqlBeforeCalcitesOwnHandling() throws Exception {
		ConvexDdlExecutor.rawSqlInterceptor = req ->
			req.sql().trim().equalsIgnoreCase("PING")
				? new ConvexDdlExecutor.RawSqlAction.Rewrite("SELECT 42 AS ANSWER")
				: null;

		ConvexDB cdb = ConvexDB.create();
		cdb.database(dbName).tables();
		cdb.register(dbName);

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
				Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery("PING")) {
			assertTrue(rs.next());
			assertEquals(42, rs.getInt("ANSWER"));
			assertFalse(rs.next());
		} finally {
			cdb.unregister(dbName);
		}
	}

	/**
	 * A {@code Done} action's {@code ExecuteResult} is returned exactly as
	 * built, with none of convex-db's own regex patterns or {@code
	 * super.prepareAndExecute} fallthrough ever running -- verified by
	 * registering an unrecognizable statement text ({@code CLAIM
	 * EVERYTHING}, not valid SQL) that would otherwise fail to parse, and
	 * confirming it succeeds instead. The precise numeric update count
	 * isn't independently asserted here: {@code AvaticaStatement}'s
	 * client-side {@code getUpdateCount()} reads {@code -1} for an
	 * {@code ExecuteResult} built this way regardless of what {@link
	 * MetaResultSet#count} was actually given -- the same "count often
	 * unavailable at this hook point" behaviour already documented for
	 * every other regex-intercepted admin statement in this codebase
	 * (which is why none of them assert it either), not something this
	 * hook introduces.
	 */
	@Test
	void doneActionIsReturnedAsIsWithNoFurtherProcessing() throws Exception {
		ConvexDdlExecutor.rawSqlInterceptor = req -> {
			if (!req.sql().trim().equalsIgnoreCase("CLAIM EVERYTHING")) return null;
			ExecuteResult result = new ExecuteResult(Collections.singletonList(
				MetaResultSet.count(req.statementHandle().connectionId, req.statementHandle().id, 7L)));
			return new ConvexDdlExecutor.RawSqlAction.Done(result);
		};

		ConvexDB cdb = ConvexDB.create();
		cdb.database(dbName).tables();
		cdb.register(dbName);

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
				Statement stmt = conn.createStatement()) {
			boolean isResultSet = stmt.execute("CLAIM EVERYTHING");
			assertFalse(isResultSet);
		} finally {
			cdb.unregister(dbName);
		}
	}

	@Test
	void requestCarriesTheConnectionsCurrentDefaultSchemaName() throws Exception {
		AtomicReference<String> seenSchema = new AtomicReference<>();
		ConvexDdlExecutor.rawSqlInterceptor = req -> {
			if (req.sql().trim().equalsIgnoreCase("SELECT 1")) seenSchema.set(req.defaultSchemaName());
			return null;
		};

		ConvexDB cdb = ConvexDB.create();
		cdb.database(dbName).tables();
		cdb.register(dbName);

		try (Connection conn = DriverManager.getConnection("jdbc:convex:database=" + dbName);
				Statement stmt = conn.createStatement()) {
			stmt.execute("SELECT 1");
		} finally {
			cdb.unregister(dbName);
		}

		assertEquals(dbName, seenSchema.get());
	}
}
