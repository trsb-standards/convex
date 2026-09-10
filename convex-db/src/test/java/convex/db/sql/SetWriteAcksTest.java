package convex.db.sql;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import convex.api.Convex;
import convex.api.ConvexRemote;
import convex.core.crypto.AKeyPair;
import convex.core.data.AVector;
import convex.core.data.ACell;
import convex.core.data.AccountKey;
import convex.core.data.prim.CVMLong;
import convex.core.store.AStore;
import convex.core.store.MemoryStore;
import convex.db.ConvexDB;
import convex.db.jdbc.ConvexDriver;
import convex.db.lattice.SQLDatabase;
import convex.node.NodeConfig;
import convex.node.NodeServer;

/**
 * End-to-end test of {@code SET WRITE_ACKS = <n>} through the real SQL/JDBC
 * stack -- two genuine {@link NodeServer} instances, peered exactly as
 * {@link ReplicationDemo} sets up. The write side goes through real SQL
 * (regex dispatch, the per-connection {@code writeAcks} field on {@code
 * ConvexMeta}, {@code syncIfAutoCommit}); the cross-node verification side
 * reads node B's own cursor directly via the raw {@link SQLDatabase} API
 * instead of a fresh SQL SELECT, because bare convex-db has no automatic
 * cross-node table/schema *discovery* (that's dbase's own MetaPopulator
 * layer, an orthogonal concern this test isn't exercising) -- the row
 * DATA merging correctly and promptly is what this feature actually does.
 */
public class SetWriteAcksTest {

	private AStore storeA;
	private AStore storeB;
	private NodeServer<?> serverA;
	private NodeServer<?> serverB;
	private String dbName;

	@BeforeEach
	public void setUp() throws Exception {
		storeA = new MemoryStore();
		storeB = new MemoryStore();
		// Unlike ConvexDB.createNodeServer (NodeConfig.port(-1), local-only,
		// no network) this needs real peer connections -- same construction
		// ReplicationDemo uses, just with an OS-assigned port (0) instead of
		// a hardcoded one, avoiding bind collisions on a busy CI runner.
		serverA = new NodeServer<>(ConvexDB.DATABASE_MAP_LATTICE, storeA, NodeConfig.port(0));
		serverB = new NodeServer<>(ConvexDB.DATABASE_MAP_LATTICE, storeB, NodeConfig.port(0));
		serverA.setInboundPropagatorSelector(c -> serverA.getPropagator());
		serverB.setInboundPropagatorSelector(c -> serverB.getPropagator());
		serverA.launch();
		serverB.launch();

		InetSocketAddress addrA = serverA.getHostAddress();
		InetSocketAddress addrB = serverB.getHostAddress();
		AccountKey keyA = AKeyPair.generate().getAccountKey();
		AccountKey keyB = AKeyPair.generate().getAccountKey();

		Convex aToB = ConvexRemote.connect(addrB);
		serverA.getPropagator().addPeer(keyB, aToB);
		Convex bToA = ConvexRemote.connect(addrA);
		serverB.getPropagator().addPeer(keyA, bToA);

		dbName = "write_acks_" + System.nanoTime();
		ConvexDB.connect(serverA.getCursor()).register(dbName);
	}

	@AfterEach
	public void tearDown() throws Exception {
		ConvexDriver.closeAll();
		if (serverA != null) serverA.close();
		if (serverB != null) serverB.close();
		if (storeA != null) storeA.close();
		if (storeB != null) storeB.close();
	}

	/** Reads a row directly from node B's own merged cursor, under the same db name node A wrote to. */
	private AVector<ACell> readRowOnB(String tableName, long id) {
		SQLDatabase dbOnB = SQLDatabase.connect(serverB.getCursor(), dbName);
		return dbOnB.tables().selectByKey(tableName, CVMLong.create(id));
	}

	/**
	 * With {@code SET WRITE_ACKS = 1}, an autocommit INSERT on node A must
	 * not return until node B has actually merged it -- checked with no
	 * poll/retry/sleep, matching {@code LatticePropagatorTest}'s own
	 * "waits for real peer merge" test at the propagator layer.
	 */
	@Test
	public void testWriteAcksMakesInsertVisibleOnPeerImmediately() throws Exception {
		try (Connection connA = DriverManager.getConnection("jdbc:convex:database=" + dbName);
				Statement stA = connA.createStatement()) {
			stA.execute("CREATE TABLE T (ID INTEGER, NM VARCHAR)");
			stA.execute("SET WRITE_ACKS = 1");
			stA.execute("INSERT INTO T VALUES (1, 'hello')");
		}

		AVector<ACell> row = readRowOnB("T", 1);
		assertNotNull(row, "Row should already be merged on peer node B -- no poll, no sleep");
		assertEquals("hello", row.get(1).toString());
	}

	/** {@code SET WRITE_ACKS = 0} must not throw and must not hang. */
	@Test
	public void testWriteAcksZeroDoesNotWaitOrThrow() throws Exception {
		try (Connection connA = DriverManager.getConnection("jdbc:convex:database=" + dbName);
				Statement stA = connA.createStatement()) {
			stA.execute("CREATE TABLE T (ID INTEGER, NM VARCHAR)");
			stA.execute("SET WRITE_ACKS = 0");
			assertDoesNotThrow(() -> stA.execute("INSERT INTO T VALUES (2, 'fast')"));
		}
	}

	/**
	 * A pending {@code WRITE_ACKS} target must apply to every subsequent
	 * autocommit statement on the connection, not just the one immediately
	 * after the SET -- confirms {@code writeAcks} is genuinely a
	 * per-connection setting, not a one-shot flag.
	 */
	@Test
	public void testWriteAcksAppliesToEverySubsequentStatement() throws Exception {
		try (Connection connA = DriverManager.getConnection("jdbc:convex:database=" + dbName);
				Statement stA = connA.createStatement()) {
			stA.execute("CREATE TABLE T (ID INTEGER, NM VARCHAR)");
			stA.execute("SET WRITE_ACKS = 1");
			stA.execute("INSERT INTO T VALUES (10, 'first')");
			stA.execute("INSERT INTO T VALUES (11, 'second')");
		}

		assertNotNull(readRowOnB("T", 10), "First insert should be immediately visible on peer node B");
		assertNotNull(readRowOnB("T", 11), "Second insert should also be immediately visible on peer node B");
	}

	/**
	 * Found live 2026-08-25: {@code syncIfAutoCommit} used to configure the
	 * ack target unconditionally, including for a plain SELECT -- {@code
	 * LatticePropagator.publishWithAckTarget} decides whether to wait
	 * purely from {@code minAcks > 0} and whether any peer is connected,
	 * neither of which checks whether the value actually changed, so a
	 * SELECT on an {@code acks=1} connection built a real ack-tracked
	 * broadcast and waited on a genuine peer round-trip -- measured live,
	 * SELECT latency jumped from ~400µs to ~7ms on a real fleet, the same
	 * order of magnitude as an actual acked write, for a statement that
	 * never touched any data.
	 *
	 * <p>Two earlier attempts at proving this with an artificially blocked
	 * peer both failed to actually exercise the bug and are worth recording
	 * so they aren't retried blind: (1) holding node B's propagator {@code
	 * writeLock} around the SELECT alone, after an already-acked INSERT had
	 * fully synced B -- {@code NodeServer.processLatticeValue} sends its ack
	 * unconditionally, right after the merge, and skips {@code cursor.sync()}
	 * (the only step that touches {@code writeLock}) entirely when nothing
	 * changed, so a no-op sync could ack instantly without ever touching the
	 * lock. (2) taking the lock *before* the row existed on B at all --
	 * {@code mergeIncoming} (the actual data merge into B's live cursor)
	 * doesn't touch {@code writeLock} either; only the propagator's own
	 * *outbound* republish step does, which isn't on the path that lets B
	 * ack an inbound message at all. Both attempts passed regardless of
	 * whether the fix was reverted, proving nothing.
	 *
	 * <p>Verified instead by direct, relative timing on the exact same
	 * {@code acks=1} connection: many rounds of SELECT (must never wait) vs.
	 * INSERT (must genuinely wait for B's real ack) against a live peer. If
	 * the bug were present, a SELECT would pay the same real round-trip
	 * cost as the INSERT, making the two comparable; fixed, the SELECT
	 * should be dramatically (not just marginally) faster. The bound
	 * (SELECT under half the INSERT's own average) is deliberately loose --
	 * this is a real, if fast, network round-trip either way, not a
	 * microbenchmark -- but wide enough that "comparable" (the buggy
	 * behaviour) could never pass it.
	 */
	@Test
	public void testWriteAcksDoesNotDelaySelectQueries() throws Exception {
		try (Connection connA = DriverManager.getConnection("jdbc:convex:database=" + dbName);
				Statement stA = connA.createStatement()) {
			stA.execute("CREATE TABLE T (ID INTEGER, NM VARCHAR)");
			stA.execute("SET WRITE_ACKS = 1");

			// Bound, reused PreparedStatements -- not interpolated SQL text
			// re-executed via plain Statement.execute(...). A first attempt
			// at this test used the latter and could not distinguish the fix
			// from the bug at all: every distinct statement text pays
			// Calcite's own full parse/plan/codegen cost (tens of ms, this
			// project's own already-documented "ad-hoc SQL" gap), which
			// dwarfs the real ack-wait cost this test is actually trying to
			// isolate and swamped it completely in both directions.
			try (java.sql.PreparedStatement ins = connA.prepareStatement("INSERT INTO T VALUES (?, ?)");
					java.sql.PreparedStatement sel = connA.prepareStatement("SELECT * FROM T WHERE ID = ?")) {
				int rounds = 30;
				long insertNanos = 0;
				long selectNanos = 0;
				for (int i = 0; i < rounds; i++) {
					long t0 = System.nanoTime();
					ins.setInt(1, i);
					ins.setString(2, "row-" + i);
					ins.executeUpdate();
					insertNanos += System.nanoTime() - t0;

					long t1 = System.nanoTime();
					sel.setInt(1, i);
					try (java.sql.ResultSet rs = sel.executeQuery()) {
						assertTrue(rs.next());
					}
					selectNanos += System.nanoTime() - t1;
				}

				double insertAvgMs = insertNanos / 1_000_000.0 / rounds;
				double selectAvgMs = selectNanos / 1_000_000.0 / rounds;
				System.out.printf("  acks=1: INSERT avg %.2fms, SELECT avg %.2fms over %d rounds%n",
					insertAvgMs, selectAvgMs, rounds);
				assertTrue(selectAvgMs < insertAvgMs / 2,
					"A SELECT on an acks=1 connection must be dramatically faster than a genuinely acked "
					+ "INSERT, not comparable to it -- INSERT avg " + insertAvgMs + "ms, SELECT avg " + selectAvgMs + "ms");
			}
		}
	}

	/**
	 * Requesting more acks than there are connected peers must cap and
	 * return promptly rather than blocking for the full configured
	 * timeout -- this fixture only has one peer.
	 */
	@Test
	public void testWriteAcksAboveConnectedPeerCountReturnsPromptly() throws Exception {
		try (Connection connA = DriverManager.getConnection("jdbc:convex:database=" + dbName);
				Statement stA = connA.createStatement()) {
			stA.execute("CREATE TABLE T (ID INTEGER, NM VARCHAR)");
			stA.execute("SET WRITE_ACKS = 50");
			long start = System.nanoTime();
			stA.execute("INSERT INTO T VALUES (3, 'capped')");
			long elapsedMs = (System.nanoTime() - start) / 1_000_000;
			assertTrue(elapsedMs < 4000,
				"Should cap at the one actually-connected peer, not wait for the full timeout: " + elapsedMs + "ms");
		}
	}
}
