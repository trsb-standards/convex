package convex.api;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import convex.core.Result;
import convex.core.cvm.transactions.ATransaction;
import convex.core.data.Blob;
import convex.core.data.SignedData;
import convex.core.data.Vectors;
import convex.core.message.AConnection;
import convex.core.message.Message;
import convex.core.message.MessageTag;
import convex.core.message.MessageType;

/**
 * Tests that {@link Convex#requestNonBlocking(Message)} genuinely never
 * blocks on network I/O, even when the underlying connection's ordinary
 * {@code sendMessage} (used by {@link Convex#request(Message)}) would.
 *
 * <p>Found live 2026-08-24: {@code LatticePropagator.broadcastToPeersWithAcks}
 * originally used {@code request()}, whose send path is documented ({@code
 * AConnection.sendMessage}) to "may block with a bounded timeout if the
 * outbound queue is full" -- and that call ran inside {@code writeLock}, the
 * propagator's sole-writer lock. A single-row insert benchmark loop with
 * {@code SET WRITE_ACKS=2} against a real, busy target node never completed:
 * one blocked send held the lock and stalled every other write on that node,
 * not just the one statement. Fixed by adding this non-blocking variant
 * (uses {@code trySendMessage} instead) and switching
 * {@code broadcastToPeersWithAcks} to it.
 */
public class AConvexConnectedTest {

	/** Minimal concrete AConvexConnected wrapping an injected fake connection. */
	private static class TestConvex extends AConvexConnected {
		TestConvex(AConnection conn) {
			super(null, null);
			setConnection(conn);
		}
		@Override public CompletableFuture<Result> messageRaw(Blob message) {
			return CompletableFuture.completedFuture(Result.SENT_MESSAGE);
		}
		@Override public CompletableFuture<Result> transact(SignedData<ATransaction> tx) {
			return CompletableFuture.completedFuture(Result.SENT_MESSAGE);
		}
		@Override public boolean isConnected() {
			return connection != null;
		}
		@Override public void reconnect() { }
		@Override public <T extends convex.core.data.ACell> CompletableFuture<T> acquire(
				convex.core.data.Hash hash, convex.core.store.AStore store) {
			return CompletableFuture.failedFuture(new UnsupportedOperationException());
		}
		@Override public CompletableFuture<Result> requestStatus() {
			return CompletableFuture.completedFuture(Result.SENT_MESSAGE);
		}
		@Override public CompletableFuture<Result> query(convex.core.data.ACell query, convex.core.cvm.Address address) {
			return CompletableFuture.completedFuture(Result.SENT_MESSAGE);
		}
		@Override public InetSocketAddress getHostAddress() { return null; }
		@Override public String toString() { return "TestConvex"; }
		@Override protected CompletableFuture<Result> sendChallenge(convex.core.data.SignedData<convex.core.data.ACell> data) {
			return CompletableFuture.completedFuture(Result.SENT_MESSAGE);
		}
	}

	/**
	 * Fake connection simulating real backpressure: {@code sendMessage}
	 * blocks indefinitely (as the real, documented contract permits),
	 * {@code trySendMessage} returns immediately.
	 */
	private static class BlockingSendConnection extends AConnection {
		final CountDownLatch sendMessageEntered = new CountDownLatch(1);

		@Override public boolean sendMessage(Message msg) {
			sendMessageEntered.countDown();
			try {
				Thread.sleep(Long.MAX_VALUE);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			return true; // unreachable in practice -- this connection never un-blocks
		}
		@Override public boolean trySendMessage(Message msg) {
			return false; // simulates "buffer full", the documented non-blocking outcome
		}
		@Override public InetSocketAddress getRemoteAddress() { return null; }
		@Override public boolean isClosed() { return false; }
		@Override public void close() { }
		@Override public long getReceivedCount() { return 0; }
	}

	private static Message latticeValueMessage() {
		var payload = Vectors.create(MessageTag.LATTICE_VALUE, null, Vectors.empty(), Vectors.of(1));
		return Message.create(MessageType.LATTICE_VALUE, payload, payload.getEncoding());
	}

	@Test
	public void testRequestNonBlockingReturnsImmediatelyEvenWhenSendMessageWouldHang() throws Exception {
		BlockingSendConnection conn = new BlockingSendConnection();
		TestConvex convex = new TestConvex(conn);

		long start = System.nanoTime();
		Result result = convex.requestNonBlocking(latticeValueMessage()).get(5, TimeUnit.SECONDS);
		long elapsedMs = (System.nanoTime() - start) / 1_000_000;

		assertTrue(elapsedMs < 1000, "requestNonBlocking must not block: took " + elapsedMs + "ms");
		assertTrue(result.isError(), "trySendMessage returning false should surface as FULL_CLIENT_BUFFER");
		assertFalse(conn.sendMessageEntered.await(0, TimeUnit.MILLISECONDS),
			"requestNonBlocking must never call the blocking sendMessage path at all");
	}

	/**
	 * Confirms the fixture itself is real: the ordinary {@code request()}
	 * (which this fix stopped using inside the propagator's write lock)
	 * genuinely blocks against the same fake connection -- proving the
	 * fake accurately represents backpressure, not a trivially-fast stub.
	 */
	@Test
	public void testOrdinaryRequestGenuinelyBlocksAgainstTheSameFixture() throws Exception {
		BlockingSendConnection conn = new BlockingSendConnection();
		TestConvex convex = new TestConvex(conn);

		Thread t = new Thread(() -> convex.request(latticeValueMessage()));
		t.setDaemon(true);
		t.start();

		assertTrue(conn.sendMessageEntered.await(5, TimeUnit.SECONDS),
			"request() should have entered the blocking sendMessage path");
		Thread.sleep(200);
		assertTrue(t.isAlive(), "request() should still be blocked in sendMessage");
	}
}
