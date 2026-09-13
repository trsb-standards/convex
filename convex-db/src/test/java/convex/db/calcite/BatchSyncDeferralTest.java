package convex.db.calcite;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BatchSyncDeferral}'s state machine directly —
 * deterministic, no flakiness, exercises exactly the sequence {@code
 * ConvexMeta}/{@code PgProtocolHandler} drive it through in production (see
 * that class's own doc for the live bug this closes: JDBC batching against
 * a live node paid a full sync per row, giving it zero throughput benefit
 * over an unbatched loop).
 */
class BatchSyncDeferralTest {

	@Test
	void endReturnsFalseWhenNoWindowWasEverOpen() {
		assertFalse(BatchSyncDeferral.end("conn-never-began"));
	}

	@Test
	void endReturnsFalseForAReadOnlyWindow() {
		String conn = "conn-readonly";
		BatchSyncDeferral.begin(conn);
		assertTrue(BatchSyncDeferral.isDeferring(conn));
		// No markDirty -- simulates a pipeline of only SELECTs.
		assertFalse(BatchSyncDeferral.end(conn));
		assertFalse(BatchSyncDeferral.isDeferring(conn));
	}

	@Test
	void endReturnsTrueExactlyOnceForAWriteWindowRegardlessOfHowManyRowsWereDirty() {
		String conn = "conn-many-writes";
		BatchSyncDeferral.begin(conn);
		for (int i = 0; i < 500; i++) {
			BatchSyncDeferral.markDirty(conn);
		}
		// This is the crux of the fix: however many rows marked dirty during
		// the window, end() reports "a sync is owed" exactly once, not once
		// per row -- the caller is expected to perform exactly one real
		// sync in response.
		assertTrue(BatchSyncDeferral.end(conn));
		// Consuming it clears the flag -- a second end() call (e.g. two
		// Syncs with nothing in between) finds nothing owed.
		assertFalse(BatchSyncDeferral.end(conn));
	}

	@Test
	void markDirtyBeforeAnyBeginIsHarmless() {
		String conn = "conn-dirty-without-begin";
		// Defensive: a caller that somehow calls markDirty without begin
		// (shouldn't happen given how ConvexMeta drives this, but the
		// registry itself shouldn't misbehave) still lets a later window
		// work correctly once begin() is actually called.
		BatchSyncDeferral.markDirty(conn);
		BatchSyncDeferral.begin(conn);
		BatchSyncDeferral.markDirty(conn);
		assertTrue(BatchSyncDeferral.end(conn));
	}

	@Test
	void discardClearsBothDeferringAndDirtyWithoutReportingASyncOwed() {
		String conn = "conn-discarded";
		BatchSyncDeferral.begin(conn);
		BatchSyncDeferral.markDirty(conn);
		BatchSyncDeferral.discard(conn);
		assertFalse(BatchSyncDeferral.isDeferring(conn));
		// end() after discard finds nothing -- the write that happened
		// during the discarded window is simply abandoned (matching a
		// connection tearing down mid-pipeline, never sending a final Sync).
		assertFalse(BatchSyncDeferral.end(conn));
	}

	@Test
	void distinctConnectionsDoNotInterfere() {
		String a = "conn-a";
		String b = "conn-b";
		BatchSyncDeferral.begin(a);
		BatchSyncDeferral.begin(b);
		BatchSyncDeferral.markDirty(a);
		// b stays read-only.
		assertTrue(BatchSyncDeferral.end(a));
		assertFalse(BatchSyncDeferral.end(b));
	}
}
