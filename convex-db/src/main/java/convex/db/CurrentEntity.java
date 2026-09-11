package convex.db;

/**
 * Per-thread record of the application <b>entity</b> (see
 * PLAN_access_rights.md's "Ownership" section, `dbase` repo) that the
 * currently-executing SQL statement's connection authenticated as — set by
 * {@code PgProtocolHandler} right around dispatching each incoming wire
 * message, once UCAN auth has resolved an issuer DID to an entity id, and
 * cleared immediately after that message finishes.
 *
 * <p>Null for a trust/no-password connection — bootstrap self-registration,
 * admin {@code psql}, and every direct {@code jdbc:convex:}/Java-API caller
 * never sets this at all. Rows written while it's null get {@code OWNERID =
 * NULL} ("system-owned") — see {@code ConvexTable.executeInsert}.
 *
 * <p>Deliberately a bare {@link ThreadLocal}, not connection-scoped state
 * threaded through Avatica's {@code StatementHandle}/{@code
 * ConnectionHandle} plumbing: Netty fully, synchronously processes one
 * channel's message before moving on to the next (no interleaving mid-message
 * on the same thread), so a plain set-around-dispatch / clear-after pattern
 * is safe even though one event-loop thread services many different
 * connections over its lifetime — see {@code PgProtocolHandler#channelRead}.
 */
public final class CurrentEntity {

	private static final ThreadLocal<Long> CURRENT = new ThreadLocal<>();

	private CurrentEntity() {}

	/** The current thread's resolved entity id, or null if not UCAN-authenticated. */
	public static Long get() {
		return CURRENT.get();
	}

	/** Sets the current thread's resolved entity id (may be null). */
	public static void set(Long entityId) {
		CURRENT.set(entityId);
	}

	/** Clears the current thread's resolved entity id. */
	public static void clear() {
		CURRENT.remove();
	}
}
