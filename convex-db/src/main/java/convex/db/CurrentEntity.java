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
	private static final ThreadLocal<String> CURRENT_DID = new ThreadLocal<>();

	/**
	 * The "entity" of a connection whose DID proved control of its key but
	 * is not bound to any active entity -- a guest. Deliberately NOT null:
	 * null means a trust connection, which may do everything. A guest may
	 * run only the statements the embedder's guest filter lets through
	 * (see PgServer.Builder#guestStatementFilter); no real entity has a
	 * negative id.
	 */
	public static final long GUEST_ENTITY_ID = -1L;

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
		CURRENT_DID.remove();
	}

	/** True if the current connection is a guest (an unbound DID). */
	public static boolean isGuest() {
		Long id = CURRENT.get();
		return id != null && id == GUEST_ENTITY_ID;
	}

	/**
	 * The DID the current connection authenticated with -- verified by the
	 * server (the token is signed by its key), unlike any DID passed as an
	 * argument. Null for a trust connection.
	 */
	public static String getDid() {
		return CURRENT_DID.get();
	}

	public static void setDid(String did) {
		if (did == null) CURRENT_DID.remove(); else CURRENT_DID.set(did);
	}
}
