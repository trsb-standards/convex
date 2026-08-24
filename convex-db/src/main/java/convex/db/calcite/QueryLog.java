package convex.db.calcite;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optional hook fired by {@code ConvexMeta.prepareAndExecute}/{@code execute}
 * — Avatica's own dispatch point for every statement executed through the
 * {@code jdbc:convex:} driver, regardless of how the caller connected. That
 * matters: {@code PgServer}'s default connection supplier itself opens a
 * plain {@code jdbc:convex:database=...} connection and issues ordinary
 * {@code Statement}/{@code PreparedStatement} calls through it, so hooking
 * in here (rather than in {@code PgProtocolHandler}, tried first and found
 * to only cover pgwire clients) covers both pgwire *and* any direct Java
 * caller of this driver in one place, with no double-counting.
 *
 * <p>Also covers the handful of statements ({@code CREATE INDEX}/{@code DROP
 * INDEX}/{@code REPLICATE DB}/{@code REGISTER PEER}/{@code REPLICATE SCHEMA})
 * that {@code ConvexMeta} regex-intercepts and returns from *before* ever
 * reaching {@code super.prepareAndExecute(...)} — the whole method is timed,
 * not just the Calcite fallthrough path, so these aren't silently excluded.
 *
 * <p>Fired unconditionally, same "always fire, let the caller decide what to
 * do with it" pattern as {@code ConvexDdlExecutor}'s own hooks — this class
 * has no concept of what "slow" means for the embedding application (e.g. a
 * per-node duration threshold), so that decision belongs entirely to
 * whatever registers {@link #onQueryExecuted}.
 */
public final class QueryLog {

	private static final Logger log = LoggerFactory.getLogger(QueryLog.class);

	private QueryLog() {}

	/**
	 * One completed statement. {@code rowCount} is rows returned (SELECT) or
	 * affected (INSERT/UPDATE/DELETE) — 0 for the regex-intercepted admin
	 * statements (they have no meaningful row count), null if the statement
	 * failed before a count was ever established. {@code errorMessage} is
	 * null on success. {@code connectionId} is Avatica's own connection id
	 * ({@code Meta.StatementHandle#connectionId}) — a String, not a small
	 * int, since it must identify a connection consistently regardless of
	 * whether it arrived via pgwire or a direct JDBC caller. {@code source}
	 * is {@code "pgwire"} if this connection is {@code PgProtocolHandler}'s
	 * own internal one (proxying an external psql/pgwire client), else
	 * {@code "native"} (a direct {@code jdbc:convex:} caller, e.g. code in
	 * the same JVM using {@code ConvexConnectionManager} directly) — see
	 * {@link #markPgwireConnection}.
	 */
	public record Event(String sql, String queryType, long durationMs, Long rowCount, String errorMessage, String connectionId, String source) {}

	/** Callback for every completed statement — see the class javadoc. */
	public static volatile Consumer<Event> onQueryExecuted;

	/**
	 * Avatica connection ids known to be {@code PgProtocolHandler}'s own
	 * internal connections (one per pgwire client session) rather than a
	 * direct {@code jdbc:convex:} caller — both look structurally identical
	 * to {@code ConvexMeta} (a {@code jdbc:convex:} statement is a {@code
	 * jdbc:convex:} statement regardless of who's ultimately behind it), so
	 * this is the only place that distinction is actually recorded.
	 * {@code PgProtocolHandler} calls {@link #markPgwireConnection} right
	 * after opening its connection and {@link #unmarkPgwireConnection} right
	 * before closing it, so this stays bounded to genuinely open sessions.
	 */
	private static final Set<String> pgwireConnectionIds = ConcurrentHashMap.newKeySet();

	/** Marks a connection id as PgProtocolHandler's own — see {@link #pgwireConnectionIds}. */
	public static void markPgwireConnection(String connectionId) {
		if (connectionId != null) pgwireConnectionIds.add(connectionId);
	}

	/** Unmarks a connection id, e.g. when its pgwire session closes — see {@link #pgwireConnectionIds}. */
	public static void unmarkPgwireConnection(String connectionId) {
		if (connectionId != null) pgwireConnectionIds.remove(connectionId);
	}

	public static void fire(String sql, long durationMs, Long rowCount, String errorMessage, String connectionId) {
		Consumer<Event> callback = onQueryExecuted;
		if (callback == null) return;
		try {
			String source = pgwireConnectionIds.contains(connectionId) ? "pgwire" : "native";
			callback.accept(new Event(sql, queryTypeOf(sql), durationMs, rowCount, errorMessage, connectionId, source));
		} catch (Exception e) {
			// A logging hook must never be able to break query execution.
			log.warn("QueryLog.onQueryExecuted hook threw", e);
		}
	}

	private static String queryTypeOf(String sql) {
		if (sql == null) return "OTHER";
		String upper = sql.trim().toUpperCase();
		if (upper.startsWith("SELECT")) return "SELECT";
		if (upper.startsWith("INSERT")) return "INSERT";
		if (upper.startsWith("UPDATE")) return "UPDATE";
		if (upper.startsWith("DELETE")) return "DELETE";
		if (upper.startsWith("CREATE") || upper.startsWith("DROP") || upper.startsWith("ALTER")) return "DDL";
		if (upper.startsWith("REPLICATE") || upper.startsWith("REGISTER")) return "ADMIN";
		return "OTHER";
	}
}
