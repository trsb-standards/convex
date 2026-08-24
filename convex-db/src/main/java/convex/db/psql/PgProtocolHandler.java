package convex.db.psql;

import convex.db.calcite.QueryLog;
import convex.db.psql.msg.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.calcite.avatica.AvaticaConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Handles the PostgreSQL wire protocol and executes SQL queries.
 */
public class PgProtocolHandler extends ChannelInboundHandlerAdapter {

	private static final Logger log = LoggerFactory.getLogger(PgProtocolHandler.class);
	private static final AtomicInteger processIdCounter = new AtomicInteger(1000);

	/**
	 * Matches an optional trailing zone offset on a bound TIMESTAMP
	 * parameter's wire text ({@code +HH}, {@code +HH:mm}, or the negative
	 * forms) — PGJDBC's own {@code TimestampUtils} appends this only when the
	 * client bound via the calendar-aware {@code setTimestamp(idx, ts, cal)}
	 * overload (targeting a TIMESTAMPTZ-flavoured column); a plain
	 * {@code setTimestamp(idx, ts)} bind never has one. Confirmed live: the
	 * offset can appear directly after a full {@code HH:mm:ss[.f]} time
	 * portion with no separating space ({@code "2018-08-01 00:00:00+00"}),
	 * or after a bare date with one ({@code "2021-09-20 +00"}) — a bound
	 * {@code java.sql.Date} value (source column has no time component at
	 * all) has no time portion to attach the offset to. Handling both shapes
	 * by stripping the offset first, rather than encoding a single rigid
	 * pattern, is deliberate.
	 */
	private static final java.util.regex.Pattern TIMESTAMP_OFFSET_SUFFIX =
		java.util.regex.Pattern.compile("\\s*([+-]\\d{2}(:?\\d{2})?)$");

	/** Postgres binary datetime epoch (2000-01-01T00:00:00Z), in millis since the real (1970) epoch. */
	private static final long PG_BINARY_EPOCH_MILLIS = 946684800000L;

	/**
	 * Parses a bound TIMESTAMP parameter's wire text into a {@link Timestamp}
	 * — see {@link #TIMESTAMP_OFFSET_SUFFIX}'s own doc for the shapes this
	 * has to tolerate.
	 */
	private static Timestamp parseTimestampText(String strValue) {
		String s = strValue.trim();
		java.util.regex.Matcher m = TIMESTAMP_OFFSET_SUFFIX.matcher(s);
		String offset = null;
		if (m.find()) {
			offset = m.group(1);
			if (offset.length() == 3) offset = offset + ":00"; // "+00" -> "+00:00"
			s = s.substring(0, m.start());
		}
		java.time.LocalDateTime ldt = s.indexOf(' ') < 0
			? java.time.LocalDate.parse(s).atStartOfDay()
			: java.time.LocalDateTime.parse(s.replace(' ', 'T'));
		return offset == null
			? Timestamp.valueOf(ldt)
			: Timestamp.from(ldt.atOffset(java.time.ZoneOffset.of(offset)).toInstant());
	}

	private final Function<String, Connection> connectionSupplier;
	private final String requiredPassword;

	private Connection connection;
	private String user;
	private String database;
	private int processId;
	private int secretKey;
	private boolean authenticated = false;

	/**
	 * Tracks whether this connection is inside an explicit {@code BEGIN}
	 * block, and whether a statement inside it has failed -- mirrors the
	 * transaction-status byte real PostgreSQL reports in every {@code
	 * ReadyForQuery} message ({@link ReadyForQuery#IDLE}/{@link
	 * ReadyForQuery#IN_TRANSACTION}/{@link ReadyForQuery#FAILED_TRANSACTION}).
	 *
	 * <p>Needed because {@code BEGIN}/{@code COMMIT}/{@code ROLLBACK} arrive
	 * as plain wire-text SQL (PostgreSQL's wire protocol has no dedicated
	 * message for transaction control -- pgjdbc's own {@code
	 * setAutoCommit(false)}/{@code commit()}/{@code rollback()} just send
	 * these three literal strings via the ordinary simple-query path), which
	 * {@link #handleQuery} must intercept and translate onto the backing
	 * {@link #connection}'s real {@code setAutoCommit}/{@code commit}/{@code
	 * rollback} -- {@code ConvexMeta} already implements the actual fork/sync
	 * transaction semantics behind those calls, this class only bridges wire
	 * text to them. Without also reporting the right status byte back,
	 * pgjdbc's own client-side transaction-state tracking desyncs from
	 * reality even once the translation itself works.
	 */
	private boolean inTransaction = false;
	private boolean transactionFailed = false;

	/**
	 * Creates a handler that uses the given connection supplier.
	 *
	 * @param connectionSupplier Supplies JDBC connections for query execution, given the client's requested database name
	 * @param requiredPassword Password required for authentication, or null for trust auth
	 */
	public PgProtocolHandler(Function<String, Connection> connectionSupplier, String requiredPassword) {
		this.connectionSupplier = connectionSupplier;
		this.requiredPassword = requiredPassword;
		this.processId = processIdCounter.incrementAndGet();
		this.secretKey = ThreadLocalRandom.current().nextInt();
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof PgMessageDecoder.SSLRequest) {
			handleSSLRequest(ctx);
		} else if (msg instanceof PgMessageDecoder.StartupMessage startup) {
			handleStartup(ctx, startup);
		} else if (msg instanceof PgMessageDecoder.PasswordMessage pwd) {
			handlePassword(ctx, pwd);
		} else if (msg instanceof PgMessageDecoder.Query query) {
			handleQuery(ctx, query);
		} else if (msg instanceof PgMessageDecoder.Terminate) {
			handleTerminate(ctx);
		} else if (msg instanceof PgMessageDecoder.Sync) {
			handleSync(ctx);
		} else if (msg instanceof PgMessageDecoder.Parse parse) {
			handleParse(ctx, parse);
		} else if (msg instanceof PgMessageDecoder.Bind bind) {
			handleBind(ctx, bind);
		} else if (msg instanceof PgMessageDecoder.Describe describe) {
			handleDescribe(ctx, describe);
		} else if (msg instanceof PgMessageDecoder.Execute execute) {
			handleExecute(ctx, execute);
		} else if (msg instanceof PgMessageDecoder.Close close) {
			handleClose(ctx, close);
		} else if (msg instanceof PgMessageDecoder.Flush) {
			ctx.flush();
		} else {
			log.warn("Unhandled message: {}", msg.getClass().getSimpleName());
		}
	}

	private void handleSSLRequest(ChannelHandlerContext ctx) {
		// Respond with 'N' - we don't support SSL
		ByteBuf buf = ctx.alloc().buffer(1);
		buf.writeByte('N');
		ctx.writeAndFlush(buf);
	}

	private void handleStartup(ChannelHandlerContext ctx, PgMessageDecoder.StartupMessage startup) {
		log.debug("Startup: version={}.{}, params={}", startup.majorVersion(), startup.minorVersion(), startup.params());

		this.user = startup.params().get("user");
		this.database = startup.params().get("database");

		if (requiredPassword != null && !requiredPassword.isEmpty()) {
			// Request password authentication
			write(ctx, AuthenticationCleartextPassword.INSTANCE);
			ctx.flush();
		} else {
			// Trust authentication
			completeAuthentication(ctx);
		}
	}

	private void handlePassword(ChannelHandlerContext ctx, PgMessageDecoder.PasswordMessage pwd) {
		if (requiredPassword != null && requiredPassword.equals(pwd.password())) {
			completeAuthentication(ctx);
		} else {
			write(ctx, ErrorResponse.authenticationFailed(user));
			ctx.flush();
			ctx.close();
		}
	}

	private void completeAuthentication(ChannelHandlerContext ctx) {
		authenticated = true;

		// Get a connection, honouring whichever database this client requested
		try {
			connection = connectionSupplier.apply(database);
			// Marks this connection's Avatica id as ours, not a direct
			// jdbc:convex: caller's -- see QueryLog.markPgwireConnection's own
			// javadoc for why this distinction can't be made any other way.
			// instanceof, not a bare cast: connectionSupplier is pluggable
			// (e.g. PgServer.builder().connectionSupplier(...) in tests), so a
			// non-Avatica Connection is a legitimate possibility here.
			if (connection instanceof AvaticaConnection avaticaConn) {
				QueryLog.markPgwireConnection(avaticaConn.id);
			}
		} catch (Exception e) {
			log.error("Failed to get connection", e);
			write(ctx, ErrorResponse.fromException(e));
			ctx.close();
			return;
		}

		// Send authentication OK
		write(ctx, AuthenticationOk.INSTANCE);

		// Send parameter status messages
		write(ctx, ParameterStatus.serverVersion("15.0 (Convex)"));
		write(ctx, ParameterStatus.clientEncoding("UTF8"));
		write(ctx, ParameterStatus.serverEncoding("UTF8"));
		write(ctx, ParameterStatus.dateStyle("ISO, MDY"));
		write(ctx, ParameterStatus.timeZone("UTC"));
		write(ctx, ParameterStatus.integerDatetimes(true));
		write(ctx, new ParameterStatus("standard_conforming_strings", "on"));

		// Send backend key data
		write(ctx, new BackendKeyData(processId, secretKey));

		// Ready for query
		write(ctx, ReadyForQuery.IDLE_INSTANCE);
		ctx.flush();
	}

	/** The real transaction-status byte for the next {@code ReadyForQuery}, per {@link #inTransaction}/{@link #transactionFailed}. */
	private ReadyForQuery currentReadyState() {
		if (transactionFailed) return new ReadyForQuery(ReadyForQuery.FAILED_TRANSACTION);
		if (inTransaction) return new ReadyForQuery(ReadyForQuery.IN_TRANSACTION);
		return ReadyForQuery.IDLE_INSTANCE;
	}

	/** Matches the wire-text forms pgjdbc (and psql) send for {@code setAutoCommit(false)}. */
	private static boolean isBeginStatement(String upperSql) {
		return upperSql.equals("BEGIN") || upperSql.startsWith("BEGIN ")
			|| upperSql.startsWith("START TRANSACTION");
	}

	/** Matches the wire-text forms sent for {@code commit()}. */
	private static boolean isCommitStatement(String upperSql) {
		return upperSql.equals("COMMIT") || upperSql.startsWith("COMMIT ")
			|| upperSql.equals("END") || upperSql.startsWith("END ");
	}

	/** Matches the wire-text forms sent for {@code rollback()}. */
	private static boolean isRollbackStatement(String upperSql) {
		return upperSql.equals("ROLLBACK") || upperSql.startsWith("ROLLBACK ")
			|| upperSql.equals("ABORT") || upperSql.startsWith("ABORT ");
	}

	/**
	 * Executes {@code sql} as a transaction-control statement (BEGIN/COMMIT/
	 * ROLLBACK) if it matches one, translating it onto the backing {@link
	 * #connection}'s real {@code setAutoCommit}/{@code commit}/{@code
	 * rollback} and updating {@link #inTransaction}/{@link
	 * #transactionFailed}. Returns false (nothing written, caller should run
	 * {@code sql} as ordinary SQL instead) if it isn't one.
	 *
	 * <p>Must be checked from <em>both</em> {@link #handleQuery} (the simple
	 * "Q" message path) and {@link #executeWithParameters} (the extended
	 * Parse/Bind/Execute path) — checked live 2026-08-12: a literal {@code
	 * Statement.execute("BEGIN")} (e.g. from {@code psql -c} or a plain
	 * pgjdbc {@code Statement}) arrives via simple query, but pgjdbc's own
	 * {@code Connection.commit()}/{@code rollback()} send their literal
	 * {@code "COMMIT"}/{@code "ROLLBACK"} text via the extended protocol
	 * instead, even though there are no bind parameters. Missing either path
	 * means half of an ordinary JDBC transaction silently falls through to
	 * Calcite's parser, which has no grammar for any of these three words.
	 */
	private boolean handleTransactionControl(ChannelHandlerContext ctx, String upperSql) throws SQLException {
		if (isBeginStatement(upperSql)) {
			connection.setAutoCommit(false);
			inTransaction = true;
			transactionFailed = false;
			write(ctx, CommandComplete.begin());
			return true;
		}
		if (isCommitStatement(upperSql)) {
			connection.commit();
			connection.setAutoCommit(true);
			inTransaction = false;
			transactionFailed = false;
			write(ctx, CommandComplete.commit());
			return true;
		}
		if (isRollbackStatement(upperSql)) {
			connection.rollback();
			connection.setAutoCommit(true);
			inTransaction = false;
			transactionFailed = false;
			write(ctx, CommandComplete.rollback());
			return true;
		}
		return false;
	}

	private void handleQuery(ChannelHandlerContext ctx, PgMessageDecoder.Query query) {
		if (!authenticated) {
			write(ctx, ErrorResponse.builder()
				.severity("FATAL")
				.code("28000")
				.message("not authenticated")
				.build());
			ctx.close();
			return;
		}

		String fullSql = query.sql().trim();
		log.debug("Query: {}", fullSql);

		if (fullSql.isEmpty()) {
			write(ctx, new EmptyQueryResponse());
			write(ctx, currentReadyState());
			ctx.flush();
			return;
		}

		// Split on semicolons and execute each statement
		// Note: This is a simple split - doesn't handle semicolons in strings
		String[] statements = fullSql.split(";");

		for (String sql : statements) {
			sql = sql.trim();
			if (sql.isEmpty()) {
				continue;
			}
			String upperSql = sql.toUpperCase();

			// Handle special commands
			if (upperSql.startsWith("SET ")) {
				// Ignore SET commands for now
				write(ctx, CommandComplete.set());
				continue;
			}

			try {
				if (!handleTransactionControl(ctx, upperSql)) {
					executeQuery(ctx, sql);
				}
			} catch (SQLException e) {
				// Expected client-caused error (bad SQL, missing table, etc.) — the
				// client already gets the real message via ErrorResponse below;
				// a stack trace here would just be log noise for an ordinary typo.
				if (inTransaction) transactionFailed = true;
				log.warn("Query error: {}", e.getMessage());
				write(ctx, ErrorResponse.fromException(e));
				// Stop processing on error
				break;
			} catch (RuntimeException e) {
				// Runtime exceptions from Calcite (type coercion, etc.) are query errors
				if (inTransaction) transactionFailed = true;
				log.warn("Query execution error: {}", e.getMessage());
				write(ctx, ErrorResponse.fromException(e));
				break;
			} catch (Exception e) {
				// Not a recognised query-error type — genuinely unexpected, so the
				// stack trace is worth keeping here.
				if (inTransaction) transactionFailed = true;
				log.error("Unexpected error", e);
				write(ctx, ErrorResponse.fromException(e));
				break;
			}
		}

		write(ctx, currentReadyState());
		ctx.flush();
	}

	private void executeQuery(ChannelHandlerContext ctx, String sql) throws SQLException {
		executeQuery(ctx, sql, true, true);
	}

	/**
	 * @param includeRowDescription Whether to send a RowDescription before the
	 *   data rows. Must be false when called from the extended-protocol Execute
	 *   path for a portal that was already Described — Describe already sent the
	 *   RowDescription, and sending it again desyncs pgjdbc's internal
	 *   pendingDescribePortalQueue bookkeeping (manifests client-side as a
	 *   NoSuchElementException in QueryExecutorImpl.processResults).
	 * @param sendTimingNotice Whether to send the "N rows in set (X sec)" /
	 *   "Query OK, N rows affected (X sec)" NoticeResponse footer. True only
	 *   for the simple-query path (psql, or any literal-text client) — the
	 *   footer exists purely for a human reading a terminal. Every extended-
	 *   protocol caller (a bound PreparedStatement, i.e. always a program, never
	 *   a person typing at psql) passes false: that caller has to receive and
	 *   discard the message for zero benefit, once per row for a bulk insert.
	 */
	private void executeQuery(ChannelHandlerContext ctx, String sql, boolean includeRowDescription, boolean sendTimingNotice) throws SQLException {
		sql = rewriteQuery(sql);

		// Null means return empty result (e.g., for system catalog queries)
		if (sql == null) {
			write(ctx, CommandComplete.select(0));
			return;
		}

		long startNanos = System.nanoTime();
		try (Statement stmt = connection.createStatement()) {
			boolean hasResultSet = stmt.execute(sql);

			if (hasResultSet) {
				long rowCount;
				try (ResultSet rs = stmt.getResultSet()) {
					rowCount = sendResultSet(ctx, rs, includeRowDescription);
				}
				if (sendTimingNotice) {
					write(ctx, NoticeResponse.timing(
						rowCount + " " + rowsWord(rowCount) + " in set (" + formatElapsed(startNanos) + ")"));
				}
			} else {
				int updateCount = stmt.getUpdateCount();
				String upperSql = sql.toUpperCase().trim();

				if (upperSql.startsWith("INSERT")) {
					write(ctx, CommandComplete.insert(updateCount));
				} else if (upperSql.startsWith("UPDATE")) {
					write(ctx, CommandComplete.update(updateCount));
				} else if (upperSql.startsWith("DELETE")) {
					write(ctx, CommandComplete.delete(updateCount));
				} else if (upperSql.startsWith("CREATE")) {
					write(ctx, CommandComplete.createTable());
				} else if (upperSql.startsWith("DROP")) {
					write(ctx, CommandComplete.dropTable());
				} else {
					write(ctx, new CommandComplete("OK"));
				}
				if (sendTimingNotice) {
					write(ctx, NoticeResponse.timing("Query OK, " + updateCount + " " + rowsWord(updateCount)
						+ " affected (" + formatElapsed(startNanos) + ")"));
				}
			}
		}
	}

	/**
	 * Rewrites PostgreSQL-specific SQL to Calcite-compatible syntax.
	 * Returns null if the query should return an empty result set.
	 *
	 * <p>WARNING: This method contains numerous hacks to work around Calcite's
	 * lack of native PostgreSQL syntax support. Each hack is marked with a TODO
	 * indicating the proper solution.
	 */
	private String rewriteQuery(String sql) {
		if (sql == null) return sql;

		sql = sql.trim();
		String lowerSql = sql.toLowerCase();

		// TODO: Remove hack needed to handle PostgreSQL regex operators (!~, ~, ~*, !~*)
		// Proper fix: Register custom Calcite operators for POSIX regex matching,
		// or use Calcite Babel parser which has built-in PostgreSQL dialect support
		if (sql.contains("!~") || sql.contains("~*") ||
			(sql.contains("~") && !sql.contains("~=") && !sql.contains("~~"))) {
			return null; // Return empty result for regex queries
		}

		// TODO: Remove hack needed to handle queries for unimplemented pg_catalog tables
		// Proper fix: Implement virtual tables for pg_constraint, pg_index, pg_proc,
		// pg_views, pg_settings, pg_am, pg_roles, pg_stat*, and information_schema
		if (lowerSql.contains("pg_constraint") ||
			lowerSql.contains("pg_index") ||
			lowerSql.contains("pg_proc") ||
			lowerSql.contains("pg_views") ||
			lowerSql.contains("pg_settings") ||
			lowerSql.contains("pg_am") ||
			lowerSql.contains("pg_roles") ||
			lowerSql.contains("pg_stat") ||
			lowerSql.contains("information_schema.")) {
			return null; // Signal to return empty result
		}

		// TODO: Remove hack needed to emulate PostgreSQL search_path behavior
		// Proper fix: Implement Calcite's SchemaPlus.setPath() to configure search path,
		// or use a custom SqlValidator that resolves unqualified table names
		sql = addPgCatalogPrefix(sql, "pg_database");
		sql = addPgCatalogPrefix(sql, "pg_type");
		sql = addPgCatalogPrefix(sql, "pg_class");
		sql = addPgCatalogPrefix(sql, "pg_namespace");
		sql = addPgCatalogPrefix(sql, "pg_attribute");
		sql = addPgCatalogPrefix(sql, "pg_tables");

		// TODO: Remove hack needed to handle CURRENT_SCHEMA function
		// Proper fix: Register CURRENT_SCHEMA as a Calcite ScalarFunction that
		// returns the current schema from session context
		sql = sql.replaceAll("(?i)CURRENT_SCHEMA\\s*\\(\\s*\\)", "'public'");
		sql = sql.replaceAll("(?i)\\bCURRENT_SCHEMA\\b", "'public'");

		// TODO: Remove hack needed to handle CURRENT_DATABASE function
		// Proper fix: Register CURRENT_DATABASE as a Calcite ScalarFunction that
		// returns the database name from session context
		sql = sql.replaceAll("(?i)CURRENT_DATABASE\\s*\\(\\s*\\)", "'" + database + "'");

		// TODO: Remove hack needed to handle CURRENT_USER keyword
		// Proper fix: Register CURRENT_USER as a Calcite ScalarFunction that
		// returns the authenticated username from session context
		sql = sql.replaceAll("(?i)\\bCURRENT_USER\\b", "'convex'");

		// TODO: Remove hack needed to handle SESSION_USER keyword
		// Proper fix: Register SESSION_USER as a Calcite ScalarFunction that
		// returns the session username from session context
		sql = sql.replaceAll("(?i)\\bSESSION_USER\\b", "'convex'");

		// TODO: Remove hack needed to handle version() function
		// Proper fix: Register version() as a Calcite ScalarFunction that
		// returns appropriate version string
		sql = sql.replaceAll("(?i)\\bversion\\s*\\(\\s*\\)", "'PostgreSQL 15.0 (Convex SQL)'");

		// TODO: Remove hack needed to handle pg_backend_pid() function
		// Proper fix: Register pg_backend_pid() as a Calcite ScalarFunction that
		// returns the connection's process ID from session context
		sql = sql.replaceAll("(?i)pg_backend_pid\\s*\\(\\s*\\)", String.valueOf(processId));

		// TODO: Remove hack needed to handle PostgreSQL cast syntax (::type)
		// Proper fix: Use Calcite Babel parser with PostgreSQL conformance,
		// which natively supports :: cast syntax
		sql = sql.replaceAll("::integer", "");
		sql = sql.replaceAll("::int", "");
		sql = sql.replaceAll("::int4", "");
		sql = sql.replaceAll("::int8", "");
		sql = sql.replaceAll("::bigint", "");
		sql = sql.replaceAll("::text", "");
		sql = sql.replaceAll("::varchar", "");
		sql = sql.replaceAll("::regclass", "");
		sql = sql.replaceAll("::oid", "");

		return sql;
	}

	private long sendResultSet(ChannelHandlerContext ctx, ResultSet rs, boolean includeRowDescription) throws SQLException {
		ResultSetMetaData meta = rs.getMetaData();
		int columnCount = meta.getColumnCount();

		// Send row description
		if (includeRowDescription) {
			write(ctx, RowDescription.fromMetaData(meta));
		}

		// Send data rows
		long rowCount = 0;
		while (rs.next()) {
			write(ctx, DataRow.fromResultSet(rs, columnCount));
			rowCount++;
		}

		// Send command complete
		write(ctx, CommandComplete.select(rowCount));
		return rowCount;
	}

	/**
	 * Formats elapsed time since {@code startNanos} the same way MySQL's CLI
	 * reports it ("0.008 sec") — a first step towards surfacing
	 * replication-staleness timing (sync time vs. query time) once that
	 * mechanism exists; for now this is just the query's own execution time.
	 */
	private static String formatElapsed(long startNanos) {
		double seconds = (System.nanoTime() - startNanos) / 1_000_000_000.0;
		return String.format("%.3f sec", seconds);
	}

	private static String rowsWord(long count) {
		return count == 1 ? "row" : "rows";
	}

	// ========== Extended Query Protocol ==========

	/**
	 * Prepared statement info - stores the query and parameter types.
	 */
	private record PreparedStmt(String query, int[] paramTypes) {}

	/**
	 * Portal info - a bound prepared statement ready for execution.
	 */
	private record Portal(PreparedStmt stmt, byte[][] paramValues, short[] paramFormats, short[] resultFormats) {}

	// Statements are named prepared statements (Parse creates these)
	private final Map<String, PreparedStmt> statements = new java.util.HashMap<>();
	// Portals are bound statements ready to execute (Bind creates these)
	private final Map<String, Portal> portals = new java.util.HashMap<>();

	/**
	 * Real, reusable JDBC {@link PreparedStatement}s, one per still-live
	 * {@link PreparedStmt} (keyed by identity, not content -- exactly one
	 * real statement per wire-level Parse, which is what we actually want to
	 * reuse). Absent until first bound+executed (see {@link
	 * #executeWithParameters}), then kept for the wire-level statement's
	 * whole lifetime.
	 *
	 * <p>Fixes a real perf bug (found live 2026-08-12 investigating why an
	 * explicit-transaction benchmark barely sped up after fixing {@code
	 * BEGIN}/{@code COMMIT} interception): {@code executeWithParameters} used
	 * to call {@code connection.prepareStatement(jdbcSql)} fresh on
	 * <em>every single Bind+Execute</em>, throwing the statement away in a
	 * try-with-resources right after. A real, wire-level "prepared"
	 * statement therefore never actually reused Calcite's parse/plan/codegen
	 * across repeated executions with different bound values -- exactly the
	 * scenario a JDBC {@code PreparedStatement} exists to make cheap. {@code
	 * ConvexMeta.execute(StatementHandle, List, int)} already implements the
	 * fast "just bind and run" path Avatica expects for a reused statement
	 * (as opposed to {@code prepareAndExecute}, which does the full parse);
	 * this cache is what actually lets the wire protocol reach it.
	 */
	private final Map<PreparedStmt, PreparedStatement> preparedStatementCache = new java.util.IdentityHashMap<>();

	/** Evicts and closes the cached real statement for a wire-level statement being replaced or dropped, if any. */
	private void evictCachedStatement(PreparedStmt removed) {
		if (removed == null) return;
		PreparedStatement cached = preparedStatementCache.remove(removed);
		if (cached != null) {
			try {
				cached.close();
			} catch (SQLException e) {
				log.debug("Error closing cached prepared statement", e);
			}
		}
	}
	// Portal names for which Describe already sent a RowDescription to the client
	// — Execute must not resend it (see executeQuery's includeRowDescription doc).
	private final java.util.Set<String> describedPortals = new java.util.HashSet<>();

	private void handleParse(ChannelHandlerContext ctx, PgMessageDecoder.Parse parse) {
		if (!authenticated) {
			sendErrorAndClose(ctx, "28000", "not authenticated");
			return;
		}

		try {
			String name = parse.name();
			String query = parse.query();
			log.debug("Parse: name='{}', query='{}'", name, query);

			// Close existing statement with same name (PostgreSQL behavior)
			evictCachedStatement(statements.remove(name));

			// Store the prepared statement
			statements.put(name, new PreparedStmt(query, parse.paramTypes()));

			write(ctx, ParseComplete.INSTANCE);
		} catch (Exception e) {
			log.warn("Parse error: {}", e.getMessage());
			write(ctx, ErrorResponse.fromException(e));
		}
	}

	private void handleBind(ChannelHandlerContext ctx, PgMessageDecoder.Bind bind) {
		if (!authenticated) {
			sendErrorAndClose(ctx, "28000", "not authenticated");
			return;
		}

		try {
			String portalName = bind.portal();
			String stmtName = bind.statement();
			log.debug("Bind: portal='{}', statement='{}', params={}", portalName, stmtName, bind.paramValues().length);

			// Find the prepared statement
			PreparedStmt stmt = statements.get(stmtName);
			if (stmt == null) {
				write(ctx, ErrorResponse.builder()
					.severity("ERROR")
					.code("26000") // invalid_sql_statement_name
					.message("prepared statement \"" + stmtName + "\" does not exist")
					.build());
				return;
			}

			// Close existing portal with same name (PostgreSQL behavior)
			portals.remove(portalName);
			describedPortals.remove(portalName);

			// Create the portal with bound parameters
			portals.put(portalName, new Portal(stmt, bind.paramValues(), bind.paramFormats(), bind.resultFormats()));

			write(ctx, BindComplete.INSTANCE);
		} catch (Exception e) {
			log.warn("Bind error: {}", e.getMessage());
			write(ctx, ErrorResponse.fromException(e));
		}
	}

	private void handleDescribe(ChannelHandlerContext ctx, PgMessageDecoder.Describe describe) {
		if (!authenticated) {
			sendErrorAndClose(ctx, "28000", "not authenticated");
			return;
		}

		log.debug("Describe: type={}, name='{}'", (char) describe.type(), describe.name());

		try {
			if (describe.type() == 'S') {
				// Describe prepared statement
				PreparedStmt stmt = statements.get(describe.name());
				if (stmt == null) {
					write(ctx, ErrorResponse.builder()
						.severity("ERROR")
						.code("26000")
						.message("prepared statement \"" + describe.name() + "\" does not exist")
						.build());
					return;
				}

				// Send parameter description
				write(ctx, new ParameterDescription(stmt.paramTypes()));

				String query = rewriteQuery(stmt.query());
				if (query != null && query.toUpperCase().trim().startsWith("SELECT")) {
					// Execute to get metadata (substitute $N with NULL for metadata query)
					String metaQuery = query.replaceAll("\\$\\d+", "NULL");
					try (Statement s = connection.createStatement();
						 ResultSet rs = s.executeQuery(metaQuery)) {
						write(ctx, RowDescription.fromMetaData(rs.getMetaData()));
					} catch (SQLException e) {
						// If metadata query fails, return NoData
						write(ctx, NoData.INSTANCE);
					}
				} else {
					write(ctx, NoData.INSTANCE);
				}
			} else {
				// Describe portal
				Portal portal = portals.get(describe.name());
				if (portal == null) {
					write(ctx, ErrorResponse.builder()
						.severity("ERROR")
						.code("34000") // invalid_cursor_name
						.message("portal \"" + describe.name() + "\" does not exist")
						.build());
					return;
				}

				String query = rewriteQuery(portal.stmt().query());
				if (query != null && query.toUpperCase().trim().startsWith("SELECT")) {
					String metaQuery = query.replaceAll("\\$\\d+", "NULL");
					try (Statement s = connection.createStatement();
						 ResultSet rs = s.executeQuery(metaQuery)) {
						write(ctx, RowDescription.fromMetaData(rs.getMetaData()));
						describedPortals.add(describe.name());
					} catch (SQLException e) {
						write(ctx, NoData.INSTANCE);
					}
				} else {
					write(ctx, NoData.INSTANCE);
				}
			}
		} catch (Exception e) {
			log.warn("Describe error: {}", e.getMessage());
			write(ctx, ErrorResponse.fromException(e));
		}
	}

	private void handleExecute(ChannelHandlerContext ctx, PgMessageDecoder.Execute execute) {
		if (!authenticated) {
			sendErrorAndClose(ctx, "28000", "not authenticated");
			return;
		}

		String portalName = execute.portal();
		log.debug("Execute: portal='{}', maxRows={}", portalName, execute.maxRows());

		try {
			Portal portal = portals.get(portalName);
			if (portal == null) {
				write(ctx, ErrorResponse.builder()
					.severity("ERROR")
					.code("34000")
					.message("portal \"" + portalName + "\" does not exist")
					.build());
				return;
			}

			String query = portal.stmt().query();
			if (query == null || query.trim().isEmpty()) {
				write(ctx, new EmptyQueryResponse());
				return;
			}

			// If Describe already sent this portal's RowDescription, Execute must not
			// resend it — doing so desyncs pgjdbc's client-side bookkeeping.
			boolean alreadyDescribed = describedPortals.remove(portalName);
			executeWithParameters(ctx, portal.stmt(), portal.paramValues(), portal.paramFormats(), !alreadyDescribed);
		} catch (SQLException e) {
			if (inTransaction) transactionFailed = true;
			log.warn("Execute error: {}", e.getMessage());
			write(ctx, ErrorResponse.fromException(e));
		} catch (RuntimeException e) {
			// Runtime exceptions from Calcite (type coercion, etc.) are query errors
			if (inTransaction) transactionFailed = true;
			log.warn("Query execution error: {}", e.getMessage());
			write(ctx, ErrorResponse.fromException(e));
		} catch (Exception e) {
			// Not a recognised query-error type — genuinely unexpected, so the
			// stack trace is worth keeping here.
			if (inTransaction) transactionFailed = true;
			log.error("Unexpected error during execute", e);
			write(ctx, ErrorResponse.fromException(e));
		}
	}

	/**
	 * Execute a query with bound parameters.
	 */
	private void executeWithParameters(ChannelHandlerContext ctx, PreparedStmt stmt, byte[][] paramValues, short[] paramFormats, boolean includeRowDescription) throws SQLException {
		String sql = rewriteQuery(stmt.query());

		if (sql == null) {
			write(ctx, CommandComplete.select(0));
			return;
		}

		// pgjdbc's own Connection.commit()/rollback() send their literal
		// "COMMIT"/"ROLLBACK" text via this extended-protocol path (not the
		// simple one handleQuery covers) even though there are no bind
		// parameters -- see handleTransactionControl's own doc.
		if (handleTransactionControl(ctx, sql.toUpperCase())) {
			return;
		}

		// If no parameters, execute directly. sendTimingNotice=false: this is
		// always the extended protocol (a bound PreparedStatement), i.e.
		// always a program, never a person typing at psql -- see
		// executeQuery's own doc.
		if (paramValues == null || paramValues.length == 0) {
			executeQuery(ctx, sql, includeRowDescription, false);
			return;
		}

		// For pg_catalog queries, substitute parameters directly
		// (Calcite's virtual tables don't support prepared statements well)
		String lowerSql = sql.toLowerCase();
		if (lowerSql.contains("pg_catalog") || lowerSql.contains("pg_database") ||
			lowerSql.contains("pg_type") || lowerSql.contains("pg_class") ||
			lowerSql.contains("pg_namespace") || lowerSql.contains("pg_attribute") ||
			lowerSql.contains("pg_tables")) {
			String substituted = substituteParameters(sql, paramValues, paramFormats);
			executeQuery(ctx, substituted, includeRowDescription, false);
			return;
		}

		// Convert $1, $2 to ? for JDBC
		String jdbcSql = sql.replaceAll("\\$\\d+", "?");

		// Reuse the real JDBC PreparedStatement across every Bind+Execute for
		// this wire-level statement (see preparedStatementCache's own doc) --
		// deliberately NOT try-with-resources: closing it here would defeat
		// the whole point, throwing away the cached query plan right after
		// paying to build it. It's closed instead when the wire-level
		// statement is replaced/closed (evictCachedStatement) or the
		// connection tears down (closeConnection).
		{
			PreparedStatement pstmt = preparedStatementCache.get(stmt);
			if (pstmt == null) {
				pstmt = connection.prepareStatement(jdbcSql);
				preparedStatementCache.put(stmt, pstmt);
			}
			// Bind parameters. Every text-format value arrives here as a
			// byte[] with no type of its own (the pg wire protocol's text
			// format is just the value's string representation) — binding
			// it via setString() unconditionally, regardless of the target
			// column's real type, used to be the only thing this did. That
			// silently broke every non-VARCHAR column: a bound value ends up
			// as a Java String sitting where ConvexTable.executeInsert (and
			// friends) expect the real typed value (e.g. Number for
			// INTEGER), so it fails with a ClassCastException at execution
			// time — not at bind time, so it looked like an "execute" bug
			// until traced back here. The same applies to a bound NULL: an
			// untyped java.sql.Types.NULL flows into Avatica as a ByteString
			// placeholder, which fails the identical cast. getParameterType
			// gives us the real target type so both cases can be bound
			// correctly; a lookup failure falls back to the old
			// string/generic-NULL behavior (correct only for VARCHAR
			// columns, same as before this fix).
			ParameterMetaData paramMeta = null;
			try {
				paramMeta = pstmt.getParameterMetaData();
			} catch (SQLException ignore) {
				// no metadata available -- every param falls back to VARCHAR below
			}
			for (int i = 0; i < paramValues.length; i++) {
				byte[] value = paramValues[i];
				int sqlType = java.sql.Types.VARCHAR;
				if (paramMeta != null) {
					try {
						sqlType = paramMeta.getParameterType(i + 1);
					} catch (SQLException ignore) {
						// keep the VARCHAR fallback
					}
				}
				if (value == null) {
					pstmt.setNull(i + 1, sqlType);
				} else {
					// Determine format: 0 = text, 1 = binary. The real
					// postgresql JDBC driver defaults to BINARY for numeric
					// setInt/setLong/etc. bind calls (confirmed live: a
					// bound int arrives here as a raw 4-byte big-endian
					// value, e.g. [0,0,0,42], not the digit string "42") —
					// text format is really only guaranteed for setString.
					short format = (paramFormats != null && paramFormats.length > 0)
						? (paramFormats.length == 1 ? paramFormats[0] : paramFormats[i])
						: 0;

					if (format == 0) {
						// Text format - convert bytes to string, then bind
						// using the target column's real type.
						String strValue = new String(value, java.nio.charset.StandardCharsets.UTF_8);
						bindTextValue(pstmt, i + 1, strValue, sqlType);
					} else {
						// Binary format - decode per the target column's
						// real type (was previously always setBytes(),
						// which is only correct for an actual BLOB column —
						// for anything else, e.g. INTEGER, that binds the
						// raw wire bytes as a byte[], which Avatica
						// represents as a ByteString and which then fails
						// to cast to the real target type at execution).
						bindBinaryValue(pstmt, i + 1, value, sqlType);
					}
				}
			}

			// No timing NoticeResponse here (unlike executeQuery's simple-
			// query path) -- this bound-PreparedStatement path is always the
			// extended protocol, i.e. always a program, never a person
			// reading a terminal. Sending it per row was pure waste on a
			// bulk insert: the client has to receive and discard a message
			// with zero value to it. See executeQuery's own doc.
			boolean hasResultSet = pstmt.execute();
			if (hasResultSet) {
				try (ResultSet rs = pstmt.getResultSet()) {
					sendResultSet(ctx, rs, includeRowDescription);
				}
			} else {
				int updateCount = pstmt.getUpdateCount();
				String upperSql = sql.toUpperCase().trim();
				if (upperSql.startsWith("INSERT")) {
					write(ctx, CommandComplete.insert(updateCount));
				} else if (upperSql.startsWith("UPDATE")) {
					write(ctx, CommandComplete.update(updateCount));
				} else if (upperSql.startsWith("DELETE")) {
					write(ctx, CommandComplete.delete(updateCount));
				} else {
					write(ctx, new CommandComplete("OK"));
				}
			}
		}
	}

	/**
	 * Binds a text-format wire value (already decoded to its string form) as
	 * the real Java type its target column expects, instead of always as a
	 * String — see the comment at this method's one call site for why that
	 * distinction matters. {@code sqlType} unrecognized here falls through to
	 * {@code setString}, which is correct for VARCHAR and a reasonable
	 * default for anything not explicitly handled.
	 */
	private void bindTextValue(PreparedStatement pstmt, int index, String strValue, int sqlType) throws SQLException {
		switch (sqlType) {
			case java.sql.Types.INTEGER, java.sql.Types.SMALLINT, java.sql.Types.TINYINT ->
				pstmt.setInt(index, Integer.parseInt(strValue));
			case java.sql.Types.BIGINT ->
				pstmt.setLong(index, Long.parseLong(strValue));
			case java.sql.Types.DOUBLE, java.sql.Types.FLOAT ->
				pstmt.setDouble(index, Double.parseDouble(strValue));
			case java.sql.Types.REAL ->
				pstmt.setFloat(index, Float.parseFloat(strValue));
			case java.sql.Types.DECIMAL, java.sql.Types.NUMERIC ->
				pstmt.setBigDecimal(index, new java.math.BigDecimal(strValue));
			case java.sql.Types.BOOLEAN, java.sql.Types.BIT ->
				pstmt.setBoolean(index, "t".equalsIgnoreCase(strValue) || "true".equalsIgnoreCase(strValue) || "1".equals(strValue));
			case java.sql.Types.TIMESTAMP ->
				pstmt.setTimestamp(index, parseTimestampText(strValue));
			default ->
				pstmt.setString(index, strValue);
		}
	}

	/**
	 * Binds a binary-format wire value (raw pg wire encoding, per
	 * https://www.postgresql.org/docs/current/protocol-message-formats.html)
	 * as the real Java type its target column expects — see the comment at
	 * this method's one call site for why {@code setBytes} unconditionally
	 * was wrong. Only the numeric/boolean encodings actually needed by this
	 * codebase's own tables are implemented; anything else (including
	 * NUMERIC/DECIMAL's variable-length base-10000 wire format, and genuine
	 * BLOB columns) falls through to the original setBytes behavior.
	 *
	 * <p>Integer/float widths are picked from the actual byte length
	 * received, not from {@code sqlType} — confirmed live: Convex's own
	 * "INTEGER" columns are backed by a 64-bit CVMLong, so
	 * getParameterMetaData() reports BIGINT for them, but a real postgres
	 * client's setInt() still wire-encodes as a plain 4-byte int4 regardless
	 * (the client has no way to know the server treats the column as
	 * 64-bit). Trusting sqlType's width there throws BufferUnderflowException
	 * reading 8 bytes out of a 4-byte buffer.
	 */
	private void bindBinaryValue(PreparedStatement pstmt, int index, byte[] value, int sqlType) throws SQLException {
		java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(value);
		switch (sqlType) {
			case java.sql.Types.SMALLINT, java.sql.Types.TINYINT, java.sql.Types.INTEGER, java.sql.Types.BIGINT -> {
				long longValue = switch (value.length) {
					case 2 -> buf.getShort();
					case 4 -> buf.getInt();
					case 8 -> buf.getLong();
					default -> throw new SQLException("Unexpected integer wire width: " + value.length + " bytes");
				};
				pstmt.setLong(index, longValue);
			}
			case java.sql.Types.REAL, java.sql.Types.DOUBLE, java.sql.Types.FLOAT -> {
				double doubleValue = switch (value.length) {
					case 4 -> buf.getFloat();
					case 8 -> buf.getDouble();
					default -> throw new SQLException("Unexpected float wire width: " + value.length + " bytes");
				};
				pstmt.setDouble(index, doubleValue);
			}
			case java.sql.Types.BOOLEAN, java.sql.Types.BIT ->
				pstmt.setBoolean(index, value.length > 0 && value[0] != 0);
			case java.sql.Types.VARCHAR, java.sql.Types.CHAR, java.sql.Types.LONGVARCHAR ->
				pstmt.setString(index, new String(value, java.nio.charset.StandardCharsets.UTF_8));
			case java.sql.Types.TIMESTAMP -> {
				if (value.length != 8) throw new SQLException("Unexpected timestamp wire width: " + value.length + " bytes");
				long microsSincePgEpoch = buf.getLong();
				pstmt.setTimestamp(index, new Timestamp(PG_BINARY_EPOCH_MILLIS + microsSincePgEpoch / 1000L));
			}
			default ->
				pstmt.setBytes(index, value);
		}
	}

	/**
	 * Adds pg_catalog. prefix to a table name if not already qualified.
	 * PostgreSQL's search path includes pg_catalog, but Calcite requires explicit schema.
	 */
	private String addPgCatalogPrefix(String sql, String tableName) {
		// Match table name that's not already prefixed with pg_catalog.
		// Use word boundaries and negative lookbehind for the dot
		String pattern = "(?i)(?<!pg_catalog\\.)\\b" + tableName + "\\b";
		return sql.replaceAll(pattern, "pg_catalog." + tableName);
	}

	/**
	 * Substitutes $1, $2, etc. with actual parameter values.
	 * Used for pg_catalog queries where PreparedStatement doesn't work well.
	 */
	private String substituteParameters(String sql, byte[][] paramValues, short[] paramFormats) {
		String result = sql;
		for (int i = 0; i < paramValues.length; i++) {
			String placeholder = "\\$" + (i + 1);
			String replacement;

			if (paramValues[i] == null) {
				replacement = "NULL";
			} else {
				short format = (paramFormats != null && paramFormats.length > 0)
					? (paramFormats.length == 1 ? paramFormats[0] : paramFormats[i])
					: 0;

				if (format == 0) {
					// Text format
					String strValue = new String(paramValues[i], java.nio.charset.StandardCharsets.UTF_8);
					// Check if it looks like a boolean or number
					if (strValue.equalsIgnoreCase("t") || strValue.equalsIgnoreCase("true")) {
						replacement = "TRUE";
					} else if (strValue.equalsIgnoreCase("f") || strValue.equalsIgnoreCase("false")) {
						replacement = "FALSE";
					} else if (strValue.matches("-?\\d+(\\.\\d+)?")) {
						replacement = strValue; // Number, no quotes
					} else {
						// String - escape single quotes and wrap
						replacement = "'" + strValue.replace("'", "''") + "'";
					}
				} else {
					// Binary format - represent as hex
					StringBuilder hex = new StringBuilder("'\\x");
					for (byte b : paramValues[i]) {
						hex.append(String.format("%02x", b & 0xff));
					}
					hex.append("'");
					replacement = hex.toString();
				}
			}

			result = result.replaceFirst(placeholder, replacement);
		}
		return result;
	}

	private void handleClose(ChannelHandlerContext ctx, PgMessageDecoder.Close close) {
		log.debug("Close: type={}, name='{}'", (char) close.type(), close.name());

		if (close.type() == 'S') {
			evictCachedStatement(statements.remove(close.name()));
		} else {
			portals.remove(close.name());
			describedPortals.remove(close.name());
		}

		write(ctx, CloseComplete.INSTANCE);
	}

	private void sendErrorAndClose(ChannelHandlerContext ctx, String code, String message) {
		write(ctx, ErrorResponse.builder()
			.severity("FATAL")
			.code(code)
			.message(message)
			.build());
		ctx.flush();
		ctx.close();
	}

	private void handleSync(ChannelHandlerContext ctx) {
		write(ctx, currentReadyState());
		ctx.flush();
	}

	private void handleTerminate(ChannelHandlerContext ctx) {
		log.debug("Client terminated connection");
		closeConnection();
		ctx.close();
	}

	private void write(ChannelHandlerContext ctx, PgMessage msg) {
		ByteBuf buf = ctx.alloc().buffer();
		msg.write(buf);
		ctx.write(buf);
	}

	private void closeConnection() {
		for (PreparedStatement cached : preparedStatementCache.values()) {
			try {
				cached.close();
			} catch (SQLException e) {
				log.debug("Error closing cached prepared statement", e);
			}
		}
		preparedStatementCache.clear();
		if (connection != null) {
			if (connection instanceof AvaticaConnection avaticaConn) {
				QueryLog.unmarkPgwireConnection(avaticaConn.id);
			}
			try {
				connection.close();
			} catch (SQLException e) {
				log.warn("Error closing connection", e);
			}
			connection = null;
		}
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) {
		closeConnection();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		log.error("Protocol error", cause);
		closeConnection();
		ctx.close();
	}

	/**
	 * EmptyQueryResponse - sent when an empty query string is received.
	 */
	private static class EmptyQueryResponse extends PgMessage {
		@Override
		public byte getType() {
			return EMPTY_QUERY;
		}

		@Override
		public void write(ByteBuf buf) {
			buf.writeByte(EMPTY_QUERY);
			buf.writeInt(4);
		}
	}
}
