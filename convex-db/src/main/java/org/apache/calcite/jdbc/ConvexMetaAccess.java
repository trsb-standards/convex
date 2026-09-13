package org.apache.calcite.jdbc;

import java.sql.Connection;

/**
 * Package-visibility bridge exposing just enough of {@code
 * CalciteConnectionImpl}'s package-private surface ({@code meta()}) for
 * {@code convex.db.psql.PgProtocolHandler} (a different package) to reach
 * the live {@link ConvexMeta} backing a connection — same "lives in {@code
 * org.apache.calcite.jdbc} specifically to reach package-private internals"
 * reasoning {@link ConvexMeta} itself already documents on its own class.
 *
 * <p>Used by {@code PgProtocolHandler#handleSync} to flush a deferred batch
 * sync — see {@code convex.db.calcite.BatchSyncDeferral}'s own doc.
 */
public final class ConvexMetaAccess {

	private ConvexMetaAccess() {}

	/**
	 * Returns the live {@link ConvexMeta} backing {@code conn}, or null if it
	 * isn't a Convex-backed Calcite connection at all (defensive — every
	 * connection this driver actually hands out is one, but callers outside
	 * this module shouldn't have to assume that).
	 */
	public static ConvexMeta metaOf(Connection conn) {
		if (conn instanceof CalciteConnectionImpl cci && cci.meta() instanceof ConvexMeta cm) {
			return cm;
		}
		return null;
	}
}
