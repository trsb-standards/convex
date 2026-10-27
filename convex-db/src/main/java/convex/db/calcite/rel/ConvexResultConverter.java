package convex.db.calcite.rel;

import java.util.Iterator;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.sql.type.SqlTypeName;

import convex.core.data.ABlob;
import convex.core.data.ACell;
import convex.core.data.prim.CVMBigInteger;
import convex.core.data.prim.CVMLong;
import convex.core.lang.RT;
import convex.db.calcite.convention.ConvexEnumerable;
import convex.db.calcite.convention.ConvexRel;

/**
 * Converts ConvexRel results (ACell[]) to Java types for JDBC consumption.
 *
 * <p>Called from Calcite-generated code at the ConvexConvention → Enumerable
 * boundary. The ConvexRel tree is passed via {@code implementor.stash()}.
 *
 * <p>Two methods match Calcite's row format contracts:
 * <ul>
 *   <li>{@link #execute} — ARRAY format, returns {@code Enumerable<Object[]>}</li>
 *   <li>{@link #executeScalar} — SCALAR format, returns {@code Enumerable<Object>}</li>
 * </ul>
 */
public class ConvexResultConverter {

	/**
	 * Executes in ARRAY format — each row is {@code Object[]}.
	 * Used for multi-column results.
	 *
	 * <p>Returns a lazy {@code Enumerable}: no rows are decoded until Calcite
	 * iterates via {@code rs.next()}, keeping the heap footprint near-zero
	 * at query-open time.
	 */
	public static Enumerable<Object[]> execute(ConvexRel rel, int fieldCount, DataContext ctx) {
		ConvexEnumerable convexResult = rel.execute(ctx);
		return Linq4j.asEnumerable(() -> {
			Iterator<ACell[]> it = convexResult.iterator();
			return new Iterator<Object[]>() {
				@Override public boolean hasNext() { return it.hasNext(); }
				@Override public Object[] next() {
					ACell[] row = it.next();
					Object[] javaRow = new Object[fieldCount];
					for (int i = 0; i < Math.min(row.length, fieldCount); i++) {
						javaRow[i] = cellToJava(row[i]);
					}
					return javaRow;
				}
			};
		});
	}

	/**
	 * Type-aware variant of {@link #execute}: converts each cell using the
	 * declared SQL type so that Calcite's generated casts always succeed.
	 *
	 * <p>For example, {@code MOD(BIGINT, INTEGER)} returns {@code INTEGER} in
	 * Calcite's type system, so the generated GROUP-BY key extractor emits
	 * {@code (Integer) row[i]}.  Without this overload the converter returns
	 * {@code Long} for every CVMLong, causing a ClassCastException at runtime.
	 *
	 * @param sqlTypeOrdinals  {@code SqlTypeName.ordinal()} for each projected
	 *                         field, in field order
	 */
	public static Enumerable<Object[]> execute(ConvexRel rel, int fieldCount,
			int[] sqlTypeOrdinals, DataContext ctx) {
		ConvexEnumerable convexResult = rel.execute(ctx);
		return Linq4j.asEnumerable(() -> {
			Iterator<ACell[]> it = convexResult.iterator();
			return new Iterator<Object[]>() {
				@Override public boolean hasNext() { return it.hasNext(); }
				@Override public Object[] next() {
					ACell[] row = it.next();
					Object[] javaRow = new Object[fieldCount];
					for (int i = 0; i < Math.min(row.length, fieldCount); i++) {
						javaRow[i] = cellToJavaTyped(row[i], sqlTypeOrdinals[i]);
					}
					return javaRow;
				}
			};
		});
	}

	/**
	 * Executes in SCALAR format — each element is the column value directly.
	 * Used for single-column results.
	 *
	 * <p>Returns a lazy {@code Enumerable}: no rows are decoded until iteration.
	 */
	public static Enumerable<Object> executeScalar(ConvexRel rel, DataContext ctx) {
		return executeScalar(rel, -1, ctx);
	}

	/**
	 * {@link #executeScalar(ConvexRel, DataContext)} with the column's SQL
	 * type, so the value comes back as the Java type Calcite's generated code
	 * expects -- an {@code Integer} for an INTEGER column, not the {@code Long}
	 * every whole number is stored as. Found 2026-10-26: a semi-join whose
	 * only output was the literal {@code 1} failed with "Long cannot be cast
	 * to Integer"; the multi-column path already passed the types.
	 *
	 * @param sqlTypeOrdinal the column's {@code SqlTypeName} ordinal, or -1 for untyped
	 */
	public static Enumerable<Object> executeScalar(ConvexRel rel, int sqlTypeOrdinal, DataContext ctx) {
		ConvexEnumerable convexResult = rel.execute(ctx);
		return Linq4j.asEnumerable(() -> {
			Iterator<ACell[]> it = convexResult.iterator();
			return new Iterator<Object>() {
				@Override public boolean hasNext() { return it.hasNext(); }
				@Override public Object next() {
					ACell[] row = it.next();
					if (row.length == 0) return null;
					return (sqlTypeOrdinal < 0) ? cellToJava(row[0]) : cellToJavaTyped(row[0], sqlTypeOrdinal);
				}
			};
		});
	}

	/**
	 * Converts a CVM cell to a Java object for JDBC consumption.
	 *
	 * <p>{@link CVMBigInteger} is special-cased to {@code Long} (via {@link
	 * CVMBigInteger#longValue()}, narrowing/wrapping on overflow same as any
	 * Java numeric narrowing conversion) rather than the {@code BigInteger}
	 * {@link RT#jvm} would otherwise return — {@link
	 * convex.db.calcite.ConvexType#BIGINT} is this driver's only mapping
	 * that ever produces a {@code CVMBigInteger} cell, and Calcite's own
	 * generated code for a {@code BIGINT}-typed field always expects a
	 * boxed {@code Long}, never {@code BigInteger} (that's Calcite's
	 * internal representation for arbitrary-precision {@code DECIMAL}, a
	 * different SQL type). Found live 2026-09: {@code SUM(BIGINT)} over
	 * enough rows to overflow a {@code long} correctly widens internally to
	 * {@code CVMBigInteger} — a genuine, expected value — but ANSI SQL still
	 * declares {@code SUM(BIGINT)}'s own result type as plain {@code
	 * BIGINT}, so Calcite's generated cast (matching that declared type,
	 * same reasoning {@link #cellToJavaTyped} already documents for {@code
	 * CVMLong}) threw {@code ClassCastException: BigInteger cannot be cast
	 * to Long} before this fix — every {@code BigInteger} value this driver
	 * could ever surface here overflowed for exactly this reason, never an
	 * intentional arbitrary-precision result the caller wanted preserved.
	 */
	public static Object cellToJava(ACell cell) {
		if (cell == null) return null;
		if (cell instanceof CVMBigInteger bi) return bi.longValue();
		if (cell instanceof ABlob blob) return blob.getBytes();
		return RT.jvm(cell);
	}

	/**
	 * Type-aware cell conversion: returns the Java type that matches the
	 * declared SQL type so Calcite's generated casts never fail.
	 */
	public static Object cellToJavaTyped(ACell cell, int sqlTypeOrdinal) {
		if (cell == null) return null;
		if (cell instanceof CVMLong l) {
			SqlTypeName t = SqlTypeName.values()[sqlTypeOrdinal];
			return switch (t) {
				case INTEGER, SMALLINT, TINYINT -> (int) l.longValue();
				default -> l.longValue();
			};
		}
		// See cellToJava's own doc for why CVMBigInteger always narrows to
		// Long here regardless of sqlTypeOrdinal -- BIGINT is this driver's
		// only source of a CVMBigInteger cell, and Calcite represents that
		// SQL type as Long throughout its generated code, never BigInteger.
		if (cell instanceof CVMBigInteger bi) return bi.longValue();
		if (cell instanceof ABlob blob) return blob.getBytes();
		return RT.jvm(cell);
	}
}
