package convex.db.calcite;

import java.util.Collections;
import java.util.List;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import convex.core.data.ABlob;
import convex.core.data.ACell;
import convex.core.data.AString;
import convex.core.data.Hash;
import convex.db.lattice.BlobCAS;

/**
 * Virtual read-only table exposing large values stored by {@link BlobCAS} —
 * queried as {@code SELECT ... FROM FILES WHERE hash = '<hex hash>'}. One
 * shared table per schema (registered unconditionally in
 * {@link ConvexSchema#getTableMap()}, not per base table), since the
 * underlying store is shared node-wide, not scoped to any one table.
 *
 * <p>Row shape: {@code HASH VARCHAR} (hex-encoded), {@code KIND VARCHAR}
 * ('BLOB'/'STRING'), {@code SIZE INTEGER}, {@code CONTENT VARCHAR} (populated
 * only when {@code KIND='STRING'}), {@code CONTENTBLOB VARBINARY} (populated
 * only when {@code KIND='BLOB'}).
 *
 * <p><b>Point lookup only — full enumeration is not supported.</b>
 * {@code AStore} is content-addressed for retrieval by a known hash; there is
 * no index of "every hash ever stored" (building one would itself need to be
 * a genuinely replicated structure, not a local-only side index — real,
 * distinct work, deliberately deferred). A query without a {@code hash =}
 * equality predicate returns an empty result rather than erring or attempting
 * a scan. Implemented via Calcite's {@link FilterableTable} rather than the
 * plain-{@code ScannableTable} full-scan pattern {@code ConvexHistoryTable}
 * uses, specifically because a full scan is exactly what's unavailable here —
 * the {@code hash =} predicate is handed directly to {@link #scan} instead of
 * requiring enumerate-then-filter.
 */
public class ConvexFilesTable extends AbstractTable implements FilterableTable {

	private static final int HASH_COLUMN_INDEX = 0;

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		RelDataTypeFactory.Builder builder = typeFactory.builder();
		builder.add("HASH", typeFactory.createSqlType(SqlTypeName.VARCHAR));
		builder.add("KIND", typeFactory.createSqlType(SqlTypeName.VARCHAR));
		builder.add("SIZE", typeFactory.createSqlType(SqlTypeName.INTEGER));
		builder.add("CONTENT", typeFactory.createTypeWithNullability(
			typeFactory.createSqlType(SqlTypeName.VARCHAR), true));
		builder.add("CONTENTBLOB", typeFactory.createTypeWithNullability(
			typeFactory.createSqlType(SqlTypeName.VARBINARY), true));
		return builder.build();
	}

	@Override
	public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
		String hashHex = extractHashEquality(filters);
		BlobCAS cas = BlobCAS.instance();
		if (hashHex == null || cas == null) {
			return Linq4j.asEnumerable(Collections.emptyList());
		}

		Hash hash;
		try {
			hash = Hash.fromHex(hashHex);
		} catch (Exception e) {
			return Linq4j.asEnumerable(Collections.emptyList());
		}

		ACell cell = cas.lookup(hash);
		Object[] row = (cell == null) ? null : toRow(hash, cell);
		List<Object[]> rows = (row == null) ? Collections.emptyList() : Collections.singletonList(row);
		return Linq4j.asEnumerable(rows);
	}

	private static Object[] toRow(Hash hash, ACell cell) {
		Object[] row = new Object[5];
		row[0] = hash.toHexString();
		if (cell instanceof AString str) {
			row[1] = "STRING";
			row[2] = (int) str.count();
			row[3] = str.toString();
			row[4] = null;
		} else if (cell instanceof ABlob blob) {
			row[1] = "BLOB";
			row[2] = (int) blob.count();
			row[3] = null;
			row[4] = blob.getBytes();
		} else {
			// BlobCAS only ever stores ABlob/AString cells -- anything else means
			// this hash wasn't one of ours (a hash collision with unrelated store
			// content, vanishingly unlikely) or the store holds something we don't
			// understand yet. Either way, not a row we can honestly return.
			return null;
		}
		return row;
	}

	/**
	 * Looks for a {@code HASH = '<literal>'} (or {@code '<literal>' = HASH})
	 * equality predicate among {@code filters}, removing it from the list per
	 * {@link FilterableTable}'s contract if found (this table fully implements
	 * that one filter; nothing left over for Calcite's own consuming operator
	 * to re-apply).
	 */
	private static String extractHashEquality(List<RexNode> filters) {
		for (int i = 0; i < filters.size(); i++) {
			RexNode f = filters.get(i);
			if (f instanceof RexCall call && call.getKind() == SqlKind.EQUALS) {
				List<RexNode> operands = call.getOperands();
				RexNode a = operands.get(0);
				RexNode b = operands.get(1);
				String hex = matchHashLiteral(a, b);
				if (hex == null) hex = matchHashLiteral(b, a);
				if (hex != null) {
					filters.remove(i);
					return hex;
				}
			}
		}
		return null;
	}

	private static String matchHashLiteral(RexNode columnSide, RexNode literalSide) {
		if (columnSide instanceof RexInputRef ref && ref.getIndex() == HASH_COLUMN_INDEX
				&& literalSide instanceof RexLiteral lit) {
			return lit.getValueAs(String.class);
		}
		return null;
	}
}
