package convex.db.calcite.rel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.DataContext;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import convex.core.data.ACell;
import convex.core.data.AString;
import convex.core.data.AVector;
import convex.core.data.prim.CVMBool;
import convex.db.calcite.ConvexSchema;
import convex.db.calcite.ConvexTable;
import convex.db.calcite.ConvexType;
import convex.db.calcite.convention.ConvexConvention;
import convex.db.calcite.convention.ConvexEnumerable;
import convex.db.calcite.convention.ConvexRel;
import convex.db.calcite.eval.ConvexExpressionEvaluator;
import convex.db.lattice.SQLTable;

/**
 * Filter in CONVEX convention.
 *
 * <p>Filters ACell[] rows using CVM-native comparisons. Two pushdowns avoid a
 * full table scan when the filter is a single-column equality:
 * <ul>
 *   <li>{@code WHERE pkCol = literal/?param}, where {@code pkCol} is the table's
 *       full (single-column) primary key — pushed to {@code selectByKey()},
 *       O(log n), at most one matching row.</li>
 *   <li>{@code WHERE col = literal/?param}, where {@code col} has a secondary
 *       index created via {@code CREATE INDEX} — pushed to {@code selectByIndex()},
 *       O(log n + k) where k = matching rows.</li>
 * </ul>
 * Falls back to a full scan + in-memory filter otherwise.
 *
 * <p>Dynamic parameters from PreparedStatements are resolved via the
 * DataContext passed through the ConvexRel execution pipeline.
 */
public class ConvexFilter extends Filter implements ConvexRel {

	public ConvexFilter(RelOptCluster cluster, RelTraitSet traitSet,
			RelNode input, RexNode condition) {
		super(cluster, traitSet, input, condition);
		assert getConvention() == ConvexConvention.INSTANCE;
	}

	@Override
	public ConvexFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
		return new ConvexFilter(getCluster(), traitSet, input, condition);
	}

	@Override
	public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
		if (canPushdownPrimaryKey()) {
			// PK pushdown: O(log n) index lookup, returns at most 1 row
			return planner.getCostFactory().makeCost(1, 1, 0);
		}
		if (canPushdownSecondaryIndex()) {
			// Secondary-index pushdown: O(log n + k); cheap regardless of table size
			return planner.getCostFactory().makeCost(1, 1, 0);
		}
		double rowCount = mq.getRowCount(this);
		return planner.getCostFactory().makeCost(rowCount, rowCount, 0);
	}

	/**
	 * Returns true if this filter can be resolved via primary key index lookup.
	 */
	private boolean canPushdownPrimaryKey() {
		if (!(getInput() instanceof ConvexTableScan scan)) return false;
		if (!hasPrimaryKeyEquality(condition)) return false;
		return isSingleColumnPk(scan);
	}

	/**
	 * Returns true if this filter is a single-column equality on a column that
	 * has a secondary index (created via {@code CREATE INDEX}).
	 *
	 * <p>Structural check only (mirrors {@link #hasPrimaryKeyEquality}) — doesn't
	 * need to resolve the actual value, so it works during cost estimation
	 * (before a {@link DataContext} exists to resolve dynamic parameters).
	 */
	private boolean canPushdownSecondaryIndex() {
		if (!(getInput() instanceof ConvexTableScan scan)) return false;
		int colIndex = equalityColumnIndex(condition);
		if (colIndex < 0) return false;
		return hasIndexOnColumn(scan, colIndex);
	}

	/** Returns true if the scanned table has a secondary index on the column at {@code colIndex}. */
	private static boolean hasIndexOnColumn(ConvexTableScan scan, int colIndex) {
		SQLTable table = getTable(scan);
		if (table == null) return false;
		AString colName = columnNameAt(table, colIndex);
		return colName != null && table.hasColumnIndex(colName);
	}

	/** Resolves the {@link SQLTable} backing a table scan, or {@code null} if unavailable. */
	private static SQLTable getTable(ConvexTableScan scan) {
		ConvexTable convexTable = scan.getTable().unwrap(ConvexTable.class);
		if (convexTable == null) return null;
		ConvexSchema schema = convexTable.getSchema();
		return schema.getTables().getTable(convexTable.getTableName());
	}

	/** Returns the column name at {@code colIndex} in the table's schema, or {@code null} if out of range. */
	private static AString columnNameAt(SQLTable table, int colIndex) {
		AVector<AVector<ACell>> tableSchema = table.getSchema();
		if (tableSchema == null || colIndex < 0 || colIndex >= tableSchema.count()) return null;
		return (AString) tableSchema.get(colIndex).get(0);
	}

	/**
	 * Returns true if the scanned table's primary key is a single column.
	 *
	 * <p>For a composite PK, an equality on only column[0] can match zero, one,
	 * or many rows — not a unique row — so the {@code selectByKey()} point-lookup
	 * pushdown must not be used; the caller should fall back to a full scan.
	 */
	private static boolean isSingleColumnPk(ConvexTableScan scan) {
		ConvexTable convexTable = scan.getTable().unwrap(ConvexTable.class);
		if (convexTable == null) return false;
		ConvexSchema schema = convexTable.getSchema();
		SQLTable table = schema.getTables().getTable(convexTable.getTableName());
		return table != null && table.getPkCount() == 1;
	}

	/**
	 * Checks if a condition contains a primary key equality (column[0] = value/param).
	 */
	private static boolean hasPrimaryKeyEquality(RexNode condition) {
		if (!(condition instanceof RexCall call)) return false;
		if (call.getKind() != SqlKind.EQUALS) return false;
		var operands = call.getOperands();
		if (operands.size() != 2) return false;
		if (operands.get(0) instanceof RexInputRef ref && ref.getIndex() == 0) {
			return operands.get(1) instanceof RexLiteral || operands.get(1) instanceof RexDynamicParam;
		}
		if (operands.get(1) instanceof RexInputRef ref && ref.getIndex() == 0) {
			return operands.get(0) instanceof RexLiteral || operands.get(0) instanceof RexDynamicParam;
		}
		return false;
	}

	/**
	 * Returns the column index of a {@code column[i] = literal/?param} equality
	 * (either operand order), for any column {@code i} — structural check only,
	 * doesn't resolve the value. Returns -1 if the condition isn't that shape.
	 */
	private static int equalityColumnIndex(RexNode condition) {
		if (!(condition instanceof RexCall call)) return -1;
		if (call.getKind() != SqlKind.EQUALS) return -1;
		var operands = call.getOperands();
		if (operands.size() != 2) return -1;
		if (operands.get(0) instanceof RexInputRef ref
				&& (operands.get(1) instanceof RexLiteral || operands.get(1) instanceof RexDynamicParam)) {
			return ref.getIndex();
		}
		if (operands.get(1) instanceof RexInputRef ref
				&& (operands.get(0) instanceof RexLiteral || operands.get(0) instanceof RexDynamicParam)) {
			return ref.getIndex();
		}
		return -1;
	}

	@Override
	public ConvexEnumerable execute(DataContext ctx) {
		// Try primary key pushdown: if filtering on column[0] = literal/param
		// over a table scan, use selectByKey() instead of full scan
		ConvexEnumerable pushed = tryPrimaryKeyLookup(ctx);
		if (pushed != null) return pushed;

		// Try secondary-index pushdown: filtering on any indexed column = literal/param
		pushed = trySecondaryIndexLookup(ctx);
		if (pushed != null) return pushed;

		// Fallback: full scan + filter
		ConvexRel inputRel = (ConvexRel) getInput();
		ConvexEnumerable input = inputRel.execute(ctx);

		List<ACell[]> result = new ArrayList<>();
		for (ACell[] row : input) {
			ACell evaluated = ConvexExpressionEvaluator.evaluate(condition, row, getRowType(), ctx);
			if (evaluated instanceof CVMBool b && b.booleanValue()) {
				result.add(row);
			}
		}

		return ConvexEnumerable.of(result);
	}

	/**
	 * Attempts primary key pushdown. If the condition is an equality check
	 * on column 0 (the primary key) with a literal or bound parameter value,
	 * and the input is a table scan, uses selectByKey() for O(log n) lookup.
	 *
	 * @param ctx DataContext for resolving dynamic parameters (may be null)
	 * @return ConvexEnumerable with the matching row, or null if pushdown not applicable
	 */
	private ConvexEnumerable tryPrimaryKeyLookup(DataContext ctx) {
		if (!(getInput() instanceof ConvexTableScan scan)) return null;
		if (!isSingleColumnPk(scan)) return null; // composite PK: column[0]=? is not a unique-row lookup

		ACell pkValue = extractPrimaryKeyEquality(condition, ctx);
		if (pkValue == null) return null;

		ConvexTable convexTable = scan.getTable().unwrap(ConvexTable.class);
		if (convexTable == null) return null;

		ConvexSchema schema = convexTable.getSchema();
		AVector<ACell> row = schema.getTables().selectByKey(convexTable.getTableName(), pkValue);
		if (row == null) return ConvexEnumerable.empty();

		return ConvexEnumerable.of(Collections.singletonList(row.toCellArray()));
	}

	/**
	 * Attempts secondary-index pushdown. If the condition is an equality check
	 * on a column with a secondary index (created via {@code CREATE INDEX}),
	 * uses {@code SQLTable.selectByIndex()} — O(log n + k) — instead of a full
	 * scan. Unlike primary-key lookup, this can return any number of rows
	 * (the whole point of an index on a non-unique column).
	 *
	 * @param ctx DataContext for resolving dynamic parameters (may be null)
	 * @return ConvexEnumerable with the matching rows, or null if pushdown not applicable
	 */
	private ConvexEnumerable trySecondaryIndexLookup(DataContext ctx) {
		if (!(getInput() instanceof ConvexTableScan scan)) return null;

		int colIndex = equalityColumnIndex(condition);
		if (colIndex < 0) return null;

		SQLTable table = getTable(scan);
		if (table == null) return null;
		AString colName = columnNameAt(table, colIndex);
		if (colName == null || !table.hasColumnIndex(colName)) return null;

		ACell value = extractEqualityValue(condition, colIndex, ctx);
		if (value == null) return null;

		List<AVector<ACell>> rows = table.selectByIndex(colName, value);
		if (rows == null) return null;

		List<ACell[]> cellRows = new ArrayList<>(rows.size());
		for (AVector<ACell> row : rows) cellRows.add(row.toCellArray());
		return ConvexEnumerable.of(cellRows);
	}

	/**
	 * Resolves the literal/param value being compared against {@code column[colIndex]}
	 * in a {@code column[colIndex] = value} equality (either operand order).
	 */
	private static ACell extractEqualityValue(RexNode condition, int colIndex, DataContext ctx) {
		if (!(condition instanceof RexCall call)) return null;
		List<RexNode> operands = call.getOperands();
		if (operands.size() != 2) return null;
		if (operands.get(0) instanceof RexInputRef ref && ref.getIndex() == colIndex) {
			return resolveValue(operands.get(1), ctx);
		}
		if (operands.get(1) instanceof RexInputRef ref && ref.getIndex() == colIndex) {
			return resolveValue(operands.get(0), ctx);
		}
		return null;
	}

	/**
	 * Extracts the primary key value from a condition of the form
	 * {@code column[0] = literal} or {@code column[0] = ?param}.
	 *
	 * @param condition Filter condition
	 * @param ctx DataContext for resolving dynamic parameters
	 * @return The value as ACell, or null if the condition doesn't match
	 */
	static ACell extractPrimaryKeyEquality(RexNode condition, DataContext ctx) {
		if (!(condition instanceof RexCall call)) return null;
		if (call.getKind() != SqlKind.EQUALS) return null;

		List<RexNode> operands = call.getOperands();
		if (operands.size() != 2) return null;

		// Try column[0] = value
		if (operands.get(0) instanceof RexInputRef ref && ref.getIndex() == 0) {
			ACell val = resolveValue(operands.get(1), ctx);
			if (val != null) return val;
		}
		// Try value = column[0]
		if (operands.get(1) instanceof RexInputRef ref && ref.getIndex() == 0) {
			ACell val = resolveValue(operands.get(0), ctx);
			if (val != null) return val;
		}

		return null;
	}

	/**
	 * Resolves a RexNode to an ACell value. Handles literals and dynamic params.
	 */
	private static ACell resolveValue(RexNode node, DataContext ctx) {
		if (node instanceof RexLiteral lit) {
			return ConvexExpressionEvaluator.literalToCell(lit);
		}
		if (node instanceof RexDynamicParam param && ctx != null) {
			Object val = ctx.get("?" + param.getIndex());
			if (val != null) {
				return ConvexType.ANY.toCell(val);
			}
		}
		return null;
	}
}
