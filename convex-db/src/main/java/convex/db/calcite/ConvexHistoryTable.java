package convex.db.calcite;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import convex.core.data.ABlob;
import convex.core.data.ACell;
import convex.core.data.AVector;
import convex.core.data.Strings;
import convex.core.data.prim.CVMLong;
import convex.db.calcite.rel.ConvexResultConverter;
import convex.db.lattice.HistoryKey;
import convex.db.lattice.SQLTable;
import convex.db.lattice.VersionedSQLTable;

/**
 * Virtual read-only table exposing the FULL row history (every insert,
 * update, and deletion) of a versioned base table — queried as
 * {@code SELECT * FROM <table>_HISTORY}. Only registered for versioned
 * tables (see {@link ConvexSchema#getTableMap()}) — a plain table has no
 * history to show.
 *
 * <p>Row shape: every column of the base table (nullable — a DELETE entry
 * has no live values), plus {@code WRITESEQ} (BIGINT, the ordering value —
 * see {@link VersionedSQLTable#nextHistorySeq()}; an HLC-style value, always
 * {@code >= System.currentTimeMillis()} at write time, so directly usable as
 * a raw millis timestamp) and {@code CHANGETYPE} (VARCHAR: INSERT/UPDATE/
 * DELETE). {@code CHANGEDAT} (TIMESTAMP, derived straight from {@code
 * WRITESEQ}) is provided as a convenience for date/time functions — e.g.
 * {@code TIMESTAMPDIFF(SECOND, CHANGEDAT, CURRENT_TIMESTAMP)}. For DELETE
 * entries the row's other column values were never recorded (see {@link
 * VersionedSQLTable}'s own class doc) — only the primary-key column (index
 * 0) is populated, by reconstructing it from the history key itself.
 *
 * <p>Implemented as a plain {@link ScannableTable} (full scan, same
 * pattern as the {@code pg_catalog} virtual tables) rather than routed
 * through {@code ConvexConvention}'s custom operators — history queries
 * aren't a hot path, and Calcite's default enumerable pipeline already
 * handles WHERE/ORDER BY generically on top of a full scan.
 */
public class ConvexHistoryTable extends AbstractTable implements ScannableTable {

	/** Suffix recognised by {@link ConvexSchema#getTableMap()} to route to this table. */
	public static final String SUFFIX = "_HISTORY";

	private final ConvexSchema schema;
	private final String baseTableName;

	public ConvexHistoryTable(ConvexSchema schema, String baseTableName) {
		this.schema = schema;
		this.baseTableName = baseTableName;
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		RelDataTypeFactory.Builder builder = typeFactory.builder();
		String[] columnNames = schema.getTables().getColumnNames(baseTableName);
		ConvexColumnType[] columnTypes = schema.getTables().getColumnTypes(baseTableName);
		if (columnNames != null && columnTypes != null) {
			for (int i = 0; i < columnNames.length; i++) {
				RelDataType type = columnTypes[i].toRelDataType(typeFactory);
				builder.add(columnNames[i], typeFactory.createTypeWithNullability(type, true));
			}
		}
		builder.add("WRITESEQ", typeFactory.createSqlType(SqlTypeName.BIGINT));
		builder.add("CHANGETYPE", typeFactory.createSqlType(SqlTypeName.VARCHAR));
		builder.add("CHANGEDAT", typeFactory.createTypeWithNullability(
			typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true));
		return builder.build();
	}

	@Override
	public Enumerable<Object[]> scan(DataContext root) {
		List<Object[]> rows = new ArrayList<>();
		SQLTable table = schema.getTables().getTable(baseTableName);
		ConvexColumnType[] columnTypes = schema.getTables().getColumnTypes(baseTableName);
		if (table instanceof VersionedSQLTable vt && columnTypes != null) {
			int[] sqlTypeOrdinals = new int[columnTypes.length];
			for (int i = 0; i < columnTypes.length; i++) {
				sqlTypeOrdinals[i] = columnTypes[i].getSqlTypeName().ordinal();
			}
			for (Map.Entry<ABlob, AVector<ACell>> e : vt.getHistoryIndex().entrySet()) {
				rows.add(toRow(e.getKey(), e.getValue(), columnTypes, sqlTypeOrdinals));
			}
		}
		return Linq4j.asEnumerable(rows);
	}

	private Object[] toRow(ABlob historyKey, AVector<ACell> entry, ConvexColumnType[] columnTypes, int[] sqlTypeOrdinals) {
		int columnCount = columnTypes.length;
		Object[] row = new Object[columnCount + 3];
		AVector<ACell> values = VersionedSQLTable.getHistoryValues(entry);
		if (values != null) {
			for (int i = 0; i < columnCount; i++) {
				row[i] = ConvexResultConverter.cellToJavaTyped(values.get(i), sqlTypeOrdinals[i]);
			}
		} else if (columnCount > 0) {
			// DELETE entry: no row values were recorded, only reconstruct the pk (column 0).
			ACell pkCell = decodePk(HistoryKey.extractPk(historyKey), columnTypes[0]);
			row[0] = ConvexResultConverter.cellToJavaTyped(pkCell, sqlTypeOrdinals[0]);
		}
		long writeSeq = VersionedSQLTable.getHistoryWriteSeq(entry);
		long changeType = ((CVMLong) entry.get(2)).longValue();
		row[columnCount] = writeSeq;
		row[columnCount + 1] = changeTypeName(changeType);
		// Calcite's Enumerable pipeline represents TIMESTAMP as a plain
		// epoch-millis Long internally (not java.sql.Timestamp) -- Avatica
		// converts it at the JDBC ResultSet boundary based on the declared
		// SQL type from getRowType. writeSeq IS already an epoch-millis-scale
		// value (see VersionedSQLTable.nextHistorySeq's own doc), so this is
		// a direct reinterpretation, not a separately-stored field.
		row[columnCount + 2] = writeSeq;
		return row;
	}

	/**
	 * Reverses {@code SQLSchema.toKey}'s encoding for the two pk shapes that
	 * scheme actually supports — an 8-byte big-endian {@code CVMLong}, or
	 * UTF-8 bytes for an {@code AString} — chosen by the pk column's own
	 * declared type rather than guessed from byte length (an 8-byte string
	 * would otherwise be indistinguishable from an encoded long). Any other
	 * raw blob (composite/opaque keys) can't occur here — versioned tables
	 * reject {@code pkCount != 1} at creation time.
	 */
	private ACell decodePk(ABlob pkBlob, ConvexColumnType pkType) {
		if (pkType.getBaseType().getCvmType() == CVMLong.class) {
			byte[] b = pkBlob.getBytes();
			long v = 0;
			for (byte value : b) v = (v << 8) | (value & 0xFF);
			return CVMLong.create(v);
		}
		return Strings.create(new String(pkBlob.getBytes(), StandardCharsets.UTF_8));
	}

	private static String changeTypeName(long ct) {
		if (ct == VersionedSQLTable.CT_INSERT) return "INSERT";
		if (ct == VersionedSQLTable.CT_UPDATE) return "UPDATE";
		if (ct == VersionedSQLTable.CT_DELETE) return "DELETE";
		return "?";
	}
}
