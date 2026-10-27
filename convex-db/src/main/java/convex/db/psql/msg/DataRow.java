package convex.db.psql.msg;

import convex.db.psql.PgMessage;
import convex.db.psql.PgType;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * DataRow message - contains the values of a single row.
 */
public class DataRow extends PgMessage {

	private final byte[][] values;

	public DataRow(byte[][] values) {
		this.values = values;
	}

	/**
	 * Creates a DataRow from a JDBC ResultSet (current row), always in text
	 * format. Kept for the simple-query path (no {@code Bind}, so no
	 * per-column format request exists at all).
	 */
	public static DataRow fromResultSet(ResultSet rs, int columnCount) throws SQLException {
		return fromResultSet(rs, columnCount, null, null);
	}

	/**
	 * Creates a DataRow from a JDBC ResultSet (current row), honoring the
	 * format each column was actually bound for (per {@code Bind}'s own
	 * {@code resultFormatCodes} — see {@link #isBinaryRequested}).
	 *
	 * <p>Found live 2026-08-25: this server always produced text-format
	 * values regardless of what the client asked for. That went unnoticed
	 * because nothing previously exercised a bound, extended-protocol SELECT
	 * whose {@code PreparedStatement} lived long enough for pgjdbc to
	 * "graduate" it to binary transfer (a real pgjdbc optimization, applied
	 * once a statement has been reused past its own {@code prepareThreshold})
	 * — the pg-wire spec makes the format the *client* requested in {@code
	 * Bind} authoritative for how it will parse {@code DataRow}, regardless
	 * of what {@code RowDescription} announced; sending text when binary was
	 * requested corrupts every subsequent column read for that row (surfaces
	 * client-side as things like {@code ArrayIndexOutOfBoundsException} deep
	 * in pgjdbc's own binary decoders, since a short text digit string is
	 * nowhere near wide enough to satisfy a fixed-width binary read).
	 *
	 * @param meta column metadata (needed to resolve each column's type OID
	 *        for binary encoding); may be null only if {@code resultFormats}
	 *        is also null/empty (an all-text row never needs it)
	 * @param resultFormats Bind's own per-column format request; null/empty
	 *        means every column is text (the pre-existing default)
	 */
	public static DataRow fromResultSet(ResultSet rs, int columnCount,
			ResultSetMetaData meta, short[] resultFormats) throws SQLException {
		byte[][] values = new byte[columnCount][];
		for (int i = 0; i < columnCount; i++) {
			int col = i + 1;
			Object obj = rs.getObject(col);
			if (obj == null || rs.wasNull()) {
				values[i] = null;
			} else if (isBinaryRequested(resultFormats, i)) {
				int typeOid = PgType.fromSqlType(meta.getColumnTypeName(col));
				values[i] = formatBinaryValue(obj, typeOid);
			} else {
				values[i] = formatValue(obj);
			}
		}
		return new DataRow(values);
	}

	/**
	 * Resolves whether column {@code columnIndex} (0-based) should be sent
	 * in binary, per pg-wire's own {@code resultFormatCodes} shape: empty
	 * means every column is text; length 1 means that one format applies to
	 * every column; otherwise one entry per column.
	 */
	private static boolean isBinaryRequested(short[] resultFormats, int columnIndex) {
		if (resultFormats == null || resultFormats.length == 0) return false;
		if (resultFormats.length == 1) return resultFormats[0] == 1;
		return columnIndex < resultFormats.length && resultFormats[columnIndex] == 1;
	}

	/**
	 * Encodes a result value in PostgreSQL's binary wire format for the
	 * types this server can meaningfully distinguish from text (mirrors
	 * {@code PgProtocolHandler.bindBinaryValue}'s inbound decoding, in
	 * reverse). String types need no special handling here — their binary
	 * form is identical to text (raw UTF-8 bytes), so they're covered by the
	 * {@code default} fallback along with any other type this server
	 * doesn't have a distinct binary form for.
	 */
	private static byte[] formatBinaryValue(Object obj, int typeOid) {
		switch (typeOid) {
			case PgType.INT2 -> {
				return ByteBuffer.allocate(2).putShort(((Number) obj).shortValue()).array();
			}
			case PgType.INT4, PgType.OID -> {
				return ByteBuffer.allocate(4).putInt(((Number) obj).intValue()).array();
			}
			case PgType.INT8 -> {
				return ByteBuffer.allocate(8).putLong(((Number) obj).longValue()).array();
			}
			case PgType.FLOAT4 -> {
				return ByteBuffer.allocate(4).putFloat(((Number) obj).floatValue()).array();
			}
			case PgType.FLOAT8 -> {
				return ByteBuffer.allocate(8).putDouble(((Number) obj).doubleValue()).array();
			}
			case PgType.BOOL -> {
				boolean b = (obj instanceof Boolean bool) ? bool : ((Number) obj).intValue() != 0;
				return new byte[]{(byte) (b ? 1 : 0)};
			}
			case PgType.TIMESTAMP, PgType.TIMESTAMPTZ -> {
				long millis = (obj instanceof Timestamp ts) ? ts.getTime() : ((Number) obj).longValue();
				long microsSincePgEpoch = (millis - PgType.BINARY_EPOCH_MILLIS) * 1000L;
				return ByteBuffer.allocate(8).putLong(microsSincePgEpoch).array();
			}
			default -> {
				return formatValue(obj);
			}
		}
	}

	/**
	 * Formats a value as text for the PostgreSQL protocol.
	 */
	private static byte[] formatValue(Object obj) {
		if (obj == null) {
			return null;
		}

		String text;
		if (obj instanceof Boolean b) {
			text = b ? "t" : "f";
		} else if (obj instanceof byte[] bytes) {
			// Format as hex with \x prefix
			StringBuilder sb = new StringBuilder("\\x");
			for (byte b : bytes) {
				sb.append(String.format("%02x", b & 0xff));
			}
			text = sb.toString();
		} else {
			text = obj.toString();
		}

		return text.getBytes(StandardCharsets.UTF_8);
	}

	@Override
	public byte getType() {
		return DATA_ROW;
	}

	@Override
	public void write(ByteBuf buf) {
		// Calculate length
		int length = 4 + 2; // length field + column count
		for (byte[] value : values) {
			length += 4; // length of value (or -1 for null)
			if (value != null) {
				length += value.length;
			}
		}

		buf.writeByte(DATA_ROW);
		buf.writeInt(length);
		buf.writeShort(values.length);

		for (byte[] value : values) {
			if (value == null) {
				buf.writeInt(-1); // NULL
			} else {
				buf.writeInt(value.length);
				buf.writeBytes(value);
			}
		}
	}
}
