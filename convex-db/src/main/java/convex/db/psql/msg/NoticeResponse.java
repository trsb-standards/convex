package convex.db.psql.msg;

import convex.db.psql.PgMessage;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * NoticeResponse message - informational message to the client that doesn't
 * abort the current command (unlike {@link ErrorResponse}). Wire format is
 * identical to ErrorResponse (same field-code/value structure), just tagged
 * with the NOTICE_RESPONSE type byte instead of ERROR_RESPONSE.
 *
 * <p>Used for query-timing feedback (see {@link #timing(String)}) — a
 * server-measured "N rows in set (X.XXX sec)"-style line, printed by psql
 * (and most other clients) as a plain notice alongside the result.
 */
public class NoticeResponse extends PgMessage {

	private final Map<Byte, String> fields;

	private NoticeResponse(Map<Byte, String> fields) {
		this.fields = fields;
	}

	@Override
	public byte getType() {
		return NOTICE_RESPONSE;
	}

	@Override
	public void write(ByteBuf buf) {
		int length = 4 + 1; // length field + terminator
		for (Map.Entry<Byte, String> entry : fields.entrySet()) {
			length += 1 + entry.getValue().getBytes(StandardCharsets.UTF_8).length + 1;
		}

		buf.writeByte(NOTICE_RESPONSE);
		buf.writeInt(length);

		for (Map.Entry<Byte, String> entry : fields.entrySet()) {
			buf.writeByte(entry.getKey());
			writeCString(buf, entry.getValue());
		}

		buf.writeByte(0); // terminator
	}

	public static class Builder {
		private final Map<Byte, String> fields = new LinkedHashMap<>();

		public Builder severity(String severity) {
			fields.put(ErrorResponse.SEVERITY, severity);
			fields.put(ErrorResponse.SEVERITY_NON_LOCALIZED, severity);
			return this;
		}

		public Builder code(String code) {
			fields.put(ErrorResponse.CODE, code);
			return this;
		}

		public Builder message(String message) {
			fields.put(ErrorResponse.MESSAGE, message);
			return this;
		}

		public NoticeResponse build() {
			return new NoticeResponse(fields);
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * A query-timing notice, e.g. "22 rows in set (0.008 sec)" — server-side
	 * measured, so it reflects actual execution time regardless of network
	 * round-trip. SQLSTATE "00000" (successful completion) since this always
	 * accompanies a successful command, never an error.
	 */
	public static NoticeResponse timing(String message) {
		return builder()
			.severity("NOTICE")
			.code("00000")
			.message(message)
			.build();
	}
}
