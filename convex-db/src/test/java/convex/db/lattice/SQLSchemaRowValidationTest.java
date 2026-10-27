package convex.db.lattice;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import convex.core.data.Strings;
import convex.core.data.Vectors;
import convex.core.data.prim.CVMLong;
import convex.db.calcite.ConvexType;

/**
 * Covers {@link SQLSchema#insert}'s row-type validation (added 2026-10-03,
 * found live, not in a test first) — a String value for a column declared
 * {@code INTEGER} must be rejected at the moment of the write, never
 * written successfully and left to surface later as a {@code
 * ClassCastException} on an unrelated read. See {@code
 * SQLSchema#validateRowTypes}'s own doc for the full incident.
 */
class SQLSchemaRowValidationTest {

	private static SQLSchema tableWithIntegerColumn() {
		SQLSchema schema = SQLSchema.create();
		schema.createTable("t", new String[]{"id", "n", "s"},
			new ConvexType[]{ConvexType.INTEGER, ConvexType.INTEGER, ConvexType.VARCHAR}, 1);
		return schema;
	}

	@Test
	void rejectsAStringValueForADeclaredIntegerColumn() {
		SQLSchema schema = tableWithIntegerColumn();

		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
			() -> schema.insert("t", Vectors.of(CVMLong.create(1L), "", "hello")));

		assertTrue(ex.getMessage().contains("INTEGER"));
		assertTrue(ex.getMessage().contains("t"));
	}

	@Test
	void theRejectedRowIsNeverWritten() {
		SQLSchema schema = tableWithIntegerColumn();

		assertThrows(IllegalArgumentException.class,
			() -> schema.insert("t", Vectors.of(CVMLong.create(1L), "bad", "hello")));

		// Not just that insert() threw -- confirm nothing was actually
		// persisted, i.e. this is a reject-before-write, not a
		// reject-after-the-fact with the bad row already sitting there.
		assertNull(schema.selectByKey("t", CVMLong.create(1L)));
	}

	@Test
	void aValidIntegerValueInsertsNormally() {
		SQLSchema schema = tableWithIntegerColumn();
		boolean ok = schema.insert("t", Vectors.of(CVMLong.create(1L), CVMLong.create(42L), "hello"));
		assertTrue(ok);
	}

	@Test
	void aNullValueForTheIntegerColumnIsStillAllowed() {
		SQLSchema schema = tableWithIntegerColumn();
		boolean ok = schema.insert("t", Vectors.of(CVMLong.create(1L), null, "hello"));
		assertTrue(ok);
	}

	@Test
	void aStringValueForAVarcharColumnIsUnaffected() {
		SQLSchema schema = tableWithIntegerColumn();
		boolean ok = schema.insert("t", Vectors.of(CVMLong.create(1L), CVMLong.create(1L), Strings.create("hello")));
		assertTrue(ok);
	}
}
