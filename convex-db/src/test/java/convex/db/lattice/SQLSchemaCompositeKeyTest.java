package convex.db.lattice;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import convex.core.data.ACell;
import convex.core.data.Vectors;
import convex.core.data.prim.CVMLong;
import convex.db.calcite.ConvexType;

/**
 * Regression coverage for a composite-PK bug found live 2026-08-07:
 * {@code selectByKey}/{@code deleteByKey} only ever encoded the FIRST column
 * of the key ({@link SQLSchema#toKey(ACell)}), while {@code insert} correctly
 * encoded all {@code pkCount} leading columns via
 * {@link SQLSchema#toCompositeKey}. On a composite-PK table, a lookup/delete
 * by the single first column silently matched nothing (a different, shorter
 * byte key than what was actually inserted) — mirrors the exact shape of
 * dbase's {@code Docerror} table, {@code (DOCID, ERRORID)}.
 */
class SQLSchemaCompositeKeyTest {

	private SQLSchema tableWithCompositeKey() {
		SQLSchema schema = SQLSchema.create();
		schema.createTable("docerror", new String[]{"DOCID", "ERRORID", "MSG"},
			new ConvexType[]{ConvexType.INTEGER, ConvexType.INTEGER, ConvexType.VARCHAR}, 2);
		return schema;
	}

	private static List<ACell> key(long docId, long errorId) {
		return List.of(CVMLong.create(docId), CVMLong.create(errorId));
	}

	@Test
	void selectByKeyFindsARowByItsFullCompositeKey() {
		SQLSchema schema = tableWithCompositeKey();
		schema.insert("docerror", Vectors.of(CVMLong.create(1), CVMLong.create(1), "boom"));
		schema.insert("docerror", Vectors.of(CVMLong.create(1), CVMLong.create(2), "bang"));

		assertNotNull(schema.selectByKey("docerror", key(1, 1)));
		assertNotNull(schema.selectByKey("docerror", key(1, 2)));
		// same DOCID, no ERRORID=3 row -- must not match either sibling row
		assertNull(schema.selectByKey("docerror", key(1, 3)));
	}

	@Test
	void deleteByKeyRemovesOnlyTheExactCompositeMatch() {
		SQLSchema schema = tableWithCompositeKey();
		schema.insert("docerror", Vectors.of(CVMLong.create(1), CVMLong.create(1), "boom"));
		schema.insert("docerror", Vectors.of(CVMLong.create(1), CVMLong.create(2), "bang"));

		boolean deleted = schema.deleteByKey("docerror", key(1, 1));

		assertTrue(deleted);
		assertNull(schema.selectByKey("docerror", key(1, 1)));
		// the sibling row sharing DOCID=1 but a different ERRORID must survive
		assertNotNull(schema.selectByKey("docerror", key(1, 2)));
	}

	@Test
	void deleteByKeyReturnsFalseWhenTheCompositeKeyDoesNotMatch() {
		SQLSchema schema = tableWithCompositeKey();
		schema.insert("docerror", Vectors.of(CVMLong.create(1), CVMLong.create(1), "boom"));

		assertFalse(schema.deleteByKey("docerror", key(1, 999)));
		assertNotNull(schema.selectByKey("docerror", key(1, 1)));
	}

	@Test
	void singleColumnPrimaryKeyOverloadsAreUnaffected() {
		SQLSchema schema = SQLSchema.create();
		schema.createTable("t", new String[]{"id", "v"},
			new ConvexType[]{ConvexType.INTEGER, ConvexType.VARCHAR}, 1);
		schema.insert("t", Vectors.of(CVMLong.create(1), "hello"));

		assertNotNull(schema.selectByKey("t", CVMLong.create(1)));
		assertEquals("hello", schema.selectByKey("t", CVMLong.create(1)).get(1).toString());

		assertTrue(schema.deleteByKey("t", CVMLong.create(1)));
		assertNull(schema.selectByKey("t", CVMLong.create(1)));
	}
}
