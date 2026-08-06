package convex.db.lattice;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

import convex.db.calcite.ConvexType;

/**
 * Covers {@link SQLSchema#createTable}'s "table already exists" merge path —
 * in particular that growing a table's column set is matched by column NAME,
 * not by position/count, so it works correctly regardless of where in the
 * desired column list the new column(s) appear.
 */
class SQLSchemaTest {

	@Test
	void createTableIsANoOpWhenColumnsAreUnchanged() {
		SQLSchema schema = SQLSchema.create();
		schema.createTable("t", new String[]{"a", "b"},
			new ConvexType[]{ConvexType.INTEGER, ConvexType.VARCHAR}, 1);

		boolean changed = schema.createTable("t", new String[]{"a", "b"},
			new ConvexType[]{ConvexType.INTEGER, ConvexType.VARCHAR}, 1);

		assertFalse(changed, "recreating with the identical column set must be a no-op");
		assertEquals(2, schema.getColumnNames("t").length);
	}

	@Test
	void createTableAppendsNewColumnAtTheEnd() {
		SQLSchema schema = SQLSchema.create();
		schema.createTable("t", new String[]{"a", "b"},
			new ConvexType[]{ConvexType.INTEGER, ConvexType.VARCHAR}, 1);

		boolean changed = schema.createTable("t", new String[]{"a", "b", "c"},
			new ConvexType[]{ConvexType.INTEGER, ConvexType.VARCHAR, ConvexType.INTEGER}, 1);

		assertTrue(changed);
		assertArrayEquals(new String[]{"a", "b", "c"}, schema.getColumnNames("t"));
	}

	@Test
	void createTableInsertingNewColumnInTheMiddleDoesNotDuplicateAnyColumn() {
		// Regression test: SQLSchema.createTable's "append new columns" logic
		// used to compare purely by count ("anything past the existing
		// column count is new"), so redefining a table's desired schema with
		// a new column inserted in the MIDDLE (e.g. [a, b] -> [a, NEW, b])
		// instead of strictly appended at the end incorrectly re-appended
		// "b" a second time (since it now sits at the position the old logic
		// treated as "new"), corrupting the table with a duplicate column
		// name — which breaks "SELECT *" with an "ambiguous column" error.
		SQLSchema schema = SQLSchema.create();
		schema.createTable("t", new String[]{"a", "b"},
			new ConvexType[]{ConvexType.INTEGER, ConvexType.VARCHAR}, 1);

		boolean changed = schema.createTable("t", new String[]{"a", "new", "b"},
			new ConvexType[]{ConvexType.INTEGER, ConvexType.INTEGER, ConvexType.VARCHAR}, 1);

		assertTrue(changed, "the genuinely new column must still trigger a write");

		String[] colNames = schema.getColumnNames("t");
		Set<String> unique = new HashSet<>(Arrays.asList(colNames));
		assertEquals(colNames.length, unique.size(),
			"no column name should appear twice: " + Arrays.toString(colNames));
		assertTrue(unique.contains("a"));
		assertTrue(unique.contains("b"));
		assertTrue(unique.contains("new"));
	}
}
