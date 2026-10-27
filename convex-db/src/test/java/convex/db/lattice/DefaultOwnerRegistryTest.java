package convex.db.lattice;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import convex.core.data.ACell;
import convex.core.data.AString;
import convex.core.data.AVector;
import convex.core.data.Strings;
import convex.core.data.Vectors;
import convex.core.data.prim.CVMLong;
import convex.db.calcite.ConvexColumnType;
import convex.db.calcite.ConvexType;

/** Rows written without an owner into a schema with a default owner ({@link DefaultOwnerRegistry}). */
class DefaultOwnerRegistryTest {

	private static final AString T = Strings.create("t");

	private static SQLSchema schema(boolean withDefault) {
		SQLSchema s = SQLSchema.create();
		if (withDefault) DefaultOwnerRegistry.set(s.getRegistryKey(), 1);
		s.createTable(T, new String[]{"ID", "VAL"},
			new ConvexColumnType[]{ConvexColumnType.of(ConvexType.INTEGER), ConvexColumnType.of(ConvexType.VARCHAR)});
		return s;
	}

	private static ACell owner(SQLSchema s, long id) {
		AVector<ACell> row = s.selectByKey(T, CVMLong.create(id));
		return row.get(SQLSchema.OWNERID_COLUMN.equals(s.getColumnNames(T)[2]) ? 2 : -1);
	}

	@Test
	void aRowWithoutAnOwnerGetsTheDefault() {
		SQLSchema s = schema(true);
		s.insert("t", CVMLong.create(1), "a");
		assertEquals(CVMLong.create(1), owner(s, 1));
	}

	@Test
	void aGivenOwnerAndAStoredOwnerAreKept() {
		SQLSchema s = schema(true);
		s.insert(T, Vectors.of(CVMLong.create(1), Strings.create("a"), CVMLong.create(7)));
		assertEquals(CVMLong.create(7), owner(s, 1));
		// a rewrite of the row without an owner keeps the stored one
		s.insert("t", CVMLong.create(1), "changed");
		assertEquals(CVMLong.create(7), owner(s, 1));
	}

	@Test
	void noDefaultLeavesTheOwnerEmpty() {
		SQLSchema s = schema(false);
		s.insert("t", CVMLong.create(1), "a");
		assertNull(owner(s, 1));
	}

	@Test
	void fillMissingOwnersFixesOlderRowsOnly() {
		SQLSchema s = schema(false);
		s.insert("t", CVMLong.create(1), "old");
		s.insert(T, Vectors.of(CVMLong.create(2), Strings.create("owned"), CVMLong.create(7)));
		DefaultOwnerRegistry.set(s.getRegistryKey(), 1);
		assertEquals(1, s.fillMissingOwners());
		assertEquals(CVMLong.create(1), owner(s, 1));
		assertEquals(CVMLong.create(7), owner(s, 2));
		assertEquals(0, s.fillMissingOwners());
	}
}
