package convex.db.lattice;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

import convex.core.data.ACell;
import convex.core.data.AHashMap;
import convex.core.data.AString;
import convex.core.data.Hash;
import convex.core.data.Index;
import convex.core.data.Keyword;
import convex.core.data.Strings;
import convex.core.data.Vectors;
import convex.core.data.prim.CVMLong;
import convex.core.lang.RT;
import convex.db.ConvexDB;
import convex.db.SchemaView;
import convex.db.calcite.ConvexColumnType;
import convex.db.calcite.ConvexType;
import convex.db.jdbc.ConvexDriver;
import convex.lattice.cursor.ALatticeCursor;
import convex.lattice.cursor.Cursors;

/**
 * The owner tree ({@link OwnerTree}): {@code owner -> db -> schema -> table},
 * with an ordinary {@link ConvexDB} as one db of one owner.
 */
class OwnerTreeTest {

	private static final String[] COLS = {"ID", "NAME"};
	private static final ConvexType[] TYPES = {ConvexType.INTEGER, ConvexType.VARCHAR};

	private static ALatticeCursor<AHashMap<CVMLong, AHashMap<AString, AHashMap<AString, Index<Keyword, ACell>>>>> newRoot() {
		return Cursors.createLattice(OwnerTree.ROOT_LATTICE);
	}

	private static ConvexDB dbOf(ALatticeCursor<?> root, long owner, String db) {
		return OwnerTree.database(root, owner, db);
	}

	private static SQLSchema schemaOf(ALatticeCursor<?> root, long owner, String db, String schema) {
		return dbOf(root, owner, db).database(schema).tables();
	}

	private static void insert(SQLSchema schema, long id, String name) {
		schema.insert("customer", Vectors.of(CVMLong.create(id), name));
	}

	private static Hash hashAt(ALatticeCursor<?> root, ACell... path) {
		ACell v = RT.getIn(root.get(), path);
		return (v == null) ? null : v.getHash();
	}

	@Test
	void theSameSchemaNameHoldsSeparateRowsPerOwnerAndPerDb() {
		var root = newRoot();
		SQLSchema dev1 = schemaOf(root, 1, "dev", "app");
		SQLSchema prod1 = schemaOf(root, 1, "prod", "app");
		SQLSchema dev2 = schemaOf(root, 2, "dev", "app");
		for (SQLSchema s : new SQLSchema[] {dev1, prod1, dev2}) s.createTable("customer", COLS, TYPES, 1);

		insert(dev1, 1, "dev row of owner 1");
		insert(prod1, 1, "prod row of owner 1");
		insert(prod1, 2, "second prod row of owner 1");

		assertEquals(1, dev1.getRowCount("customer"));
		assertEquals(2, prod1.getRowCount("customer"));
		assertEquals(0, dev2.getRowCount("customer"));
	}

	@Test
	void aWriteUnderOneOwnerLeavesTheOtherOwnersSubtreeUntouched() {
		var root = newRoot();
		SQLSchema own1 = schemaOf(root, 1, "dev", "app");
		SQLSchema own2 = schemaOf(root, 2, "dev", "app");
		own1.createTable("customer", COLS, TYPES, 1);
		own2.createTable("customer", COLS, TYPES, 1);
		insert(own1, 1, "a");

		Hash owner1Before = hashAt(root, CVMLong.create(1));
		Hash owner2Before = hashAt(root, CVMLong.create(2));
		insert(own2, 7, "b");

		assertEquals(owner1Before, hashAt(root, CVMLong.create(1)));
		assertNotEquals(owner2Before, hashAt(root, CVMLong.create(2)));
	}

	@Test
	void replicatingOneOwnersSubtreeBringsNothingOfAnotherOwner() {
		var source = newRoot();
		SQLSchema own1 = schemaOf(source, 1, "dev", "app");
		SQLSchema own2 = schemaOf(source, 2, "dev", "app");
		own1.createTable("customer", COLS, TYPES, 1);
		own2.createTable("customer", COLS, TYPES, 1);
		insert(own1, 1, "mine");
		insert(own2, 2, "theirs");

		// What a personal node of owner 1 does: merge the value at path [1], nothing else.
		var replica = newRoot();
		ACell owner1Tree = RT.getIn(source.get(), CVMLong.create(1));
		replica.path(CVMLong.create(1)).merge(owner1Tree);

		assertEquals(1, schemaOf(replica, 1, "dev", "app").getRowCount("customer"));
		assertNull(RT.getIn(replica.get(), CVMLong.create(2)));
		assertEquals(hashAt(source, CVMLong.create(1)), hashAt(replica, CVMLong.create(1)));
	}

	@Test
	void replicatingOneDbOfAnOwnerIsASingleSubtreeToo() {
		var source = newRoot();
		SQLSchema dev = schemaOf(source, 1, "dev", "app");
		SQLSchema prod = schemaOf(source, 1, "prod", "app");
		dev.createTable("customer", COLS, TYPES, 1);
		prod.createTable("customer", COLS, TYPES, 1);
		insert(dev, 1, "dev");
		insert(prod, 2, "prod");

		var replica = newRoot();
		ACell prodTree = RT.getIn(source.get(), CVMLong.create(1), Strings.create("prod"));
		replica.path(CVMLong.create(1), Strings.create("prod")).merge(prodTree);

		assertEquals(1, schemaOf(replica, 1, "prod", "app").getRowCount("customer"));
		assertNull(RT.getIn(replica.get(), CVMLong.create(1), Strings.create("dev")));
	}

	@Test
	void twoReplicasOfOneOwnerConvergeByMergingAtTheOwnerPath() {
		var a = newRoot();
		var b = newRoot();
		SQLSchema onA = schemaOf(a, 1, "dev", "app");
		SQLSchema onB = schemaOf(b, 1, "dev", "app");
		onA.createTable("customer", COLS, TYPES, 1);
		onB.createTable("customer", COLS, TYPES, 1);
		insert(onA, 1, "written on a");
		insert(onB, 2, "written on b");

		ACell fromA = RT.getIn(a.get(), CVMLong.create(1));
		ACell fromB = RT.getIn(b.get(), CVMLong.create(1));
		a.path(CVMLong.create(1)).merge(fromB);
		b.path(CVMLong.create(1)).merge(fromA);

		assertEquals(2, schemaOf(a, 1, "dev", "app").getRowCount("customer"));
		assertEquals(2, schemaOf(b, 1, "dev", "app").getRowCount("customer"));
	}

	@Test
	void aConnectionSeesItsOwnDbAndThePublicSchemasOnly() throws Exception {
		var root = newRoot();
		ConvexDB publicDb = dbOf(root, 1, "meta");
		ConvexDB minePrivate = dbOf(root, 1, "dev");
		ConvexDB theirs = dbOf(root, 2, "dev");
		fill(publicDb.database("pub").tables(), 100, "public row");
		fill(minePrivate.database("app").tables(), 1, "row of owner 1");
		fill(theirs.database("app").tables(), 2, "row of owner 2");

		// Entity 2 connected to its db "dev": its own schemas, plus the public one.
		SchemaView view = new SchemaView(theirs, "app").addAll(theirs).add("pub", publicDb);
		try (Connection c = ConvexDriver.connect(view); Statement st = c.createStatement()) {
			assertEquals("row of owner 2", single(st, "SELECT NAME FROM CUSTOMER"));
			assertEquals("row of owner 2", single(st, "SELECT NAME FROM app.CUSTOMER"));
			assertEquals("public row", single(st, "SELECT NAME FROM pub.CUSTOMER"));

			st.execute("INSERT INTO CUSTOMER VALUES (3, 'written by owner 2')");
		}
		assertEquals(2, theirs.database("app").tables().getRowCount("CUSTOMER"));
		assertEquals(1, minePrivate.database("app").tables().getRowCount("CUSTOMER"));
	}

	@Test
	void createSchemaOnAConnectionLandsInItsOwnDb() throws Exception {
		var root = newRoot();
		ConvexDB publicDb = dbOf(root, 1, "meta");
		ConvexDB mine = dbOf(root, 2, "dev");
		fill(publicDb.database("pub").tables(), 100, "public row");

		// Only a public schema is visible at first: the new schema must still go to the home db.
		SchemaView view = new SchemaView(mine, "pub").add("pub", publicDb);
		try (Connection c = ConvexDriver.connect(view); Statement st = c.createStatement()) {
			st.execute("CREATE SCHEMA notes");
			st.execute("CREATE TABLE notes.memo (ID INTEGER, NAME VARCHAR, PRIMARY KEY (ID))");
			st.execute("INSERT INTO notes.memo VALUES (1, 'kept')");
			assertEquals("kept", single(st, "SELECT NAME FROM notes.memo"));
		}
		assertEquals(1, mine.database("notes").tables().getRowCount("MEMO"));
		assertFalse(java.util.Arrays.asList(publicDb.getDatabaseNames()).contains("notes"));
		assertNull(ConvexDB.lookup("notes"), "a schema of an owner tree must not enter the process-wide registry");
	}

	private static void fill(SQLSchema schema, long id, String name) {
		schema.createTable("CUSTOMER", COLS, TYPES, 1);
		schema.insert("CUSTOMER", Vectors.of(CVMLong.create(id), name));
	}

	private static String single(Statement st, String sql) throws Exception {
		try (ResultSet rs = st.executeQuery(sql)) {
			assertTrue(rs.next(), sql);
			String v = rs.getString(1);
			assertFalse(rs.next(), "more than one row: " + sql);
			return v;
		}
	}

	@Test
	void aTableIsVersionedForOneOwnerAndDbOnly() {
		var root = newRoot();
		AString table = Strings.create("ledger");
		SQLSchema versioned = schemaOf(root, 1, "dev", "app");
		SQLSchema plainOtherDb = schemaOf(root, 1, "prod", "app");
		SQLSchema plainOtherOwner = schemaOf(root, 2, "dev", "app");
		try {
			versioned.createTable(table, COLS, colTypes(), 1, true);
			plainOtherDb.createTable(table, COLS, colTypes(), 1, false);
			plainOtherOwner.createTable(table, COLS, colTypes(), 1, false);

			assertTrue(versioned.getTable(table) instanceof VersionedSQLTable);
			assertFalse(plainOtherDb.getTable(table) instanceof VersionedSQLTable);
			assertFalse(plainOtherOwner.getTable(table) instanceof VersionedSQLTable);
			assertEquals(Strings.create("1/dev/app"), versioned.getRegistryKey());
			assertEquals(Strings.create("app"), versioned.getSchemaName());
		} finally {
			TableVersionRegistry.unmark(versioned.getRegistryKey(), table);
		}
	}

	private static ConvexColumnType[] colTypes() {
		return new ConvexColumnType[] {ConvexColumnType.of(ConvexType.INTEGER), ConvexColumnType.of(ConvexType.VARCHAR)};
	}
}
