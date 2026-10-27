package convex.db.lattice;

import convex.core.data.ACell;
import convex.core.data.AHashMap;
import convex.core.data.AString;
import convex.core.data.Index;
import convex.core.data.Keyword;
import convex.core.data.Strings;
import convex.core.data.prim.CVMLong;
import convex.db.ConvexDB;
import convex.lattice.ALattice;
import convex.lattice.cursor.ALatticeCursor;

/**
 * The owner tree: {@code owner -> db -> schema -> table}. Every entity has its
 * own subtree, so one owner's data, or one db of one owner, is a single
 * subtree that can be replicated without touching anything else (see
 * PLAN_owner_tree_replication.md).
 *
 * <p>The two upper levels are added above the existing schema map; one db of
 * one owner is an ordinary {@link ConvexDB}.
 */
public final class OwnerTree {

    private OwnerTree() {}

    /** Root lattice: owner entity id -> db name -> schema map. */
    public static final ALattice<AHashMap<CVMLong, AHashMap<AString, AHashMap<AString, Index<Keyword, ACell>>>>> ROOT_LATTICE =
        new KeyedChildMapLattice<>(CVMLong.class, OwnerTree::dbMapLattice);

    private static ALattice<AHashMap<AString, AHashMap<AString, Index<Keyword, ACell>>>> dbMapLattice(CVMLong owner) {
        return new KeyedChildMapLattice<>(AString.class,
            db -> HybridDatabaseMapLattice.forScope(scope(owner.longValue(), db.toString())));
    }

    /**
     * The prefix that identifies one db of one owner, e.g. {@code "7/dev/"}.
     * A db name cannot contain {@code '/'}, so the prefix is unambiguous.
     */
    public static String scope(long owner, String db) {
        if (db.indexOf('/') >= 0) throw new IllegalArgumentException("A db name cannot contain '/': " + db);
        return owner + "/" + db + "/";
    }

    /** The table-flag registry key of a schema in one db of one owner. */
    public static AString registryKey(long owner, String db, String schema) {
        return Strings.create(scope(owner, db) + schema);
    }

    /** Path of one owner's subtree below the root. */
    public static ACell[] path(long owner) {
        return new ACell[] {CVMLong.create(owner)};
    }

    /** Path of one db of one owner below the root. */
    public static ACell[] path(long owner, String db) {
        return new ACell[] {CVMLong.create(owner), Strings.create(db)};
    }

    /** Path of one schema in one db of one owner below the root. */
    public static ACell[] path(long owner, String db, String schema) {
        return new ACell[] {CVMLong.create(owner), Strings.create(db), Strings.create(schema)};
    }

    /**
     * One db of one owner, as a ConvexDB.
     *
     * @param root Cursor at the root of an owner tree (lattice {@link #ROOT_LATTICE})
     */
    public static ConvexDB database(ALatticeCursor<?> root, long owner, String db) {
        return ConvexDB.connect(root.path(path(owner, db)), scope(owner, db));
    }
}
