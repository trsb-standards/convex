package convex.db.lattice;

import convex.core.data.ACell;
import convex.core.data.AString;
import convex.core.data.AVector;
import convex.core.data.Index;
import convex.db.calcite.ConvexColumnType;
import convex.lattice.cursor.ALatticeCursor;
import convex.lattice.cursor.Cursors;

/**
 * Convenience facade over the now-polymorphic {@link SQLSchema}: every table
 * created through a {@code VersionedSQLSchema} instance is automatically
 * versioned (row-history-tracked), preserving this class's original "the
 * whole schema is versioned" contract for existing callers ({@code
 * DBComparisonBench}'s "direct versioned" benchmark and several convex-db
 * demos/tests) without them needing to pass {@code versioned=true}
 * explicitly at every {@code createTable} call.
 *
 * <p>Mixing versioned and non-versioned tables in one schema (the actual
 * point of the {@code TableVersionRegistry}/{@code HybridTableStoreLattice}
 * machinery this class now delegates to) doesn't need this facade at all —
 * use a plain {@link SQLSchema} and pass {@code versioned} explicitly per
 * table, e.g. via {@code createTable(name, columns, types, pkCount, true)}
 * or SQL's {@code CREATE TABLE ... VERSIONED}.
 *
 * <p>{@link #getHistory}/{@link #getAsOf}/{@link #convertToVersioned} live
 * directly on {@link SQLSchema} now (not overridden here) — a plain
 * SQLSchema needs them too, since that's what SQL-driven {@code CREATE
 * TABLE ... VERSIONED} actually creates, not this facade.
 */
public class VersionedSQLSchema extends SQLSchema {

    VersionedSQLSchema(ALatticeCursor<Index<AString, AVector<ACell>>> cursor, AString schemaName) {
        super(cursor, schemaName);
    }

    /**
     * Creates a new standalone versioned schema backed by an in-memory cursor.
     *
     * @return New VersionedSQLSchema instance
     */
    public static VersionedSQLSchema create() {
        AString name = anonymousSchemaName();
        ALatticeCursor<Index<AString, AVector<ACell>>> cursor =
            Cursors.createLattice(new HybridTableStoreLattice(name));
        return new VersionedSQLSchema(cursor, name);
    }

    /**
     * Connects to an existing cursor (e.g. from a NodeServer chain) for persistence.
     *
     * @param cursor Lattice cursor pointing at the table store
     * @return New VersionedSQLSchema instance
     */
    public static VersionedSQLSchema connect(ALatticeCursor<Index<AString, AVector<ACell>>> cursor) {
        return new VersionedSQLSchema(cursor, anonymousSchemaName());
    }

    /**
     * Wraps an existing {@link SQLSchema} with this facade, sharing the same
     * underlying cursor and schema name — every table subsequently created
     * through the wrapper is versioned, tables already created through
     * {@code schema} keep whatever versioned-ness they already had.
     *
     * @param schema Existing SQLSchema instance
     * @return VersionedSQLSchema sharing the same cursor
     */
    public static VersionedSQLSchema wrap(SQLSchema schema) {
        return new VersionedSQLSchema(schema.cursor(), schema.getSchemaName());
    }

    /**
     * Every table created through this facade is versioned, matching this
     * class's historical "the whole schema is versioned" contract — every
     * other {@code createTable} overload (inherited from {@link SQLSchema})
     * funnels down to this exact signature, so overriding it here is enough
     * to intercept all of them.
     */
    @Override
    public boolean createTable(AString name, String[] columns, ConvexColumnType[] types, int pkCount) {
        return createTable(name, columns, types, pkCount, true);
    }

    // ── Covariant table accessors ────────────────────────────────────────
    // Convenience for callers of this facade specifically (avoids an
    // instanceof cast for the common case). Returns null rather than
    // throwing if the named table happens not to actually be versioned
    // (possible after wrap() of a schema with pre-existing plain tables) --
    // fails gracefully ("not found from this facade's perspective") instead
    // of a ClassCastException.

    @Override
    public VersionedSQLTable getTable(AString name) {
        SQLTable table = super.getTable(name);
        return (table instanceof VersionedSQLTable vt) ? vt : null;
    }

    @Override
    public VersionedSQLTable getTable(String name) {
        return getTable(convex.core.data.Strings.create(name));
    }

    @Override
    public VersionedSQLTable getLiveTable(AString name) {
        VersionedSQLTable table = getTable(name);
        if (table == null || !table.isLive()) return null;
        return table;
    }

    @Override
    public VersionedSQLTable getLiveTable(String name) {
        return getLiveTable(convex.core.data.Strings.create(name));
    }
}
