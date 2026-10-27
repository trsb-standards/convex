package convex.db.lattice;

import java.util.Map;

import convex.core.data.ACell;
import convex.core.data.AString;
import convex.core.data.AVector;
import convex.core.data.Index;
import convex.core.util.Utils;
import convex.lattice.ALattice;
import convex.lattice.LatticeContext;

/**
 * Lattice implementation for a SQL schema's table store, dispatching each
 * table to either {@link SQLTableLattice} or {@link VersionedSQLTableLattice}
 * per-table, based on {@link TableVersionRegistry}.
 *
 * <p>Unlike {@link TableStoreLattice}/{@link VersionedTableStoreLattice} —
 * both thin {@link convex.lattice.generic.IndexLattice} wrappers, and
 * therefore homogeneous by construction (one single child lattice applies to
 * every key, with no way to vary per key) — this lattice looks up the right
 * child lattice per table name, so a single schema can hold both versioned
 * and non-versioned tables side by side.
 *
 * <p>One instance per schema name (table names are unique within a schema,
 * not across a whole node — see {@link TableVersionRegistry}'s own javadoc
 * for why the registry is scoped the same way).
 *
 * <p>Every node merging a peer's write for a table must already agree which
 * lattice to use for it, resolved here from {@link TableVersionRegistry}
 * alone (no shape-sniffing of the incoming value) — the registry must be
 * populated from {@code meta.ot.VERSIONED} before any row-data merge for a
 * newly-arrived (replicated) table, not derived after the fact.
 */
public class HybridTableStoreLattice extends ALattice<Index<AString, AVector<ACell>>> {

    private final AString schemaName;

    public HybridTableStoreLattice(AString schemaName) {
        this.schemaName = schemaName;
    }

    private ALattice<AVector<ACell>> latticeFor(AString tableName) {
        boolean versioned = TableVersionRegistry.isVersioned(schemaName, tableName);
        return versioned ? VersionedSQLTableLattice.INSTANCE : SQLTableLattice.INSTANCE;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Index<AString, AVector<ACell>> zero() {
        return (Index<AString, AVector<ACell>>) Index.EMPTY;
    }

    @Override
    public boolean checkForeign(Index<AString, AVector<ACell>> value) {
        return (value instanceof Index);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends ACell> ALattice<T> path(ACell childKey) {
        if (!(childKey instanceof AString tableName)) return null;
        return (ALattice<T>) latticeFor(tableName);
    }

    @Override
    public Index<AString, AVector<ACell>> merge(
            Index<AString, AVector<ACell>> ownValue,
            Index<AString, AVector<ACell>> otherValue) {
        return mergeImpl(null, ownValue, otherValue);
    }

    @Override
    public Index<AString, AVector<ACell>> merge(
            LatticeContext context,
            Index<AString, AVector<ACell>> ownValue,
            Index<AString, AVector<ACell>> otherValue) {
        return mergeImpl(context, ownValue, otherValue);
    }

    /**
     * Mirrors {@code AKeyedLattice.mergeImpl}'s explicit per-key loop, but over
     * the open/dynamic key set actually present in {@code otherValue} (table
     * names created at runtime by CREATE TABLE) rather than a fixed registered
     * list — {@code IndexLattice.mergeDifferences}'s merge function has no key
     * parameter, so it can't vary per key the way this needs to.
     */
    private Index<AString, AVector<ACell>> mergeImpl(
            LatticeContext context,
            Index<AString, AVector<ACell>> ownValue,
            Index<AString, AVector<ACell>> otherValue) {
        if (otherValue == null) return ownValue;
        // #561: never wholesale-accept a foreign map on the first (own==null) merge —
        // start from empty so every table's foreign value is validated through its
        // own child merge below.
        if (ownValue == null) ownValue = zero();

        Index<AString, AVector<ACell>> result = ownValue;
        for (Map.Entry<AString, AVector<ACell>> entry : otherValue.entrySet()) {
            AString tableName = entry.getKey();
            AVector<ACell> b = entry.getValue();
            AVector<ACell> a = ownValue.get(tableName);

            ALattice<AVector<ACell>> lattice = latticeFor(tableName);
            AVector<ACell> m = (context == null) ? lattice.merge(a, b) : lattice.merge(context, a, b);

            if (!Utils.equals(m, a)) {
                result = result.assoc(tableName, m);
            }
        }
        return result;
    }
}
