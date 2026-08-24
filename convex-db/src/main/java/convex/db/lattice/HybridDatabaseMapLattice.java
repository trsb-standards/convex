package convex.db.lattice;

import java.util.Map;

import convex.core.data.ACell;
import convex.core.data.AHashMap;
import convex.core.data.AString;
import convex.core.data.Index;
import convex.core.data.Keyword;
import convex.core.data.Maps;
import convex.core.util.Utils;
import convex.db.ConvexDB;
import convex.lattice.ALattice;
import convex.lattice.LatticeContext;
import convex.lattice.generic.KeyedLattice;

/**
 * Lattice for the {@code ConvexDB} database map (schema name → schema
 * state), replacing the plain {@link convex.lattice.generic.MapLattice} that
 * {@code ConvexDB.DATABASE_MAP_LATTICE} previously used.
 *
 * <p>{@code MapLattice} is homogeneous (confirmed by reading its source: one
 * single child lattice for every key, {@code path()} ignores the key
 * entirely) — it can't give each schema its own {@link
 * HybridTableStoreLattice} bound to that schema's own name, which is what
 * lets {@code TableVersionRegistry} disambiguate same-named tables in
 * different schemas.
 *
 * <p>Constructing a fresh {@code KeyedLattice.create(ConvexDB.KEY_TABLES, new
 * HybridTableStoreLattice(schemaName))} per schema name on every {@link
 * #path} call is cheap and needs no caching — neither {@code KeyedLattice}
 * nor {@code HybridTableStoreLattice} hold any state of their own; all real
 * state lives in the shared static {@link TableVersionRegistry}.
 */
public class HybridDatabaseMapLattice extends ALattice<AHashMap<AString, Index<Keyword, ACell>>> {

    public static final HybridDatabaseMapLattice INSTANCE = new HybridDatabaseMapLattice();

    private HybridDatabaseMapLattice() {}

    private ALattice<Index<Keyword, ACell>> latticeFor(AString schemaName) {
        return KeyedLattice.create(ConvexDB.KEY_TABLES, new HybridTableStoreLattice(schemaName));
    }

    @Override
    public AHashMap<AString, Index<Keyword, ACell>> zero() {
        return Maps.empty();
    }

    @Override
    public boolean checkForeign(AHashMap<AString, Index<Keyword, ACell>> value) {
        return (value instanceof AHashMap);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends ACell> ALattice<T> path(ACell childKey) {
        if (!(childKey instanceof AString schemaName)) return null;
        return (ALattice<T>) latticeFor(schemaName);
    }

    @Override
    public AHashMap<AString, Index<Keyword, ACell>> merge(
            AHashMap<AString, Index<Keyword, ACell>> ownValue,
            AHashMap<AString, Index<Keyword, ACell>> otherValue) {
        return mergeImpl(null, ownValue, otherValue);
    }

    @Override
    public AHashMap<AString, Index<Keyword, ACell>> merge(
            LatticeContext context,
            AHashMap<AString, Index<Keyword, ACell>> ownValue,
            AHashMap<AString, Index<Keyword, ACell>> otherValue) {
        return mergeImpl(context, ownValue, otherValue);
    }

    /** Mirrors {@link HybridTableStoreLattice}'s own per-key merge loop, one level up. */
    private AHashMap<AString, Index<Keyword, ACell>> mergeImpl(
            LatticeContext context,
            AHashMap<AString, Index<Keyword, ACell>> ownValue,
            AHashMap<AString, Index<Keyword, ACell>> otherValue) {
        if (otherValue == null) return ownValue;
        if (ownValue == null) ownValue = zero();

        AHashMap<AString, Index<Keyword, ACell>> result = ownValue;
        for (Map.Entry<AString, Index<Keyword, ACell>> entry : otherValue.entrySet()) {
            AString schemaName = entry.getKey();
            Index<Keyword, ACell> b = entry.getValue();
            Index<Keyword, ACell> a = ownValue.get(schemaName);

            ALattice<Index<Keyword, ACell>> lattice = latticeFor(schemaName);
            Index<Keyword, ACell> m = (context == null) ? lattice.merge(a, b) : lattice.merge(context, a, b);

            if (!Utils.equals(m, a)) {
                result = result.assoc(schemaName, m);
            }
        }
        return result;
    }
}
