package convex.db.lattice;

import java.util.Map;
import java.util.function.Function;

import convex.core.data.ACell;
import convex.core.data.AHashMap;
import convex.core.data.Maps;
import convex.core.util.Utils;
import convex.lattice.ALattice;
import convex.lattice.LatticeContext;

/**
 * A map lattice whose child lattice depends on the key. {@code MapLattice}
 * uses one child lattice for every key, which is not enough for the upper
 * levels of an owner tree: the lattice below owner 7, db "dev" has to know it
 * is below exactly that owner and db (see {@link OwnerTree}).
 *
 * <p>Merges key by key, like {@link HybridDatabaseMapLattice}.
 */
public class KeyedChildMapLattice<K extends ACell, V extends ACell> extends ALattice<AHashMap<K, V>> {

    private final Class<K> keyType;
    private final Function<K, ALattice<V>> childFor;

    public KeyedChildMapLattice(Class<K> keyType, Function<K, ALattice<V>> childFor) {
        this.keyType = keyType;
        this.childFor = childFor;
    }

    @Override
    public AHashMap<K, V> zero() {
        return Maps.empty();
    }

    @Override
    public boolean checkForeign(AHashMap<K, V> value) {
        return (value instanceof AHashMap);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends ACell> ALattice<T> path(ACell childKey) {
        if (!keyType.isInstance(childKey)) return null;
        return (ALattice<T>) childFor.apply(keyType.cast(childKey));
    }

    @Override
    public AHashMap<K, V> merge(AHashMap<K, V> ownValue, AHashMap<K, V> otherValue) {
        return mergeImpl(null, ownValue, otherValue);
    }

    @Override
    public AHashMap<K, V> merge(LatticeContext context, AHashMap<K, V> ownValue, AHashMap<K, V> otherValue) {
        return mergeImpl(context, ownValue, otherValue);
    }

    private AHashMap<K, V> mergeImpl(LatticeContext context, AHashMap<K, V> ownValue, AHashMap<K, V> otherValue) {
        if (otherValue == null) return ownValue;
        if (ownValue == null) ownValue = zero();
        AHashMap<K, V> result = ownValue;
        for (Map.Entry<K, V> entry : otherValue.entrySet()) {
            K key = entry.getKey();
            // A key of the wrong type has no child lattice to validate it with; leave it out.
            if (!keyType.isInstance(key)) continue;
            V b = entry.getValue();
            V a = ownValue.get(key);
            ALattice<V> lattice = childFor.apply(key);
            V m = (context == null) ? lattice.merge(a, b) : lattice.merge(context, a, b);
            if (!Utils.equals(m, a)) {
                result = result.assoc(key, m);
            }
        }
        return result;
    }
}
