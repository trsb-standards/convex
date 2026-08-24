package convex.db.lattice;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import convex.core.data.AString;

/**
 * Live, mutable, process-wide registry of which tables are versioned
 * (row-history-tracked) — one map per schema name, since table names are
 * only unique within a schema, not across a whole node.
 *
 * <p>Consulted by {@link HybridTableStoreLattice#path} at merge/navigation
 * time, not baked into a precomputed lattice tree — {@code ALattice.path()}
 * has no caching contract, so this sidesteps the chicken-and-egg problem of
 * needing to read a schema's own catalog before the lattice that would open
 * it exists. See {@code DbaseServer}'s bootstrap/replicate wiring (in the
 * dbase project) for where this gets populated from {@code meta.ot.VERSIONED}
 * on each node.
 *
 * <p>Every node merging a peer's write for a table must already agree which
 * lattice to use for it — populate this registry BEFORE any row-data merge
 * for a table, not after, or a peer merges with the wrong shape assumption
 * and corrupts state.
 */
public class TableVersionRegistry {

    private static final ConcurrentHashMap<AString, ConcurrentHashMap<AString, Boolean>> BY_SCHEMA =
        new ConcurrentHashMap<>();

    private TableVersionRegistry() {}

    private static ConcurrentHashMap<AString, Boolean> tablesFor(AString schemaName) {
        return BY_SCHEMA.computeIfAbsent(schemaName, k -> new ConcurrentHashMap<>());
    }

    /** Marks a table as versioned (row-history-tracked). */
    public static void markVersioned(AString schemaName, AString tableName) {
        tablesFor(schemaName).put(tableName, Boolean.TRUE);
    }

    /** Clears a table's versioned marking (e.g. on DROP TABLE, so a later re-CREATE of the same name doesn't inherit stale state). */
    public static void unmark(AString schemaName, AString tableName) {
        Map<AString, Boolean> tables = BY_SCHEMA.get(schemaName);
        if (tables != null) tables.remove(tableName);
    }

    /** Returns true if {@code tableName} in {@code schemaName} is currently marked versioned. */
    public static boolean isVersioned(AString schemaName, AString tableName) {
        Map<AString, Boolean> tables = BY_SCHEMA.get(schemaName);
        return (tables != null) && Boolean.TRUE.equals(tables.get(tableName));
    }
}
