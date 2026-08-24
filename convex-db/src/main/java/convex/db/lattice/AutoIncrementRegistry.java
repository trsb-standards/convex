package convex.db.lattice;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import convex.core.data.AString;

/**
 * Live, mutable, process-wide registry of which tables have an
 * auto-generating (single-column) primary key — one map per schema name,
 * since table names are only unique within a schema, not across a whole
 * node. Mirrors {@link TableVersionRegistry}'s exact shape; a purely
 * declarative "is this table configured this way" marker, kept separate
 * from {@link AutoIncrementCounters} (which holds the actual per-table
 * counter state) the same way this codebase already keeps other
 * sequence-generation concerns independent even when their generation
 * shape is identical (see {@code Querylog.LOGID}'s own doc).
 *
 * <p>Consulted by {@code SQLSchema.insert}/{@code insertAll} to decide
 * whether an omitted/NULL primary key value should be auto-generated. See
 * {@code DbaseServer}'s bootstrap/replicate wiring (in the dbase project)
 * for where this gets populated from {@code meta.ot.AUTOINCREMENT} on each
 * node.
 *
 * <p>Every node inserting into a table must already agree whether it's
 * auto-increment — populate this registry BEFORE any row-data merge for a
 * table, not after, mirroring {@link TableVersionRegistry}'s own contract.
 */
public class AutoIncrementRegistry {

    private static final ConcurrentHashMap<AString, ConcurrentHashMap<AString, Boolean>> BY_SCHEMA =
        new ConcurrentHashMap<>();

    private AutoIncrementRegistry() {}

    private static ConcurrentHashMap<AString, Boolean> tablesFor(AString schemaName) {
        return BY_SCHEMA.computeIfAbsent(schemaName, k -> new ConcurrentHashMap<>());
    }

    /** Marks a table as having an auto-generating primary key. */
    public static void markAutoIncrement(AString schemaName, AString tableName) {
        tablesFor(schemaName).put(tableName, Boolean.TRUE);
    }

    /** Clears a table's auto-increment marking (e.g. on DROP TABLE, so a later re-CREATE of the same name doesn't inherit stale state). */
    public static void unmark(AString schemaName, AString tableName) {
        Map<AString, Boolean> tables = BY_SCHEMA.get(schemaName);
        if (tables != null) tables.remove(tableName);
    }

    /** Returns true if {@code tableName} in {@code schemaName} is currently marked auto-increment. */
    public static boolean isAutoIncrement(AString schemaName, AString tableName) {
        Map<AString, Boolean> tables = BY_SCHEMA.get(schemaName);
        return (tables != null) && Boolean.TRUE.equals(tables.get(tableName));
    }
}
