package convex.db.lattice;

import java.util.concurrent.ConcurrentHashMap;

import convex.core.data.AString;

/**
 * Process-wide registry of the owner given to a row that is written into a
 * schema without one — for catalog schemas, whose rows are written by the
 * server itself (through the Java API, where no connection's entity stamps
 * {@link SQLSchema#OWNERID_COLUMN}). Keyed by the schema's registry key, like
 * {@link TableVersionRegistry}. A schema with no entry gets no default.
 *
 * <p>Applied by {@code SQLSchema}'s insert paths: an empty OWNERID becomes the
 * owner already stored for that key, or this default for a new row.
 */
public final class DefaultOwnerRegistry {

    private static final ConcurrentHashMap<AString, Long> BY_SCHEMA = new ConcurrentHashMap<>();

    private DefaultOwnerRegistry() {}

    /** Rows written into this schema without an owner get {@code owner}. */
    public static void set(AString schemaKey, long owner) {
        BY_SCHEMA.put(schemaKey, owner);
    }

    public static void clear(AString schemaKey) {
        BY_SCHEMA.remove(schemaKey);
    }

    /** The default owner of the schema, or null if it has none. */
    public static Long get(AString schemaKey) {
        return (schemaKey == null) ? null : BY_SCHEMA.get(schemaKey);
    }
}
