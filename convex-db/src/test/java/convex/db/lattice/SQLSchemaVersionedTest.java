package convex.db.lattice;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import convex.core.data.ACell;
import convex.core.data.AVector;
import convex.core.data.Vectors;
import convex.core.data.prim.CVMLong;

/**
 * Covers the actual point of this feature: a single {@link SQLSchema}
 * instance holding both a versioned and a non-versioned table side by side,
 * plus {@link SQLSchema#convertToVersioned} for turning an existing plain
 * table into a versioned one in place.
 */
class SQLSchemaVersionedTest {

    @Test
    void oneSchemaHoldsBothVersionedAndPlainTablesIndependently() {
        SQLSchema schema = SQLSchema.create();
        schema.createTable("plain", new String[]{"id", "name"}, false);
        schema.createTable("versioned", new String[]{"id", "name"}, true);

        schema.insert("plain", Vectors.of(CVMLong.create(1L), "alpha"));
        schema.insert("versioned", Vectors.of(CVMLong.create(1L), "alpha"));
        schema.insert("versioned", Vectors.of(CVMLong.create(1L), "beta")); // update

        // Plain table: no history at all -- getHistory on a non-versioned table is empty, not an error.
        assertTrue(schema.getHistory("plain", CVMLong.create(1L)).isEmpty());

        // Versioned table: two recorded changes (insert then update).
        List<AVector<ACell>> history = schema.getHistory("versioned", CVMLong.create(1L));
        assertEquals(2, history.size());

        // Both tables' live data is independently correct.
        assertEquals("alpha", schema.selectByKey("plain", CVMLong.create(1L)).get(1).toString());
        assertEquals("beta", schema.selectByKey("versioned", CVMLong.create(1L)).get(1).toString());
    }

    @Test
    void convertToVersionedPreservesExistingRowsWithNoBackfilledHistory() {
        SQLSchema schema = SQLSchema.create();
        schema.createTable("t", new String[]{"id", "name"}, false);
        schema.insert("t", Vectors.of(CVMLong.create(1L), "alpha"));
        schema.insert("t", Vectors.of(CVMLong.create(2L), "beta"));

        boolean converted = schema.convertToVersioned("t");
        assertTrue(converted);

        // Existing rows survive the conversion unchanged.
        assertEquals("alpha", schema.selectByKey("t", CVMLong.create(1L)).get(1).toString());
        assertEquals("beta", schema.selectByKey("t", CVMLong.create(2L)).get(1).toString());

        // No history was backfilled for the pre-conversion writes.
        assertTrue(schema.getHistory("t", CVMLong.create(1L)).isEmpty());

        // A write from this point onward IS tracked.
        schema.insert("t", Vectors.of(CVMLong.create(1L), "alpha-v2"));
        List<AVector<ACell>> history = schema.getHistory("t", CVMLong.create(1L));
        assertEquals(1, history.size(), "only the post-conversion write should be recorded");
    }

    @Test
    void convertToVersionedIsANoOpWhenAlreadyVersioned() {
        SQLSchema schema = SQLSchema.create();
        schema.createTable("t", new String[]{"id"}, true);
        schema.insert("t", Vectors.of(CVMLong.create(1L)));

        assertTrue(schema.convertToVersioned("t"));
        // Still versioned, history from before the (no-op) conversion call intact.
        assertEquals(1, schema.getHistory("t", CVMLong.create(1L)).size());
    }

    @Test
    void convertToVersionedReturnsFalseForAMissingTable() {
        SQLSchema schema = SQLSchema.create();
        assertEquals(false, schema.convertToVersioned("nope"));
    }

    @Test
    void versionedTableRejectsCompositePrimaryKeyAtCreation() {
        SQLSchema schema = SQLSchema.create();
        assertThrows(UnsupportedOperationException.class, () ->
            schema.createTable(convex.core.data.Strings.create("t"),
                new String[]{"a", "b"},
                new convex.db.calcite.ConvexColumnType[]{
                    convex.db.calcite.ConvexColumnType.of(convex.db.calcite.ConvexType.INTEGER),
                    convex.db.calcite.ConvexColumnType.of(convex.db.calcite.ConvexType.INTEGER)},
                2, true));
    }

    @Test
    void convertToVersionedRejectsCompositePrimaryKeyTable() {
        SQLSchema schema = SQLSchema.create();
        schema.createTable(convex.core.data.Strings.create("t"),
            new String[]{"a", "b"},
            new convex.db.calcite.ConvexColumnType[]{
                convex.db.calcite.ConvexColumnType.of(convex.db.calcite.ConvexType.INTEGER),
                convex.db.calcite.ConvexColumnType.of(convex.db.calcite.ConvexType.INTEGER)},
            2, false);

        assertThrows(UnsupportedOperationException.class, () -> schema.convertToVersioned("t"));
    }

    @Test
    void getAsOfReturnsNullForNonVersionedTable() {
        SQLSchema schema = SQLSchema.create();
        schema.createTable("t", new String[]{"id"}, false);
        schema.insert("t", Vectors.of(CVMLong.create(1L)));
        assertNull(schema.getAsOf("t", CVMLong.create(1L), System.currentTimeMillis()));
    }

    @Test
    void getAsOfWorksForVersionedTableCreatedOnPlainSchema() {
        SQLSchema schema = SQLSchema.create();
        schema.createTable("t", new String[]{"id"}, true);
        schema.insert("t", Vectors.of(CVMLong.create(1L)));
        // Derived from the actual recorded writeSeq, not an external clock
        // capture -- writeSeq is HLC-style (see VersionedSQLTable.nextHistorySeq),
        // not comparable to a raw System.nanoTime() snapshot.
        long afterInsert = VersionedSQLTable.getHistoryWriteSeq(schema.getHistory("t", CVMLong.create(1L)).get(0));

        AVector<ACell> asOf = schema.getAsOf("t", CVMLong.create(1L), afterInsert);
        assertNotNull(asOf);
    }
}
