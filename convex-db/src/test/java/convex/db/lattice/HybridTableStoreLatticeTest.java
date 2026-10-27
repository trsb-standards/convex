package convex.db.lattice;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import convex.core.data.ACell;
import convex.core.data.AString;
import convex.core.data.AVector;
import convex.core.data.Index;
import convex.core.data.Strings;
import convex.core.data.Vectors;
import convex.core.data.prim.CVMLong;

/**
 * Direct unit test for {@link HybridTableStoreLattice} — proves a single
 * {@code Index} can hold both a versioned and a non-versioned table entry,
 * each merging correctly per its own type, which {@code IndexLattice} (used
 * by {@link TableStoreLattice}/{@link VersionedTableStoreLattice}) cannot do
 * since it applies one single child lattice to every key.
 */
public class HybridTableStoreLatticeTest {

    private static final AString SCHEMA = Strings.create("hybrid_test_schema_" + System.nanoTime());
    private static final String PLAIN = "plain";
    private static final String VERSIONED = "versioned";

    @Test
    void mixedIndexMergesEachTablePerItsOwnVersionedStatus() {
        TableVersionRegistry.markVersioned(SCHEMA, Strings.create(VERSIONED));

        // "own" side: each table has row id=1
        SQLSchema plainOwnSchema = SQLSchema.create();
        plainOwnSchema.createTable(PLAIN, new String[]{"id"});
        plainOwnSchema.insert(PLAIN, Vectors.of(CVMLong.create(1L)));
        AVector<ACell> plainOwnState = plainOwnSchema.getTable(Strings.create(PLAIN)).getState();

        VersionedSQLSchema versionedOwnSchema = VersionedSQLSchema.create();
        versionedOwnSchema.createTable(VERSIONED, new String[]{"id"});
        versionedOwnSchema.insert(VERSIONED, Vectors.of(CVMLong.create(1L)));
        AVector<ACell> versionedOwnState = versionedOwnSchema.getTable(Strings.create(VERSIONED)).getState();

        // "other" side: each table has a DIFFERENT row id=2 (simulating a second node's write)
        SQLSchema plainOtherSchema = SQLSchema.create();
        plainOtherSchema.createTable(PLAIN, new String[]{"id"});
        plainOtherSchema.insert(PLAIN, Vectors.of(CVMLong.create(2L)));
        AVector<ACell> plainOtherState = plainOtherSchema.getTable(Strings.create(PLAIN)).getState();

        VersionedSQLSchema versionedOtherSchema = VersionedSQLSchema.create();
        versionedOtherSchema.createTable(VERSIONED, new String[]{"id"});
        versionedOtherSchema.insert(VERSIONED, Vectors.of(CVMLong.create(2L)));
        AVector<ACell> versionedOtherState = versionedOtherSchema.getTable(Strings.create(VERSIONED)).getState();

        Index<AString, AVector<ACell>> ownValue = Index.of(
            Strings.create(PLAIN), plainOwnState,
            Strings.create(VERSIONED), versionedOwnState);
        Index<AString, AVector<ACell>> otherValue = Index.of(
            Strings.create(PLAIN), plainOtherState,
            Strings.create(VERSIONED), versionedOtherState);

        HybridTableStoreLattice lattice = new HybridTableStoreLattice(SCHEMA);
        Index<AString, AVector<ACell>> merged = lattice.merge(ownValue, otherValue);

        // Plain table: union merge should give live count 2 (rows id=1 and id=2) --
        // proves the "plain" entry was merged via SQLTableLattice, not VersionedSQLTableLattice.
        AVector<ACell> mergedPlain = merged.get(Strings.create(PLAIN));
        assertEquals(2, SQLTable.getLiveCount(mergedPlain));

        // Versioned table: union merge should give 2 history entries (one from each side) --
        // proves the "versioned" entry was merged via VersionedSQLTableLattice, not SQLTableLattice.
        AVector<ACell> mergedVersioned = merged.get(Strings.create(VERSIONED));
        assertEquals(2, VersionedSQLTable.historyFrom(mergedVersioned).count());
    }
}
