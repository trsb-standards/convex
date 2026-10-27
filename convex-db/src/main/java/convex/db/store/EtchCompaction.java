package convex.db.store;

import java.io.File;
import java.io.IOException;

import convex.core.data.ACell;
import convex.etch.EtchStore;

/**
 * Compacts an Etch store by copying what is reachable from its root into a
 * fresh file. Everything else in the source file -- superseded versions of
 * cells that were rewritten -- is left behind.
 */
public final class EtchCompaction {

	private EtchCompaction() {}

	/**
	 * @param source store to compact; must have root data
	 * @param targetFile file for the compacted copy
	 * @return the compacted store, open
	 */
	public static EtchStore compact(EtchStore source, File targetFile) throws IOException {
		ACell root = source.getRootData();
		if (root == null) throw new IOException("Source store has no root data — call setRootData first");
		EtchStore target = EtchStore.create(targetFile);
		target.setRootData(root);
		return target;
	}
}
