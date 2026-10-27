package convex.db.calcite;

import java.lang.reflect.Method;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import convex.db.CurrentEntity;

/**
 * SQL-callable functions specific to this driver, registered on every {@link
 * ConvexSchema} instance (see {@link ConvexSchema#getFunctionNames()}/{@link
 * ConvexSchema#getFunctions(String)}) so an unqualified call resolves
 * correctly regardless of which mounted database is the connection's current
 * default schema — this driver mounts every registered database as a sibling
 * schema on one Calcite connection, so the function has to be reachable from
 * every one of them, not just a single "root" schema.
 */
public final class ConvexFunctions {

	private ConvexFunctions() {}

	/** SQL-visible name for {@link #currentEntity()}. */
	public static final String CURRENT_ENTITY_NAME = "current_entity";

	public static final ScalarFunction CURRENT_ENTITY_FUNCTION;
	static {
		try {
			Method m = ConvexFunctions.class.getMethod("currentEntity");
			CURRENT_ENTITY_FUNCTION = ScalarFunctionImpl.create(m);
		} catch (NoSuchMethodException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	/**
	 * Backing implementation for the SQL {@code current_entity()} function —
	 * this connection's resolved entity id (see {@link CurrentEntity}), or
	 * {@code null} if this connection didn't authenticate via UCAN (trust/
	 * no-password connections, including every direct {@code jdbc:convex:}
	 * caller).
	 *
	 * <p>Must be {@code public static} with no arguments for {@link
	 * ScalarFunctionImpl#create(Method)} to accept it as a zero-arg scalar
	 * function.
	 */
	public static Long currentEntity() {
		return CurrentEntity.get();
	}
}
