package org.apache.ignite.internal.sql.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.plan.Context;
import org.apache.ignite.internal.util.ArrayUtils;

/**
 * User query context.
 * */
public class QueryContext implements Context {
    /** Context params. */
    private final Object[] params;

    /**
     * Constructor.
     *
     * @param params Context params.
     * */
    private QueryContext(Object[] params) {
        this.params = params;
    }

    /** {@inheritDoc} */
    @Override
    public <C> C unwrap(Class<C> cls) {
        if (Object[].class == cls) {
            return cls.cast(params);
        }

        return Arrays.stream(params).filter(cls::isInstance).findFirst().map(cls::cast).orElse(null);
    }

    /**
     * Creates a context from a list of parameters.
     *
     * @param params Context parameters.
     * @return Query context.
     */
    public static QueryContext of(Object... params) {
        return !ArrayUtils.nullOrEmpty(params) ? new QueryContext(build(null, params).toArray())
                : new QueryContext(ArrayUtils.OBJECT_EMPTY_ARRAY);
    }

    private static List<Object> build(List<Object> dst, Object[] src) {
        if (dst == null) {
            dst = new ArrayList<>();
        }

        for (Object obj : src) {
            if (obj == null) {
                continue;
            }

            if (obj.getClass() == QueryContext.class) {
                build(dst, ((QueryContext) obj).params);
            } else {
                dst.add(obj);
            }
        }

        return dst;
    }
}
