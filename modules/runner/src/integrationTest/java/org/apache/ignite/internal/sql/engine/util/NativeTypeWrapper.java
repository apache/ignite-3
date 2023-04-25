package org.apache.ignite.internal.sql.engine.util;

import java.util.Arrays;
import org.apache.ignite.internal.schema.NativeTypes;

/**
 * A wrapper interface for non-comparable {@link NativeTypes native types} that implements {@link Comparable}.
 *
 * @param <T> a native type wrapper.
 */
public interface NativeTypeWrapper<T extends NativeTypeWrapper<T>> extends Comparable<T> {

    /** Returns a value of a native type. */
    Object get();

    /** If the given value is native type wrapper, unwraps it. Otherwise returns the same value. */
    static Object unwrap(Object value) {
        if (value instanceof NativeTypeWrapper) {
            return ((NativeTypeWrapper<?>) value).get();
        } else {
            return value;
        }
    }
}
