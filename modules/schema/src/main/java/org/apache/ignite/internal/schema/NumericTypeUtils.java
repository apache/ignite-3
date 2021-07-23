package org.apache.ignite.internal.schema;

/**
 *
 * */
public class NumericTypeUtils {
    /** */
    public static int byteSizeByPrecision(int precision) {
        long numBits = ((precision * 3402L) >>> 10) + 1;
        return (int)(numBits / 8 + 1);
    }
}
