/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class provides various method for manipulating arrays.
 */
@SuppressWarnings("SwitchStatementWithTooFewBranches")
public final class ArrayUtils {
    /** Empty array of byte. */
    public static final byte[] BYTE_EMPTY_ARRAY = new byte[0];

    /** Empty array of short. */
    public static final short[] SHORT_EMPTY_ARRAY = new short[0];

    /** Empty array of int. */
    public static final int[] INT_EMPTY_ARRAY = new int[0];

    /** Empty array of long. */
    public static final long[] LONG_EMPTY_ARRAY = new long[0];

    /** Empty array of float. */
    public static final float[] FLOAT_EMPTY_ARRAY = new float[0];

    /** Empty array of double. */
    public static final double[] DOUBLE_EMPTY_ARRAY = new double[0];

    /** Empty array of char. */
    public static final char[] CHAR_EMPTY_ARRAY = new char[0];

    /** Empty array of boolean. */
    public static final boolean[] BOOLEAN_EMPTY_ARRAY = new boolean[0];

    /** Empty object array. */
    public static final Object[] OBJECT_EMPTY_ARRAY = new Object[0];

    /** Empty string array. */
    public static final String[] STRING_EMPTY_ARRAY = new String[0];

    /** {@code byte} array factory. */
    public static final ArrayFactory<byte[]> BYTE_ARRAY = len -> {
        if (len < 0) {
            throw new IgniteInternalException("Read invalid byte array length: " + len);
        }

        switch (len) {
            case 0:
                return BYTE_EMPTY_ARRAY;

            default:
                return new byte[len];
        }
    };

    /** {@code short} array factory. */
    public static final ArrayFactory<short[]> SHORT_ARRAY = len -> {
        if (len < 0) {
            throw new IgniteInternalException("Read invalid short array length: " + len);
        }

        switch (len) {
            case 0:
                return SHORT_EMPTY_ARRAY;

            default:
                return new short[len];
        }
    };

    /** {@code int} array factory. */
    public static final ArrayFactory<int[]> INT_ARRAY = len -> {
        if (len < 0) {
            throw new IgniteInternalException("Read invalid int array length: " + len);
        }

        switch (len) {
            case 0:
                return INT_EMPTY_ARRAY;

            default:
                return new int[len];
        }
    };

    /** {@code long} array factory. */
    public static final ArrayFactory<long[]> LONG_ARRAY = len -> {
        if (len < 0) {
            throw new IgniteInternalException("Read invalid long array length: " + len);
        }

        switch (len) {
            case 0:
                return LONG_EMPTY_ARRAY;

            default:
                return new long[len];
        }
    };

    /** {@code float} array factory. */
    public static final ArrayFactory<float[]> FLOAT_ARRAY = len -> {
        if (len < 0) {
            throw new IgniteInternalException("Read invalid float array length: " + len);
        }

        switch (len) {
            case 0:
                return FLOAT_EMPTY_ARRAY;

            default:
                return new float[len];
        }
    };

    /** {@code double} array factory. */
    public static final ArrayFactory<double[]> DOUBLE_ARRAY = len -> {
        if (len < 0) {
            throw new IgniteInternalException("Read invalid double array length: " + len);
        }

        switch (len) {
            case 0:
                return DOUBLE_EMPTY_ARRAY;

            default:
                return new double[len];
        }
    };

    /** {@code char} array factory. */
    public static final ArrayFactory<char[]> CHAR_ARRAY = len -> {
        if (len < 0) {
            throw new IgniteInternalException("Read invalid char array length: " + len);
        }

        switch (len) {
            case 0:
                return CHAR_EMPTY_ARRAY;

            default:
                return new char[len];
        }
    };

    /** {@code boolean} array factory. */
    public static final ArrayFactory<boolean[]> BOOLEAN_ARRAY = len -> {
        if (len < 0) {
            throw new IgniteInternalException("Read invalid boolean array length: " + len);
        }

        switch (len) {
            case 0:
                return BOOLEAN_EMPTY_ARRAY;

            default:
                return new boolean[len];
        }
    };

    /**
     * Returns {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     *
     * @param arr Array to check.
     * @param <T> Array element type.
     * @return {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     */
    public static <T> boolean nullOrEmpty(T @Nullable[] arr) {
        return arr == null || arr.length == 0;
    }

    /**
     * Returns {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     *
     * @param arr Array to check.
     * @return {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     */
    public static boolean nullOrEmpty(byte[] arr) {
        return arr == null || arr.length == 0;
    }

    /**
     * Returns {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     *
     * @param arr Array to check.
     * @return {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     */
    public static boolean nullOrEmpty(short[] arr) {
        return arr == null || arr.length == 0;
    }

    /**
     * Returns {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     *
     * @param arr Array to check.
     * @return {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     */
    public static boolean nullOrEmpty(int[] arr) {
        return arr == null || arr.length == 0;
    }

    /**
     * Returns {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     *
     * @param arr Array to check.
     * @return {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     */
    public static boolean nullOrEmpty(long[] arr) {
        return arr == null || arr.length == 0;
    }

    /**
     * Returns {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     *
     * @param arr Array to check.
     * @return {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     */
    public static boolean nullOrEmpty(float[] arr) {
        return arr == null || arr.length == 0;
    }

    /**
     * Returns {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     *
     * @param arr Array to check.
     * @return {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     */
    public static boolean nullOrEmpty(double[] arr) {
        return arr == null || arr.length == 0;
    }

    /**
     * Returns {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     *
     * @param arr Array to check.
     * @return {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     */
    public static boolean nullOrEmpty(boolean[] arr) {
        return arr == null || arr.length == 0;
    }

    /**
     * Converts array to {@link List}. Note that resulting list cannot be altered in size, as it it based on the passed in array - only
     * current elements can be changed.
     *
     * <p>Note that unlike {@link Arrays#asList(Object[])}, this method is {@code null}-safe. If {@code null} is passed in, then empty list
     * will be returned.
     *
     * @param vals Array of values.
     * @param <T>  Array type.
     * @return Unmodifiable {@link List} instance for array.
     */
    @SafeVarargs
    public static <T> List<T> asList(@Nullable T... vals) {
        return nullOrEmpty(vals) ? Collections.emptyList() : List.of(vals);
    }

    /**
     * Converts array to {@link Set}.
     *
     * <p>Note that unlike {@link Arrays#asList(Object[])}, this method is {@code null}-safe. If {@code null} is passed in, then empty set
     * will be returned.
     *
     * @param vals Array of values.
     * @param <T>  Array type.
     * @return Unmodifiable {@link Set} instance for input array.
     */
    @SafeVarargs
    public static <T> Set<T> asSet(@Nullable T... vals) {
        if (nullOrEmpty(vals)) {
            return Collections.emptySet();
        } else {
            return Set.of(vals);
        }
    }

    /**
     * Concatenates an elements to an array.
     *
     * @param arr Array.
     * @param obj One or more elements.
     * @param <T> Type of the elements of the array.
     * @return Concatenated array.
     */
    @SafeVarargs
    public static <T> T[] concat(@Nullable T[] arr, T... obj) {
        T[] newArr;

        if (arr == null || arr.length == 0) {
            newArr = obj;
        } else {
            newArr = Arrays.copyOf(arr, arr.length + obj.length);

            System.arraycopy(obj, 0, newArr, arr.length, obj.length);
        }

        return newArr;
    }

    /**
     * Concatenates an elements to an array.
     *
     * @param arr Array.
     * @param longs One or more elements.
     * @return Concatenated array.
     */
    public static long[] concat(@Nullable long[] arr, long... longs) {
        if (nullOrEmpty(arr)) {
            return longs;
        }

        long[] newArr = Arrays.copyOf(arr, arr.length + longs.length);

        System.arraycopy(longs, 0, newArr, arr.length, longs.length);

        return newArr;
    }

    /**
     * Concatenates elements to an array.
     *
     * @param arr Array.
     * @param bytes One or more elements.
     * @return Concatenated array.
     */
    public static byte[] concat(@Nullable byte[] arr, byte... bytes) {
        if (nullOrEmpty(arr)) {
            return bytes;
        }

        byte[] newArr = Arrays.copyOf(arr, arr.length + bytes.length);

        System.arraycopy(bytes, 0, newArr, arr.length, bytes.length);

        return newArr;
    }

    /**
     * Removes an element from an array with decrementing the array itself.
     *
     * @param arr Array.
     * @param idx Index to remove.
     * @return New array.
     */
    public static <T> T[] remove(T[] arr, int idx) {
        int len = arr.length;

        assert idx >= 0 && idx < len : idx + " < " + len;

        if (idx < len >>> 1) {
            T[] res = Arrays.copyOfRange(arr, 1, len);

            System.arraycopy(arr, 0, res, 0, idx);

            return res;
        } else {
            T[] res = Arrays.copyOf(arr, len - 1);

            System.arraycopy(arr, idx + 1, res, idx, len - idx - 1);

            return res;
        }
    }

    /**
     * Removes an element from an array with decrementing the array itself.
     *
     * @param arr Array.
     * @param idx Index to remove.
     * @return New array.
     */
    public static long[] remove(long[] arr, int idx) {
        int len = arr.length;

        assert idx >= 0 && idx < len : idx + " < " + len;

        if (idx < len >>> 1) {
            long[] res = Arrays.copyOfRange(arr, 1, len);

            System.arraycopy(arr, 0, res, 0, idx);

            return res;
        } else {
            long[] res = Arrays.copyOf(arr, len - 1);

            System.arraycopy(arr, idx + 1, res, idx, len - idx - 1);

            return res;
        }
    }

    /**
     * Set element to the array at the given index. Grows the array if needed.
     *
     * @param arr Array.
     * @param idx Index.
     * @param o Object.
     * @return The given or grown array.
     */
    public static <T> T[] set(T[] arr, int idx, T o) {
        int len = arr.length;

        if (idx >= len) {
            len += len >>> 1; // len *= 1.5
            len = Math.max(len, idx + 1);
            arr = Arrays.copyOf(arr, len);
        }

        arr[idx] = o;

        return arr;
    }

    /**
     * Nullify array elements from the given index until the first {@code null} element
     * (assuming that after the first {@code null} tail is already cleared).
     *
     * @param arr Array.
     * @param fromIdx From index (including).
     */
    public static void clearTail(Object[] arr, int fromIdx) {
        while (fromIdx < arr.length && arr[fromIdx] != null) {
            arr[fromIdx++] = null;
        }
    }
}
