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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.jetbrains.annotations.Nullable;

/**
 * Utils for introspecting a String efficiently.
 */
public class StringIntrospection {
    private static final boolean USE_UNSAFE_TO_GET_LATIN1_BYTES;

    private static final VarHandle STRING_CODER;
    private static final VarHandle STRING_VALUE;

    private static final MethodHandle HAS_NEGATIVES;

    private static final byte LATIN1 = 0;

    static {
        Optional<Boolean> maybeCompactStrings = compactStrings();

        STRING_CODER = privateStringHandle("coder");
        STRING_VALUE = privateStringHandle("value");

        MethodHandle hasNegatives;
        try {
            Class<?> stringCodingClass = Class.forName("java.lang.StringCoding");

            hasNegatives = MethodHandles.privateLookupIn(stringCodingClass, MethodHandles.lookup()).findStatic(
                    stringCodingClass,
                    "hasNegatives",
                    MethodType.methodType(boolean.class, new Class[]{byte[].class, int.class, int.class})
            );
        } catch (Exception e) {
            hasNegatives = null;
        }

        HAS_NEGATIVES = hasNegatives;

        USE_UNSAFE_TO_GET_LATIN1_BYTES = maybeCompactStrings.orElse(false) && STRING_CODER != null && STRING_VALUE != null;
    }

    private static Optional<Boolean> compactStrings() {
        return stringField("COMPACT_STRINGS")
                .map(field -> {
                    try {
                        return (Boolean) field.get(null);
                    } catch (IllegalAccessException e) {
                        throw new IllegalStateException("Should not be thrown", e);
                    }
                });
    }

    private static @Nullable VarHandle privateStringHandle(String name) {
        return stringField(name).map(field -> {
            try {
                return MethodHandles.privateLookupIn(String.class, MethodHandles.lookup()).unreflectVarHandle(field);
            } catch (IllegalAccessException e) {
                return null;
            }
        }).orElse(null);
    }

    private static Optional<Field> stringField(String name) {
        try {
            return Optional.of(String.class.getDeclaredField(name))
                    .map(field -> {
                        field.setAccessible(true);
                        return field;
                    });
        } catch (NoSuchFieldException e) {
            return Optional.empty();
        }
    }

    /**
     * Returns {@code true} if the current String is represented as Latin1 internally AND we can get access to that
     * representation fast.
     *
     * @param str   the string to check
     * @return {@code true} if the current String is represented as Latin1 internally AND we can get access to that
     *     representation fast
     */
    public static boolean supportsFastGetLatin1Bytes(String str) {
        if (!USE_UNSAFE_TO_GET_LATIN1_BYTES) {
            return false;
        }
        return (byte) STRING_CODER.get(str) == LATIN1;
    }

    /**
     * Returns a byte array with ASCII representation of the string. This *ONLY* returns a meaningful result
     * if each string character fits ASCII encoding (that is, its code is less than 128)!
     * This may return the internal buffer of the string, so *THE ARRAY CONTENTS SHOULD NEVER BE MODIFIED!*.
     * The method is 'fast' because in the current Hotspot JVMs (versions 9+) it avoids copying string bytes (as well
     * as encoding/decoding), it just returns the internal String buffer.
     *
     * @param str string to work with
     * @return byte representation of an ASCII string
     */
    public static byte[] fastAsciiBytes(String str) {
        if (STRING_VALUE != null) {
            return (byte[]) STRING_VALUE.get(str);
        } else {
            // Fallback: something is different, let's not fail here, just pay a performance penalty.
            return str.getBytes(StandardCharsets.US_ASCII);
        }
    }

    /**
     * Returns a byte array with Latin1 representation of the string. This *ONLY* returns a meaningful result
     * if each string character fits Latin1 encoding!
     * This may return the internal buffer of the string, so *THE ARRAY CONTENTS SHOULD NEVER BE MODIFIED!*.
     * The method is 'fast' because in the current Hotspot JVMs (versions 9+) it avoids copying string bytes (as well
     * as encoding/decoding), it just returns the internal String buffer.
     *
     * @param str string to work with
     * @return byte representation of a Latin1 string
     */
    public static byte[] fastLatin1Bytes(String str) {
        if (STRING_VALUE != null) {
            return (byte[]) STRING_VALUE.get(str);
        } else {
            // Fallback: something is different, let's not fail here, just pay performance penalty.
            return str.getBytes(StandardCharsets.ISO_8859_1);
        }
    }

    /**
     * Checks if given array has any negative bytes in it.
     */
    public static boolean hasNegatives(byte[] bytes) {
        try {
            if (HAS_NEGATIVES != null) {
                // Must be "invokeExact", everything else is slow.
                return (boolean) HAS_NEGATIVES.invokeExact(bytes, 0, bytes.length);
            }
        } catch (Throwable ignore) {
            // No-op.
        }

        // Fallback algorithm if there's an exception or if method handle is missing.
        int length = bytes.length;
        int off = 0;

        // SWAR optimization to check 8 bytes at once.
        for (int limit = length & -Long.BYTES; off < limit; off += Long.BYTES) {
            long v = GridUnsafe.getLong(bytes, GridUnsafe.BYTE_ARR_OFF + off);

            if ((v & 0x8080808080808080L) != 0L) {
                return true;
            }
        }

        for (; off < length; off++) {
            if (bytes[off] < 0) {
                return true;
            }
        }

        return false;
    }

    private StringIntrospection() {
    }
}
