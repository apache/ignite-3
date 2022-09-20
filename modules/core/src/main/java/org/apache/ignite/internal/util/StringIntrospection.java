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

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Utils for introspecting a String efficiently.
 */
public class StringIntrospection {
    private static final boolean USE_UNSAFE_TO_GET_LATIN1_BYTES;
    private static final long STRING_CODER_FIELD_OFFSET;
    private static final long STRING_VALUE_FIELD_OFFSET;

    private static final byte LATIN1 = 0;

    private static final long NO_OFFSET = Long.MIN_VALUE;

    static {
        Optional<Boolean> maybeCompactStrings = compactStrings();
        Optional<Long> maybeCoderFieldOffset = coderFieldOffset();

        USE_UNSAFE_TO_GET_LATIN1_BYTES = maybeCompactStrings.isPresent() && maybeCoderFieldOffset.isPresent();
        STRING_CODER_FIELD_OFFSET = maybeCoderFieldOffset.orElse(NO_OFFSET);

        Optional<Long> maybeValueFieldOffset = byteValueFieldOffset();
        STRING_VALUE_FIELD_OFFSET = maybeValueFieldOffset.orElse(NO_OFFSET);
    }

    private static Optional<Boolean> compactStrings() {
        return compactStringsField()
                .map(field -> {
                    try {
                        return (Boolean) field.get(null);
                    } catch (IllegalAccessException e) {
                        throw new IllegalStateException("Should not be thrown", e);
                    }
                });
    }

    private static Optional<Field> compactStringsField() {
        return stringField("COMPACT_STRINGS")
                .map(field -> {
                    field.setAccessible(true);
                    return field;
                });
    }

    private static Optional<Field> stringField(String name) {
        try {
            return Optional.of(String.class.getDeclaredField(name));
        } catch (NoSuchFieldException e) {
            return Optional.empty();
        }
    }

    private static Optional<Long> coderFieldOffset() {
        return stringField("coder").map(GridUnsafe::objectFieldOffset);
    }

    private static Optional<Long> byteValueFieldOffset() {
        return stringField("value")
                .filter(field -> field.getType() == byte[].class)
                .map(GridUnsafe::objectFieldOffset);
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
        return GridUnsafe.getByteField(str, STRING_CODER_FIELD_OFFSET) == LATIN1;
    }

    /**
     * Returns a byte array with ASCII representation of the string. This *ONLY* returns a meaningful result
     * if each string character fits ASCII encoding (that is, its code is less than 128)!
     * This may return the internal buffer of the string, so *THE ARRAY CONTENTS SHOULD NEVER BE MODIFIED!*.
     * The method is 'fast' because in the current Hotspot JVMs (versions 9+) it avoids copying string bytes (as well
     * as encoding/decoding), it just returns the internal String buffer.
     *
     * @param str string to work with
     * @return byte represenation of an ASCII string
     */
    public static byte[] fastAsciiBytes(String str) {
        if (STRING_VALUE_FIELD_OFFSET != NO_OFFSET) {
            return (byte[]) GridUnsafe.getObjectField(str, STRING_VALUE_FIELD_OFFSET);
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
     * @return byte represenation of a Latin1 string
     */
    public static byte[] fastLatin1Bytes(String str) {
        if (STRING_VALUE_FIELD_OFFSET != NO_OFFSET) {
            return (byte[]) GridUnsafe.getObjectField(str, STRING_VALUE_FIELD_OFFSET);
        } else {
            // Fallback: something is different, let's not fail here, just pay performance penalty.
            return str.getBytes(StandardCharsets.ISO_8859_1);
        }
    }

    private StringIntrospection() {
    }
}
