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

package org.apache.ignite.internal.schema;

import static java.lang.Integer.signum;
import static org.apache.ignite.internal.binarytuple.BinaryTupleCommon.EQUALITY_FLAG;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.sql.ColumnType;

/**
 * The utility class has methods to use to compare fields in binary representation.
 */
class BinaryTupleComparatorUtils {
    /**
     * Compares individual fields of two tuples using ascending order.
     */
    @SuppressWarnings("DataFlowIssue")
    static int compareFieldValue(ColumnType typeSpec, BinaryTupleReader tuple1, BinaryTupleReader tuple2, int index) {
        switch (typeSpec) {
            case INT8:
            case BOOLEAN:
                return Byte.compare(tuple1.byteValue(index), tuple2.byteValue(index));

            case INT16:
                return Short.compare(tuple1.shortValue(index), tuple2.shortValue(index));

            case INT32:
                return Integer.compare(tuple1.intValue(index), tuple2.intValue(index));

            case INT64:
                return Long.compare(tuple1.longValue(index), tuple2.longValue(index));

            case FLOAT:
                return Float.compare(tuple1.floatValue(index), tuple2.floatValue(index));

            case DOUBLE:
                return Double.compare(tuple1.doubleValue(index), tuple2.doubleValue(index));

            case BYTE_ARRAY:
                return Arrays.compareUnsigned(tuple1.bytesValue(index), tuple2.bytesValue(index));

            case UUID:
                return tuple1.uuidValue(index).compareTo(tuple2.uuidValue(index));

            case STRING:
                return tuple1.stringValue(index).compareTo(tuple2.stringValue(index));

            case DECIMAL:
                BigDecimal numeric1 = tuple1.decimalValue(index, Integer.MIN_VALUE);
                BigDecimal numeric2 = tuple2.decimalValue(index, Integer.MIN_VALUE);

                return numeric1.compareTo(numeric2);

            case TIMESTAMP:
                return tuple1.timestampValue(index).compareTo(tuple2.timestampValue(index));

            case DATE:
                return tuple1.dateValue(index).compareTo(tuple2.dateValue(index));

            case TIME:
                return tuple1.timeValue(index).compareTo(tuple2.timeValue(index));

            case DATETIME:
                return tuple1.dateTimeValue(index).compareTo(tuple2.dateTimeValue(index));

            default:
                throw new IllegalArgumentException(format("Unsupported column type in binary tuple comparator. [type={}]", typeSpec));
        }
    }

    static boolean isFlagSet(ByteBuffer tuple, int flag) {
        return (tuple.get(0) & flag) != 0;
    }

    static int equalityFlag(ByteBuffer tuple) {
        return isFlagSet(tuple, EQUALITY_FLAG) ? 1 : -1;
    }

    /**
     * Compares a value in a binary tuple, interpreted as a string, with a given string.
     * The comparison can be performed as case-sensitive or case-insensitive.
     * The method first attempts a fast comparison for ASCII sequences and falls back
     * to Unicode comparison if non-ASCII characters are detected.
     *
     * @param tuple The BinaryTupleReader containing the tuple to be compared.
     * @param colIndex The column index in the tuple to retrieve the value for comparison.
     * @param cmp The string to compare the value in the tuple against.
     * @param ignoreCase Flag indicating whether the comparison should ignore case differences.
     * @return 0 if the strings are equal, a negative value if the tuple string is lexicographically
     *         less than the given string, or a positive value if it is greater.
     */
    static int compareAsString(BinaryTupleReader tuple, int colIndex, String cmp, boolean ignoreCase) {
        tuple.seek(colIndex);
        int begin = tuple.begin();
        int end = tuple.end();

        ByteBuffer buf = tuple.byteBuffer();
        int fullStrLength = end - begin;
        int trimmedSize = Math.min(fullStrLength, buf.capacity() - begin);

        // Copying the direct byte buffer and then accessing it is better for performance than comparing with access by index in the buffer.
        byte[] bytes = tuple.bytesValue(begin, begin + trimmedSize);
        char[] cmpArray = cmp.toCharArray();

        // The tuple can contain a specific character (VARLEN_EMPTY_BYTE) that is not a part of the value.
        // In that case the value size in bytes (fullStrLength) should be reduced by 1.
        if (bytes.length < trimmedSize) {
            assert bytes.length == trimmedSize - 1 : "Only one first byte can have a special value.";

            fullStrLength--;
        }

        // Fast pass for ASCII string.
        int asciiResult = compareAsciiSequences(
                bytes,
                fullStrLength,
                cmpArray,
                ignoreCase
        );

        if (asciiResult != Integer.MIN_VALUE) {
            return asciiResult;
        }

        // If the string contains non-ASCII characters, we compare it as a Unicode string.
        return fullUnicodeCompare(
                bytes,
                fullStrLength,
                cmpArray,
                ignoreCase
        );
    }

    /**
     * Compares a UTF-8 encoded byte array with a character array, optionally ignoring case differences.
     * The comparison performs a Unicode-aware lexicographical comparison.
     *
     * @param bytes The byte array containing a UTF-8 encoded string.
     * @param fullStrLength The full length of the string, which had been truncated in the byte array.
     * @param cmpArray The character array to compare against.
     * @param ignoreCase A flag indicating whether the comparison should ignore case differences.
     * @return 0 if the strings are equal, a negative value if the byte array represents a string that is
     *         lexicographically less than the character array, or a positive value if it is greater. Returns
     *         0 if the string comparison is incomplete due to truncation in either of the arrays.
     */
    private static int fullUnicodeCompare(
            byte[] bytes,
            int fullStrLength,
            char[] cmpArray,
            boolean ignoreCase
    ) {
        int[] idx = {0};
        int i = 0;

        while (idx[0] < bytes.length && i < cmpArray.length) {
            int cp = getNextCodePoint(bytes, idx);

            if (cp == -1) {
                // Comparison is impossible because the string is truncated.
                return 0;
            }

            char v1 = (char) cp;
            char v2 = cmpArray[i];
            i++;

            if (v1 != v2) {
                if (ignoreCase) {
                    char upper1 = Character.toUpperCase(v1);
                    char upper2 = Character.toUpperCase(v2);
                    if (upper1 != upper2) {
                        return signum(upper1 - upper2);
                    }
                } else {
                    return signum(v1 - v2);
                }
            }
        }

        if (fullStrLength > bytes.length && cmpArray.length > i) {
            // Comparison is not completed yet. Both strings have more characters.
            return 0;
        }

        return fullStrLength == bytes.length && cmpArray.length == i ? 0 : fullStrLength == bytes.length ? -1 : 1;
    }

    /**
     * Decodes the next UTF-8 code point from a byte array, updating the index reference to the next position.
     * The method validates the UTF-8 byte sequence and throws an exception if it encounters an invalid sequence.
     *
     * @param bytes The byte array containing the UTF-8 encoded data.
     * @param idx An array containing the current index position; the index will be updated to point
     *            to the next position after reading the code point.
     * @return The decoded code point, or -1 if the end of the array is reached before completing a valid sequence.
     * @throws IllegalArgumentException If an invalid UTF-8 sequence is encountered.
     */
    private static int getNextCodePoint(byte[] bytes, int[] idx) {
        byte b1 = bytes[idx[0]];
        idx[0]++;

        if ((b1 & 0x80) == 0) {
            return b1; // ASCII
        }

        int remainingBytes;
        if ((b1 & 0xE0) == 0xC0) {
            remainingBytes = 1;
        } else if ((b1 & 0xF0) == 0xE0) {
            remainingBytes = 2;
        } else if ((b1 & 0xF8) == 0xF0) {
            remainingBytes = 3;
        } else {
            throw new IllegalArgumentException("Invalid UTF-8");
        }

        if (idx[0] + remainingBytes > bytes.length) {
            return -1;
        }

        int codePoint = b1 & (0x3F >> remainingBytes);
        for (int i = 0; i < remainingBytes; i++) {
            byte nextByte = bytes[idx[0]];
            idx[0]++;

            if ((nextByte & 0xC0) != 0x80) {
                throw new IllegalArgumentException("Invalid UTF-8 continuation");
            }
            codePoint = (codePoint << 6) | (nextByte & 0x3F);
        }

        return codePoint;
    }

    /**
     * Compares an ASCII sequence encoded in a byte array with a character array,
     * optionally ignoring case differences. The method assumes that the byte
     * array contains only valid ASCII characters. If non-ASCII characters are
     * detected, the method returns a special error value {@link Integer.MIN_VALUE}.
     * The comparison is performed lexicographically.
     *
     * @param bytes The byte array representation of the ASCII sequence to be compared.
     * @param fullStrLength The full length of the string, which had been truncated in the byte array.
     * @param cmpArray The character array to compare against the byte array content.
     * @param ignoreCase Flag indicating whether the comparison should be case-insensitive.
     * @return A negative value if the byte array sequence is lexicographically less
     *         than the character array, 0 if they are equal, or a positive value if
     *         the byte array sequence is greater. Returns {@link Integer.MIN_VALUE} if the
     *         byte array contains non-ASCII characters.
     */
    private static int compareAsciiSequences(
            byte[] bytes,
            int fullStrLength,
            char[] cmpArray,
            boolean ignoreCase
    ) {
        int i = 0;
        int remaining = Math.min(cmpArray.length, bytes.length);

        while (i < remaining) {
            byte b = bytes[i];

            // Checking if it is an ASCII character.
            if ((b & 0x80) != 0) {
                return Integer.MIN_VALUE;
            }

            char v1 = (char) b;
            char v2 = cmpArray[i];
            i++;

            if (v1 != v2) {
                if (ignoreCase) {
                    char upper1 = Character.toUpperCase(v1);
                    char upper2 = Character.toUpperCase(v2);
                    if (upper1 != upper2) {
                        return signum(upper1 - upper2);
                    }
                } else {
                    return signum(v1 - v2);
                }
            }
        }

        if (fullStrLength > remaining && cmpArray.length > remaining) {
            // Comparison is not completed yet. Both strings have more characters.
            return 0;
        }

        return signum(fullStrLength - cmpArray.length);
    }
}
