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
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.binarytuple.ByteBufferAccessor;
import org.apache.ignite.sql.ColumnType;

/**
 * The utility class has methods to use to compare fields in binary representation.
 */
public class BinaryTupleComparatorUtils {
    /**
     * Compares individual fields of two tuples using ascending order.
     */
    @SuppressWarnings("DataFlowIssue")
    static int compareFieldValue(
            ColumnType typeSpec,
            BinaryTupleReader tuple1,
            BinaryTupleReader tuple2,
            int index
    ) {
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
                return compareAsBytes(tuple1, tuple2, index);

            case UUID:
                return compareAsUuid(tuple1, tuple2, index);

            case STRING:
                return compareAsString(tuple1, tuple2, index);

            case DECIMAL:
                BigDecimal numeric1 = tuple1.decimalValue(index, Integer.MIN_VALUE);
                BigDecimal numeric2 = tuple2.decimalValue(index, Integer.MIN_VALUE);

                return numeric1.compareTo(numeric2);

            case TIMESTAMP:
                return compareAsTimestamp(tuple1, tuple2, index);

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

    public static boolean isFlagSet(ByteBuffer tuple, int flag) {
        return (tuple.get(0) & flag) != 0;
    }

    static int equalityFlag(ByteBuffer tuple) {
        return isFlagSet(tuple, EQUALITY_FLAG) ? 1 : -1;
    }

    /**
     * Compares two binary tuples as timestamps. The comparison is performed on the column
     * specified by the given index. The column values in the tuples are expected to represent
     * timestamps encoded as seconds and optionally nanoseconds. The method considers both the
     * second and nanosecond parts for comparison and accounts for potential truncation of data.
     *
     * @param tuple1 the first binary tuple reader
     * @param tuple2 the second binary tuple reader
     * @param colIndex the index of the column in the tuple to compare
     * @return a negative integer, zero, or a positive integer if the first tuple is less than,
     *         equal to, or greater than the second tuple, respectively, when interpreted as timestamps
     */
    static int compareAsTimestamp(BinaryTupleReader tuple1, BinaryTupleReader tuple2, int colIndex) {
        tuple1.seek(colIndex);
        int begin1 = tuple1.begin();
        int end1 = tuple1.end();

        tuple2.seek(colIndex);
        int begin2 = tuple2.begin();
        int end2 = tuple2.end();

        return compareAsTimestamp(tuple1.accessor(), begin1, end1, tuple2.accessor(), begin2, end2);
    }

    /**
     * Compares timestamp values of two binary tuples.
     *
     * @param buf1 Buffer accessor for the first tuple.
     * @param begin1 Begin position in the first tuple.
     * @param end1 End position in the first tuple.
     * @param buf2 Buffer accessor for the second tuple.
     * @param begin2 Begin position in the second tuple.
     * @param end2 End position in the second tuple.
     * @return Comparison result.
     *
     * @see #compareAsTimestamp(BinaryTupleReader, BinaryTupleReader, int)
     */
    public static int compareAsTimestamp(ByteBufferAccessor buf1, int begin1, int end1, ByteBufferAccessor buf2, int begin2, int end2) {
        int fullSize1 = end1 - begin1;
        int trimmedSize1 = Math.min(fullSize1, buf1.capacity() - begin1);

        int fullSize2 = end2 - begin2;
        int trimmedSize2 = Math.min(fullSize2, buf2.capacity() - begin2);

        int remaining = Math.min(trimmedSize1, trimmedSize2);

        if (remaining >= 8) {
            long seconds1 = buf1.getLong(begin1);
            long seconds2 = buf2.getLong(begin2);

            int cmp = Long.compare(seconds1, seconds2);

            if (cmp != 0) {
                return cmp;
            }

            if (remaining == 12) {
                int nanos1 = buf1.getInt(begin1 + 8);
                int nanos2 = buf2.getInt(begin2 + 8);

                return nanos1 - nanos2;
            }

            if (fullSize1 == 8 && fullSize2 == 12) {
                return -1;
            }

            if (fullSize1 == 12 && fullSize2 == 8) {
                return 1;
            }
        }

        return 0;
    }

    /**
     * Compares two binary tuples as UUIDs. The comparison is performed on the column
     * specified by the given index. The column values in the tuples are expected to
     * represent UUIDs encoded as two sequential 64-bit values (most significant bits
     * and least significant bits).
     *
     * @param tuple1 the first binary tuple reader
     * @param tuple2 the second binary tuple reader
     * @param colIndex the index of the column in the tuple to compare
     * @return a negative integer, zero, or a positive integer if the first tuple is less than,
     *         equal to, or greater than the second tuple, respectively, when interpreted as UUIDs
     */
    static int compareAsUuid(BinaryTupleReader tuple1, BinaryTupleReader tuple2, int colIndex) {
        tuple1.seek(colIndex);
        int begin1 = tuple1.begin();

        tuple2.seek(colIndex);
        int begin2 = tuple2.begin();

        return compareAsUuid(tuple1.accessor(), begin1, tuple2.accessor(), begin2);
    }

    /**
     * Compares UUID values of two binary tuples.
     *
     * @param buf1 Buffer accessor for the first tuple.
     * @param begin1 Begin position in the first tuple.
     * @param buf2 Buffer accessor for the second tuple.
     * @param begin2 Begin position in the second tuple.
     * @return Comparison result.
     *
     * @see #compareAsUuid(BinaryTupleReader, BinaryTupleReader, int)
     */
    public static int compareAsUuid(ByteBufferAccessor buf1, int begin1, ByteBufferAccessor buf2, int begin2) {
        int trimmedSize1 = Math.min(16, buf1.capacity() - begin1);

        int trimmedSize2 = Math.min(16, buf2.capacity() - begin2);

        int remaining = Math.min(trimmedSize1, trimmedSize2);

        if (remaining >= 8) {
            long msb1 = buf1.getLong(begin1);
            long msb2 = buf2.getLong(begin2);

            int cmp = Long.compare(msb1, msb2);

            if (cmp != 0) {
                return cmp;
            }

            if (remaining == 16) {
                long lsb1 = buf1.getLong(begin1 + 8);
                long lsb2 = buf2.getLong(begin2 + 8);

                return Long.compare(lsb1, lsb2);
            }
        }

        return 0;
    }

    /**
     * Compares two binary tuples as byte sequences. The comparison is performed on the column
     * specified by the given index.
     *
     * @param tuple1 the first binary tuple reader
     * @param tuple2 the second binary tuple reader
     * @param colIndex the index of the column in the tuple to compare
     * @return a negative integer, zero, or a positive integer if the first tuple is less than, equal to,
     *         or greater than the second tuple, respectively
     */
    static int compareAsBytes(BinaryTupleReader tuple1, BinaryTupleReader tuple2, int colIndex) {
        tuple1.seek(colIndex);
        int begin1 = tuple1.begin();
        int end1 = tuple1.end();

        tuple2.seek(colIndex);
        int begin2 = tuple2.begin();
        int end2 = tuple2.end();

        return compareAsBytes(tuple1.accessor(), begin1, end1, tuple2.accessor(), begin2, end2);
    }

    /**
     * Compares {@code byte[]} values of two binary tuples.
     *
     * @param buf1 Buffer accessor for the first tuple.
     * @param begin1 Begin position in the first tuple.
     * @param end1 End position in the first tuple.
     * @param buf2 Buffer accessor for the second tuple.
     * @param begin2 Begin position in the second tuple.
     * @param end2 End position in the second tuple.
     * @return Comparison result.
     *
     * @see #compareAsBytes(BinaryTupleReader, BinaryTupleReader, int)
     */
    public static int compareAsBytes(ByteBufferAccessor buf1, int begin1, int end1, ByteBufferAccessor buf2, int begin2, int end2) {
        if (buf1.get(begin1) == BinaryTupleCommon.VARLEN_EMPTY_BYTE) {
            begin1++;
        }

        int fullSize1 = end1 - begin1;
        int trimmedSize1 = Math.min(fullSize1, buf1.capacity() - begin1);

        if (buf2.get(begin2) == BinaryTupleCommon.VARLEN_EMPTY_BYTE) {
            begin2++;
        }

        int fullSize2 = end2 - begin2;
        int trimmedSize2 = Math.min(fullSize2, buf2.capacity() - begin2);

        int remaining = Math.min(trimmedSize1, trimmedSize2);

        int wordBytes = remaining - remaining % 8;

        for (int i = 0; i < wordBytes; i += 8) {
            long w1 = Long.reverseBytes(buf1.getLong(begin1 + i));
            long w2 = Long.reverseBytes(buf2.getLong(begin2 + i));

            int cmp = Long.compareUnsigned(w1, w2);

            if (cmp != 0) {
                return cmp;
            }
        }

        for (int i = wordBytes; i < remaining; i++) {
            byte b1 = buf1.get(begin1 + i);
            byte b2 = buf2.get(begin2 + i);

            int cmp = Byte.compareUnsigned(b1, b2);

            if (cmp != 0) {
                return cmp;
            }
        }

        if (fullSize1 > remaining && fullSize2 > remaining) {
            // Comparison is not completed yet. Both strings have more characters.
            return 0;
        }

        return signum(fullSize1 - fullSize2);
    }

    /**
     * Compares two binary tuples as strings. The comparison is performed on the column
     * specified by the given index. The comparison can optionally ignore case differences.
     *
     * @param tuple1 the first binary tuple reader
     * @param tuple2 the second binary tuple reader
     * @param colIndex the index of the column in the tuple to compare
     * @return a negative integer, zero, or a positive integer if the first tuple is less than, equal to,
     *         or greater than the second tuple, respectively
     */
    static int compareAsString(BinaryTupleReader tuple1, BinaryTupleReader tuple2, int colIndex) {
        tuple1.seek(colIndex);
        int begin1 = tuple1.begin();
        int end1 = tuple1.end();

        tuple2.seek(colIndex);
        int begin2 = tuple2.begin();
        int end2 = tuple2.end();

        return compareAsString(tuple1.accessor(), begin1, end1, tuple2.accessor(), begin2, end2);
    }

    /**
     * Compares string values of two binary tuples.
     *
     * @param buf1 Buffer accessor for the first tuple.
     * @param begin1 Begin position in the first tuple.
     * @param end1 End position in the first tuple.
     * @param buf2 Buffer accessor for the second tuple.
     * @param begin2 Begin position in the second tuple.
     * @param end2 End position in the second tuple.
     * @return Comparison result.
     * @see #compareAsString(BinaryTupleReader, BinaryTupleReader, int)
     */
    public static int compareAsString(
            ByteBufferAccessor buf1, int begin1, int end1,
            ByteBufferAccessor buf2, int begin2, int end2
    ) {
        if (buf1.get(begin1) == BinaryTupleCommon.VARLEN_EMPTY_BYTE) {
            begin1++;
        }

        int fullStrLength1 = end1 - begin1;
        int trimmedSize1 = Math.min(fullStrLength1, buf1.capacity() - begin1);

        if (buf2.get(begin2) == BinaryTupleCommon.VARLEN_EMPTY_BYTE) {
            begin2++;
        }

        int fullStrLength2 = end2 - begin2;
        int trimmedSize2 = Math.min(fullStrLength2, buf2.capacity() - begin2);

        // Fast pass for ASCII string.
        int asciiResult = compareAsciiSequences(
                buf1,
                begin1,
                fullStrLength1,
                trimmedSize1,
                buf2,
                begin2,
                fullStrLength2,
                trimmedSize2
        );

        if (asciiResult != Integer.MIN_VALUE) {
            return asciiResult;
        }

        // If the string contains non-ASCII characters, we compare it as a Unicode string.
        return fullUnicodeCompare(
                buf1,
                begin1,
                fullStrLength1,
                trimmedSize1,
                buf2,
                begin2,
                fullStrLength2,
                trimmedSize2
        );
    }

    /**
     * Decodes the next UTF-8 code point from the specified buffer starting at the given index.
     * The method updates the index array to reflect the position after the decoded code point.
     * If the code point cannot be fully decoded due to insufficient bytes, it returns -1.
     *
     * @param buf the byte buffer accessor for reading binary data
     * @param begin the starting position in the buffer to begin decoding
     * @param idx an array containing the current index position; it will be updated to the new position
     *            after the code point is decoded
     * @param trimmedSize the maximum limit up to which decoding is allowed
     * @return the decoded Unicode code point as an integer, or -1 if decoding fails due to insufficient bytes
     * @throws IllegalArgumentException if the input data does not conform to the UTF-8 encoding standard
     */
    private static int getNextCodePoint(ByteBufferAccessor buf, int begin, int[] idx, int trimmedSize) {
        int startIdx = idx[0];

        byte b1 = buf.get(begin + startIdx);
        startIdx++;

        if ((b1 & 0x80) == 0) {
            idx[0] = startIdx;

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

        if (startIdx + remainingBytes > trimmedSize) {
            return -1;
        }

        int codePoint = b1 & (0x3F >> remainingBytes);
        for (int i = 0; i < remainingBytes; i++) {
            byte nextByte = buf.get(begin + startIdx);
            startIdx++;

            if ((nextByte & 0xC0) != 0x80) {
                throw new IllegalArgumentException("Invalid UTF-8 continuation");
            }
            codePoint = (codePoint << 6) | (nextByte & 0x3F);
        }

        idx[0] = startIdx;

        return codePoint;
    }

    /**
     * Compares two UTF-8 encoded strings based on their content, length, and
     * the given comparison rules. The comparison processes the encoded bytes
     * as Unicode code points to ensure proper handling of multibyte characters.
     * Optionally, the comparison can ignore case differences for Unicode characters.
     *
     * @param buf1 the first ByteBufferAssessor containing the UTF-8 encoded string
     * @param begin1 the starting position of the first string in the buffer
     * @param fullStrLength1 the full length of the first string in characters
     * @param trimmedSize1 the size limit of the first string in bytes used for comparison
     * @param buf2 the second ByteBufferAssessor containing the UTF-8 encoded string
     * @param begin2 the starting position of the second string in the buffer
     * @param fullStrLength2 the full length of the second string in characters
     * @param trimmedSize2 the size limit of the second string in bytes used for comparison
     * @return a negative integer if the first string is less than the second string,
     *         zero if they are equal, or a positive integer if the first string is
     *         greater than the second string. Returns 0 if either string is truncated
     *         and the comparison is inconclusive.
     */
    private static int fullUnicodeCompare(
            ByteBufferAccessor buf1,
            int begin1,
            int fullStrLength1,
            int trimmedSize1,
            ByteBufferAccessor buf2,
            int begin2,
            int fullStrLength2,
            int trimmedSize2
    ) {
        int remaining = Math.min(trimmedSize1, trimmedSize2);

        int[] idx1 = {0};
        int[] idx2 = {0};

        while (idx1[0] < remaining) {
            int cp1 = getNextCodePoint(buf1, begin1, idx1, trimmedSize1);
            int cp2 = getNextCodePoint(buf2, begin2, idx2, trimmedSize2);

            if (cp1 == -1 || cp2 == -1) {
                // Comparison is impossible because the string is truncated.
                return 0;
            }

            char v1 = (char) cp1;
            char v2 = (char) cp2;

            if (v1 != v2) {
                return signum(v1 - v2);
            }
        }

        if (fullStrLength1 > remaining && fullStrLength2 > remaining) {
            // Comparison is not completed yet. Both strings have more characters.
            return 0;
        }

        return fullStrLength1 == remaining && fullStrLength2 == remaining ? 0 : fullStrLength1 == remaining ? -1 : 1;
    }

    /**
     * Compares two ASCII-encoded byte sequences from the provided buffers, starting at the specified positions,
     * considering their lengths and an optional case-insensitive comparison mode.
     * If either sequence contains non-ASCII characters, the method returns Integer.MIN_VALUE.
     *
     * @param buf1 the first ByteBufferAssessor containing the ASCII sequence
     * @param begin1 the starting position of the first ASCII sequence in the buffer
     * @param fullStrLength1 the full length of the first ASCII sequence in characters
     * @param trimmedSize1 the size limit of the first sequence used for comparison
     * @param buf2 the second ByteBufferAssessor containing the ASCII sequence
     * @param begin2 the starting position of the second ASCII sequence in the buffer
     * @param fullStrLength2 the full length of the second ASCII sequence in characters
     * @param trimmedSize2 the size limit of the second sequence used for comparison
     * @return a negative integer if the first sequence is less than the second sequence,
     *         zero if they are equal, or a positive integer if the first sequence is greater.
     *         If either sequence contains non-ASCII characters, returns Integer.MIN_VALUE.
     *         If the comparison is inconclusive due to truncation, returns 0.
     */
    private static int compareAsciiSequences(
            ByteBufferAccessor buf1,
            int begin1,
            int fullStrLength1,
            int trimmedSize1,
            ByteBufferAccessor buf2,
            int begin2,
            int fullStrLength2,
            int trimmedSize2
    ) {
        int i = 0;
        int remaining = Math.min(trimmedSize1, trimmedSize2);

        while (i + Long.BYTES <= remaining) {
            long w1 = buf1.getLong(begin1 + i);
            long w2 = buf2.getLong(begin2 + i);

            if (((w1 | w2) & 0x8080808080808080L) != 0) {
                return Integer.MIN_VALUE;
            }

            if (w1 != w2) {
                // Big endian comparison of 8 ASCII characters. None of the bytes have a sign bit set, so no masks required.
                return Long.compare(Long.reverseBytes(w1), Long.reverseBytes(w2));
            }

            i += Long.BYTES;
        }

        while (i < remaining) {
            byte b1 = buf1.get(begin1 + i);
            byte b2 = buf2.get(begin2 + i);

            // Checking if it is an ASCII character.
            if ((b1 & 0x80) != 0 || (b2 & 0x80) != 0) {
                return Integer.MIN_VALUE;
            }

            char v1 = (char) b1;
            char v2 = (char) b2;

            i++;

            if (v1 != v2) {
                return signum(v1 - v2);
            }
        }

        if (fullStrLength1 > remaining && fullStrLength2 > remaining) {
            // Comparison is not completed yet. Both strings have more characters.
            return 0;
        }

        return signum(fullStrLength1 - fullStrLength2);
    }
}
