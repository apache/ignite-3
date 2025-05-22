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

import java.nio.ByteBuffer;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * The utility class has methods to use to compare fields in binary representation.
 */
class BinaryTupleComparatorUtilsWithoutCopy {

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
        ByteBuffer buf1 = tuple1.byteBuffer();
        long offset1 = buf1.isDirect() ? GridUnsafe.bufferAddress(buf1) + begin1 : GridUnsafe.BYTE_ARR_OFF + buf1.arrayOffset() + begin1;
        int fullSize1 = end1 - begin1;
        int trimmedSize1 = Math.min(fullSize1, buf1.capacity() - begin1);

        tuple2.seek(colIndex);
        int begin2 = tuple2.begin();
        int end2 = tuple2.end();
        ByteBuffer buf2 = tuple2.byteBuffer();
        long offset2 = buf2.isDirect() ? GridUnsafe.bufferAddress(buf2) + begin2 : GridUnsafe.BYTE_ARR_OFF + buf2.arrayOffset() + begin2;
        int fullSize2 = end2 - begin2;
        int trimmedSize2 = Math.min(fullSize2, buf2.capacity() - begin2);

        int remaining = Math.min(trimmedSize1, trimmedSize2);

        if (remaining >= 8) {
            long seconds1 = buf1.isDirect() ? GridUnsafe.getLong(offset1) : GridUnsafe.getLong(buf1.array(), offset1);
            long seconds2 = buf2.isDirect() ? GridUnsafe.getLong(offset2) : GridUnsafe.getLong(buf2.array(), offset2);

            int cmp = Long.compare(seconds1, seconds2);

            if (cmp != 0) {
                return cmp;
            }

            if (remaining == 12) {
                int nanos1 = buf1.isDirect() ? GridUnsafe.getInt(offset1 + 8) : GridUnsafe.getInt(buf1.array(), offset1 + 8);
                int nanos2 = buf2.isDirect() ? GridUnsafe.getInt(offset2 + 8) : GridUnsafe.getInt(buf2.array(), offset2 + 8);

                return nanos1 - nanos2;
            }

            if (fullSize1 == 8 && fullSize2 == 12) {
                return -1;
            } else if (fullSize1 == 12 && fullSize2 == 8) {
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
        ByteBuffer buf1 = tuple1.byteBuffer();
        long offset1 = buf1.isDirect() ? GridUnsafe.bufferAddress(buf1) + begin1 : GridUnsafe.BYTE_ARR_OFF + buf1.arrayOffset() + begin1;
        int trimmedSize1 = Math.min(16, buf1.capacity() - begin1);

        tuple2.seek(colIndex);
        int begin2 = tuple2.begin();
        ByteBuffer buf2 = tuple2.byteBuffer();
        long offset2 = buf2.isDirect() ? GridUnsafe.bufferAddress(buf2) + begin2 : GridUnsafe.BYTE_ARR_OFF + buf2.arrayOffset() + begin2;
        int trimmedSize2 = Math.min(16, buf2.capacity() - begin2);

        int remaining = Math.min(trimmedSize1, trimmedSize2);

        if (remaining >= 8) {
            long msb1 = buf1.isDirect() ? GridUnsafe.getLong(offset1) : GridUnsafe.getLong(buf1.array(), offset1);
            long msb2 = buf2.isDirect() ? GridUnsafe.getLong(offset2) : GridUnsafe.getLong(buf2.array(), offset2);

            int cmp = Long.compare(msb1, msb2);

            if (cmp != 0) {
                return cmp;
            }

            if (remaining == 16) {
                long lsb1 = buf1.isDirect() ? GridUnsafe.getLong(offset1 + 8) : GridUnsafe.getLong(buf1.array(), offset1 + 8);
                long lsb2 = buf2.isDirect() ? GridUnsafe.getLong(offset2 + 8) : GridUnsafe.getLong(buf2.array(), offset2 + 8);

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

        ByteBuffer buf1 = tuple1.byteBuffer();
        ByteBufferWrapper tupleWrapper1 =
                buf1.isDirect() ? new DirectByteBufferWrapper(buf1, begin1) : new HeapByteBufferWrapper(buf1, begin1);

        begin1 += tupleWrapper1.shift();

        int fullSize1 = end1 - begin1;
        int trimmedSize1 = Math.min(fullSize1, buf1.capacity() - begin1);

        tuple2.seek(colIndex);
        int begin2 = tuple2.begin();
        int end2 = tuple2.end();

        ByteBuffer buf2 = tuple2.byteBuffer();
        ByteBufferWrapper tupleWrapper2 =
                buf2.isDirect() ? new DirectByteBufferWrapper(buf2, begin2) : new HeapByteBufferWrapper(buf2, begin2);

        begin2 += tupleWrapper2.shift();

        int fullSize2 = end2 - begin2;
        int trimmedSize2 = Math.min(fullSize2, buf2.capacity() - begin2);

        int remaining = Math.min(trimmedSize1, trimmedSize2);

        int wordBytes = remaining - remaining % 8;

        for (int i = 0; i < wordBytes; i += 8) {
            long w1 = tupleWrapper1.getLongLittleEndian(i);
            long w2 = tupleWrapper2.getLongLittleEndian(i);

            int cmp = Long.compareUnsigned(w1, w2);

            if (cmp != 0) {
                return cmp;
            }
        }

        for (int i = wordBytes; i < remaining; i++) {
            byte b1 = tupleWrapper1.get(i);
            byte b2 = tupleWrapper2.get(i);

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
     * @param ignoreCase specifies whether the comparison should ignore case differences
     * @return a negative integer, zero, or a positive integer if the first tuple is less than, equal to,
     *         or greater than the second tuple, respectively
     */
    static int compareAsString(BinaryTupleReader tuple1, BinaryTupleReader tuple2, int colIndex, boolean ignoreCase) {
        tuple1.seek(colIndex);
        int begin1 = tuple1.begin();
        int end1 = tuple1.end();

        ByteBuffer buf1 = tuple1.byteBuffer();
        ByteBufferWrapper tupleWrapper1 =
                buf1.isDirect() ? new DirectByteBufferWrapper(buf1, begin1) : new HeapByteBufferWrapper(buf1, begin1);

        begin1 += tupleWrapper1.shift();

        int fullStrLength1 = end1 - begin1;
        int trimmedSize1 = Math.min(fullStrLength1, buf1.capacity() - begin1);

        tuple2.seek(colIndex);
        int begin2 = tuple2.begin();
        int end2 = tuple2.end();

        ByteBuffer buf2 = tuple2.byteBuffer();
        ByteBufferWrapper tupleWrapper2 =
                buf2.isDirect() ? new DirectByteBufferWrapper(buf2, begin2) : new HeapByteBufferWrapper(buf2, begin2);

        begin2 += tupleWrapper2.shift();

        int fullStrLength2 = end2 - begin2;
        int trimmedSize2 = Math.min(fullStrLength2, buf2.capacity() - begin2);

        // Fast pass for ASCII string.
        int asciiResult = compareAsciiSequences(
                tupleWrapper1,
                fullStrLength1,
                trimmedSize1,
                tupleWrapper2,
                fullStrLength2,
                trimmedSize2,
                ignoreCase
        );

        if (asciiResult != Integer.MIN_VALUE) {
            return asciiResult;
        }

        // If the string contains non-ASCII characters, we compare it as a Unicode string.
        return fullUnicodeCompare(
                tupleWrapper1,
                fullStrLength1,
                trimmedSize1,
                tupleWrapper2,
                fullStrLength2,
                trimmedSize2,
                ignoreCase
        );
    }

    /**
     * Decodes the next UTF-8 encoded code point from the specified position in the given buffer.
     *
     * @param tuple the ByteBufferWrapper instance containing the UTF-8 encoded data
     * @param idx a single-element array holding the current index in the buffer.
     * @param trimmedSize the maximum number of bytes available for decoding in the buffer
     * @return the decoded code point as an integer, or -1 if the remaining bytes are insufficient to decode a valid character
     * @throws IllegalArgumentException if the data in the buffer does not conform to valid UTF-8 encoding
     */
    private static int getNextCodePoint(ByteBufferWrapper tuple, int[] idx, int trimmedSize) {
        int startIdx = idx[0];

        byte b1 = tuple.get(startIdx);
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
            byte nextByte = tuple.get(startIdx);
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
     * Compares two binary tuples encoded as Unicode strings. The comparison considers Unicode
     * code points and optionally ignores case differences. If the strings are truncated, the
     * comparison might be incomplete.
     *
     * @param tuple1 the first ByteBufferWrapper containing the binary-encoded string
     * @param fullStrLength1 the full length of the first string in characters
     * @param trimmedSize1 the trimmed size of the first string, in bytes, used for comparison
     * @param tuple2 the second ByteBufferWrapper containing the binary-encoded string
     * @param fullStrLength2 the full length of the second string in characters
     * @param trimmedSize2 the trimmed size of the second string, in bytes, used for comparison
     * @param ignoreCase specifies whether the comparison should ignore casing during evaluation
     * @return a negative integer if the first string is less than the second string, zero if
     *         they are equal, or a positive integer if the first string is greater than the
     *         second string
     */
    private static int fullUnicodeCompare(
            ByteBufferWrapper tuple1,
            int fullStrLength1,
            int trimmedSize1,
            ByteBufferWrapper tuple2,
            int fullStrLength2,
            int trimmedSize2,
            boolean ignoreCase
    ) {
        int remaining = Math.min(trimmedSize1, trimmedSize2);

        int[] idx1 = {0};
        int[] idx2 = {0};

        while (idx1[0] < remaining) {
            int cp1 = getNextCodePoint(tuple1, idx1, trimmedSize1);
            int cp2 = getNextCodePoint(tuple2, idx2, trimmedSize2);

            if (cp1 == -1 || cp2 == -1) {
                // Comparison is impossible because the string is truncated.
                return 0;
            }

            char v1 = (char) cp1;
            char v2 = (char) cp2;

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

        if (fullStrLength1 > remaining && fullStrLength2 > remaining) {
            // Comparison is not completed yet. Both strings have more characters.
            return 0;
        }

        return fullStrLength1 == remaining && fullStrLength2 == remaining ? 0 : fullStrLength1 == remaining ? -1 : 1;
    }

    /**
     * Compares two ASCII-encoded sequences based on their content, length, and
     * the given comparison rules. The comparison stops when the sequences differ
     * or one sequence is exhausted. Optionally, the comparison can ignore case
     * differences for ASCII letters.
     *
     * @param tuple1 the first ByteBufferWrapper containing the ASCII-encoded sequence
     * @param fullStrLength1 the full length of the first sequence in characters
     * @param trimmedSize1 the trimmed size of the first sequence in bytes used for comparison
     * @param tuple2 the second ByteBufferWrapper containing the ASCII-encoded sequence
     * @param fullStrLength2 the full length of the second sequence in characters
     * @param trimmedSize2 the trimmed size of the second sequence in bytes used for comparison
     * @param ignoreCase specifies whether the comparison should ignore case differences
     * @return a negative integer if the first sequence is less than the second sequence,
     *         zero if they are equal, or a positive integer if the first sequence is
     *         greater than the second sequence. Returns Integer.MIN_VALUE if any of
     *         the sequences contains non-ASCII characters.
     */
    private static int compareAsciiSequences(
            ByteBufferWrapper tuple1,
            int fullStrLength1,
            int trimmedSize1,
            ByteBufferWrapper tuple2,
            int fullStrLength2,
            int trimmedSize2,
            boolean ignoreCase
    ) {
        int i = 0;
        int remaining = Math.min(trimmedSize1, trimmedSize2);

        while (i < remaining) {
            byte b1 = tuple1.get(i);
            byte b2 = tuple2.get(i);

            // Checking if it is an ASCII character.
            if ((b1 & 0x80) != 0 || (b2 & 0x80) != 0) {
                return Integer.MIN_VALUE;
            }

            char v1 = (char) b1;
            char v2 = (char) b2;

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

        if (fullStrLength1 > remaining && fullStrLength2 > remaining) {
            // Comparison is not completed yet. Both strings have more characters.
            return 0;
        }

        return signum(fullStrLength1 - fullStrLength2);
    }

    /**
     * Represents a wrapper around a byte buffer that provides access to its contents
     * with additional metadata such as an offset shift. This interface allows for
     * unified handling of different types of underlying byte buffer implementations
     * (e.g., heap-based or direct memory).
     */
    private interface ByteBufferWrapper {
        /**
         * Returns the offset shift applied to the underlying byte buffer in the implementation.
         *
         * @return the offset shift value as an integer.
         */
        int shift();

        /**
         * Retrieves the byte value at the specified index after applying any conceptual
         * offset or shift adjustments as per the implementation.
         *
         * @param p the index, adjusted for the internal representation of the wrapped byte buffer,
         *          from which the byte value is to be retrieved.
         * @return the byte value from the adjusted index in the internal buffer.
         */
        byte get(int p);

        /**
         * Retrieves a 64-bit long value from the underlying byte buffer at the specified index
         * using little-endian byte order. The method interprets the specified position as the starting
         * index of an 8-byte region and reads the bytes in little-endian order to construct the long value.
         *
         * @param p the index in the underlying byte buffer to start reading the 64-bit long value from.
         * @return the 64-bit long value interpreted from the 8 bytes starting at the specified index in little-endian byte order.
         */
        long getLongLittleEndian(int p);
    }

    /**
     * Represents a wrapper around a direct {@code ByteBuffer} with added functionality
     * for handling a potential offset shift. This class is specifically tailored to
     * work with direct byte buffers and provides efficient access to their contents
     * using native memory operations.
     * This implementation supports identifying a specific data marker for variable-length
     * data (e.g., {@code BinaryTupleCommon.VARLEN_EMPTY_BYTE}) and adjusts the internal
     * address and offset shift accordingly.
     */
    private static class DirectByteBufferWrapper implements ByteBufferWrapper {
        private final long addr;
        private final int shift;

        DirectByteBufferWrapper(ByteBuffer buff, int begin) {
            this(buff, begin, true);
        }

        DirectByteBufferWrapper(ByteBuffer buff, int begin, boolean mightEmpty) {
            assert buff.isDirect();

            long addr = GridUnsafe.bufferAddress(buff) + begin;

            if (mightEmpty && GridUnsafe.getByte(addr) == BinaryTupleCommon.VARLEN_EMPTY_BYTE) {
                shift = 1;
                addr++;
            } else {
                shift = 0;
            }

            this.addr = addr;
        }

        @Override
        public int shift() {
            return shift;
        }

        @Override
        public byte get(int p) {
            return GridUnsafe.getByte(addr + p);
        }

        @Override
        public long getLongLittleEndian(int p) {
            return GridUnsafe.getLongLittleEndian(addr + p);
        }
    }

    /**
     * A heap-based implementation of the {@code ByteBufferWrapper} interface. This class wraps
     * a non-direct {@link ByteBuffer}, providing access to its contents with additional management
     * of an offset and variable-length encoding indicator.
     * The wrapper maintains an internal reference to the byte array backing the given byte buffer
     * and adjusts its offset based on the starting position and any special marker indicating
     * variable-length fields.
     */
    private static class HeapByteBufferWrapper implements ByteBufferWrapper {
        private final byte[] bytes;
        private final int begin;
        private final int shift;

        HeapByteBufferWrapper(ByteBuffer buff, int begin) {
            this(buff, begin, true);
        }

        HeapByteBufferWrapper(ByteBuffer buff, int begin, boolean mightEmpty) {
            assert !buff.isDirect();

            bytes = buff.array();

            begin += GridUnsafe.BYTE_ARR_OFF;
            begin += buff.arrayOffset();

            if (mightEmpty && GridUnsafe.getByte(bytes, begin) == BinaryTupleCommon.VARLEN_EMPTY_BYTE) {
                shift = 1;
                begin++;
            } else {
                shift = 0;
            }

            this.begin = begin;
        }

        @Override
        public int shift() {
            return shift;
        }

        @Override
        public byte get(int p) {
            return GridUnsafe.getByte(bytes, begin + p);
        }

        @Override
        public long getLongLittleEndian(int p) {
            return GridUnsafe.getLongLittleEndian(bytes, begin + p);
        }
    }
}
