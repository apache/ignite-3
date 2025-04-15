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

package org.apache.ignite.internal.binarytuple;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.lang.IgniteInternalException;

/**
 * Common binary tuple constants and utils.
 */
public class BinaryTupleCommon {
    /** Size of a tuple header, in bytes. */
    public static final int HEADER_SIZE = 1;

    /** Empty varlen token. */
    public static final byte VARLEN_EMPTY_BYTE = (byte) 0x80;

    /** Mask for size of entries in variable-length offset table. */
    public static final int VARSIZE_MASK = 0b011;

    /** Flag that indicates that offset table is oversized. */
    public static final int OFFSET_TABLE_OVERSIZED = 1 << 2;

    /**
     * Flag that indicates that a Binary Tuple is instead a Binary Tuple Prefix.
     *
     * @see BinaryTuplePrefixBuilder
     */
    public static final int PREFIX_FLAG = 1 << 3;

    /**
     * Flag, which indicates how to interpret situations when Binary Tuple Prefix columns are equal to first N columns of a Binary Tuple
     * (where N is the length of the prefix).
     *
     * <p>This flag is used by some index implementations for internal optimizations.
     */
    public static final int EQUALITY_FLAG = 1 << 4;

    /**
     * Calculates flags for a given size of variable-length area.
     *
     * @param size Variable-length area size.
     * @return Flags value.
     */
    public static byte valueSizeToFlags(long size) {
        if (size <= 0xff) {
            return 0b00;
        }
        if (size <= 0xffff) {
            return 0b01;
        }
        if (size <= Integer.MAX_VALUE) {
            return 0b10;
        }
        throw new IgniteInternalException("Too big binary tuple size");
    }

    /**
     * Calculates the size of entry in variable-length offset table for given flags.
     *
     * @param flags Flags value.
     * @return Size of entry in variable-length offset table.
     */
    public static int flagsToEntrySize(byte flags) {
        return 1 << (flags & VARSIZE_MASK);
    }

    /**
     * Calculates the size of entry in variable-length offset table.
     *
     * @param size Variable-length area size.
     * @return Size in bytes.
     */
    public static int valueSizeToEntrySize(long size) {
        if (size <= 0xff) {
            return 1;
        }

        if (size <= 0xffff) {
            return 2;
        }

        if (size <= Integer.MAX_VALUE) {
            return 4;
        }

        throw new IgniteInternalException("Too big binary tuple size");
    }

    /**
     * Converts specified {@link BigDecimal} value to a more compact form, if possible.
     *
     * @param value Field value.
     * @param scale Maximum scale.
     * @return Decimal with a scale reduced to the specified scale and trimmed trailing zeros.
     */
    public static BigDecimal shrinkDecimal(BigDecimal value, int scale) {
        if (value.scale() > scale) {
            value = value.setScale(scale, RoundingMode.HALF_UP);
        }

        BigDecimal noZeros = value.stripTrailingZeros();
        if (noZeros.scale() <= Short.MAX_VALUE && noZeros.scale() >= Short.MIN_VALUE) {
            // Use more compact representation if possible.
            return noZeros;
        }

        return value;
    }

    /**
     * Gets a function that reads the field offset.
     *
     * @param entrySize Offset table entry size.
     * @return Offset calculation function.
     */
    public static OffsetReadFunction offsetReadFunction(int entrySize) {
        switch (entrySize) {
            case Byte.BYTES:
                return BinaryTupleCommon::readByteOffset;
            case Short.BYTES:
                return BinaryTupleCommon::readShortOffset;
            case Integer.BYTES:
                return BinaryTupleCommon::readIntOffset;
            case Long.BYTES:
                throw new BinaryTupleFormatException("Unsupported offset table size");
            default:
                throw new BinaryTupleFormatException("Invalid offset table size");
        }
    }

    /**
     * Gets a function that writes the field offset.
     *
     * @param entrySize Offset table entry size.
     * @return Offset calculation function.
     */
    public static OffsetWriteFunction offsetWriteFunction(int entrySize) {
        switch (entrySize) {
            case Byte.BYTES:
                return BinaryTupleCommon::writeByteOffset;
            case Short.BYTES:
                return BinaryTupleCommon::writeShortOffset;
            case Integer.BYTES:
                return BinaryTupleCommon::writeIntOffset;
            case Long.BYTES:
                throw new BinaryTupleFormatException("Unsupported offset table size");
            default:
                throw new BinaryTupleFormatException("Invalid offset table size");
        }
    }

    private static int readByteOffset(ByteBuffer buffer, int index) {
        return Byte.toUnsignedInt(buffer.get(index));
    }

    private static int readShortOffset(ByteBuffer buffer, int index) {
        return Short.toUnsignedInt(buffer.getShort(index));
    }

    private static int readIntOffset(ByteBuffer buffer, int index) {
        int offset = buffer.getInt(index);
        if (offset < 0) {
            throw new BinaryTupleFormatException("Unsupported offset table size");
        }
        return offset;
    }

    private static void writeByteOffset(ByteBuffer buffer, int index, int offset) {
        buffer.put(index, (byte) offset);
    }

    private static void writeShortOffset(ByteBuffer buffer, int index, int offset) {
        buffer.putShort(index, (short) offset);
    }

    private static void writeIntOffset(ByteBuffer buffer, int index, int offset) {
        buffer.putInt(index, offset);
    }

    /** Offset read function. */
    @FunctionalInterface
    public interface OffsetReadFunction {
        /** Returns offset of the given column index. */
        int offset(ByteBuffer buffer, int index);
    }

    /** Offset write function. */
    @FunctionalInterface
    public interface OffsetWriteFunction {
        /** Writes offset of the given column index. */
        void offset(ByteBuffer buffer, int index, int offset);
    }
}
