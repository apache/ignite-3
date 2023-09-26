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
}
