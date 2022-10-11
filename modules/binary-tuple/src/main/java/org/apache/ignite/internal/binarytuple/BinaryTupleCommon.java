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

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.UUID;
import org.apache.ignite.lang.IgniteInternalException;

/**
 * Common binary tuple constants and utils.
 */
public class BinaryTupleCommon {
    /** Size of a tuple header, in bytes. */
    public static final int HEADER_SIZE = 1;

    /** Mask for size of entries in variable-length offset table. */
    public static final int VARSIZE_MASK = 0b011;

    /** Flag that indicates null map presence. */
    public static final int NULLMAP_FLAG = 1 << 2;

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

    /** Default value for UUID elements. */
    public static final UUID DEFAULT_UUID = new UUID(0, 0);

    /** Default value for Date elements (Jan 1st 1 BC). */
    public static final LocalDate DEFAULT_DATE = LocalDate.of(0, 1, 1);

    /** Default value for Time elements (00:00:00). */
    public static final LocalTime DEFAULT_TIME = LocalTime.of(0, 0);

    /** Default value for DateTime elements (Jan 1st 1 BC, 00:00:00). */
    public static final LocalDateTime DEFAULT_DATE_TIME = LocalDateTime.of(0, 1, 1, 0, 0);

    /** Default value for Timestamp elements. */
    public static final Instant DEFAULT_TIMESTAMP = Instant.EPOCH;

    /** Default value for Duration elements. */
    public static final Duration DEFAULT_DURATION = Duration.ZERO;

    /** Default value for Period elements. */
    public static final Period DEFAULT_PERIOD = Period.ZERO;

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
     * Calculates the null map size.
     *
     * @param numElements Number of tuple elements.
     * @return Null map size in bytes.
     */
    public static int nullMapSize(int numElements) {
        return (numElements + 7) / 8;
    }

    /**
     * Returns offset of the byte that contains null-bit of a given tuple element.
     *
     * @param index Tuple element index.
     * @return Offset of the required byte relative to the tuple start.
     */
    public static int nullOffset(int index) {
        return HEADER_SIZE + index / 8;
    }

    /**
     * Returns a null-bit mask corresponding to a given tuple element.
     *
     * @param index Tuple element index.
     * @return Mask to extract the required null-bit.
     */
    public static byte nullMask(int index) {
        return (byte) (1 << (index % 8));
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
