/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage.index;

import java.util.function.IntPredicate;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.Cursor;
import org.intellij.lang.annotations.MagicConstant;
import org.jetbrains.annotations.Nullable;

/**
 * Storage for a sorted index.
 */
public interface SortedIndexMvStorage {
    /** Exclude lower bound. */
    byte GREATER = 0;

    /** Include lower bound. */
    byte GREATER_OR_EQUAL = 1;

    /** Exclude upper bound. */
    byte LESS = 0;

    /** Include upper bound. */
    byte LESS_OR_EQUAL = 1 << 1;

    /** Forward scan. */
    byte FORWARD = 0;

    /** Backwards scan. */
    byte BACKWARDS = 1 << 2;

    /**
     * The sole purpose of this class is to avoid massive refactoring while changing the original IndexRow.
     */
    interface IndexRowEx {
        /**
         * Key-only binary row if index-only scan is supported, full binary row otherwise.
         */
        BinaryRow row();

        /**
         * Returns indexed column value.
         *
         * @param idx PK column index.
         * @return Indexed column value.
         */
        Object value(int idx);
    }

    boolean supportsBackwardsScan();

    boolean supportsIndexOnlyScan();

    /**
     * Returns a range of index values between the lower bound and the upper bound, consistent with the passed timestamp.
     *
     * @param lowerBound Lower bound. Exclusivity is controlled by a {@link #GREATER_OR_EQUAL} or {@link #GREATER} flag.
     *      {@code null} means unbounded.
     * @param upperBound Upper bound. Exclusivity is controlled by a {@link #LESS} or {@link #LESS_OR_EQUAL} flag.
     *      {@code null} means unbounded.
     * @param flags Control flags. {@link #GREATER} | {@link #LESS} | {@link #FORWARD} by default. Other available values
     *      are {@link #GREATER_OR_EQUAL}, {@link #LESS_OR_EQUAL} and {@link #BACKWARDS}.
     * @param timestamp Timestamp value for consistent multiversioned index scan.
     * @param partitionFilter Partition filter predicate. {@code null} means returning data from all partitions.
     * @return Cursor with fetched index rows.
     * @throws IllegalArgumentException If backwards flag is passed and backwards iteration is not supported by the storage.
     */
    Cursor<IndexRowEx> scan(
            @Nullable IndexRowPrefix lowerBound,
            @Nullable IndexRowPrefix upperBound,
            @MagicConstant(flagsFromClass = SortedIndexStorage.class) byte flags,
            Timestamp timestamp,
            @Nullable IntPredicate partitionFilter
    );
}
