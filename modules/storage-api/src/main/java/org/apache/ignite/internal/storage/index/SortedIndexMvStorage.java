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

import java.util.UUID;
import java.util.function.IntPredicate;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.Cursor;
import org.intellij.lang.annotations.MagicConstant;
import org.jetbrains.annotations.Nullable;

/**
 * Storage for a sorted index.
 * POC version, that represents a combination between a replicated TX-aware MV storage and physical MV storage. Real future implementation
 * will be defined later. Things to notice here: TX-aware implementation should have a projection bitset instead of full row reading.
 * Physical storage API will be enriched with append/remove methods, like in reference implementation.
 */
public interface SortedIndexMvStorage {
    /** Exclude lower bound. */
    int GREATER = 0;

    /** Include lower bound. */
    int GREATER_OR_EQUAL = 1;

    /** Exclude upper bound. */
    int LESS = 0;

    /** Include upper bound. */
    int LESS_OR_EQUAL = 1 << 1;

    /** Forward scan. */
    int FORWARD = 0;

    /** Backwards scan. */
    int BACKWARDS = 1 << 2;

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
     * Returns a range of index values between the lower bound and the upper bound, consistent with the passed transaction id.
     *
     * @param lowerBound Lower bound. Exclusivity is controlled by a {@link #GREATER_OR_EQUAL} or {@link #GREATER} flag.
     *      {@code null} means unbounded.
     * @param upperBound Upper bound. Exclusivity is controlled by a {@link #LESS} or {@link #LESS_OR_EQUAL} flag.
     *      {@code null} means unbounded.
     * @param flags Control flags. {@link #GREATER} | {@link #LESS} | {@link #FORWARD} by default. Other available values
     *      are {@link #GREATER_OR_EQUAL}, {@link #LESS_OR_EQUAL} and {@link #BACKWARDS}.
     * @param txId Transaction id for consistent multi-versioned index scan.
     * @param partitionFilter Partition filter predicate. {@code null} means returning data from all partitions.
     * @return Cursor with fetched index rows.
     * @throws IllegalArgumentException If backwards flag is passed and backwards iteration is not supported by the storage.
     */
    Cursor<IndexRowEx> scan(
            @Nullable IndexRowPrefix lowerBound,
            @Nullable IndexRowPrefix upperBound,
            @MagicConstant(flagsFromClass = SortedIndexStorage.class) int flags,
            UUID txId,
            @Nullable IntPredicate partitionFilter
    );

    /**
     * Returns a range of index values between the lower bound and the upper bound, consistent with the passed timestamp.
     *
     * @param lowerBound Lower bound. Exclusivity is controlled by a {@link #GREATER_OR_EQUAL} or {@link #GREATER} flag.
     *      {@code null} means unbounded.
     * @param upperBound Upper bound. Exclusivity is controlled by a {@link #LESS} or {@link #LESS_OR_EQUAL} flag.
     *      {@code null} means unbounded.
     * @param flags Control flags. {@link #GREATER} | {@link #LESS} | {@link #FORWARD} by default. Other available values
     *      are {@link #GREATER_OR_EQUAL}, {@link #LESS_OR_EQUAL} and {@link #BACKWARDS}.
     * @param timestamp Timestamp value for consistent multi-versioned index scan.
     * @param partitionFilter Partition filter predicate. {@code null} means returning data from all partitions.
     * @return Cursor with fetched index rows.
     * @throws IllegalArgumentException If backwards flag is passed and backwards iteration is not supported by the storage.
     */
    Cursor<IndexRowEx> scan(
            @Nullable IndexRowPrefix lowerBound,
            @Nullable IndexRowPrefix upperBound,
            @MagicConstant(flagsFromClass = SortedIndexStorage.class) int flags,
            Timestamp timestamp,
            @Nullable IntPredicate partitionFilter
    );
}
