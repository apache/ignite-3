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

package org.apache.ignite.internal.storage.index;

import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.util.Cursor;
import org.intellij.lang.annotations.MagicConstant;
import org.jetbrains.annotations.Nullable;

/**
 * Storage for a Sorted Index.
 *
 * <p>This storage serves as a sorted mapping from a subset of a table's columns (a.k.a. index columns) to a set of {@link RowId}s
 * from a single {@link org.apache.ignite.internal.storage.MvPartitionStorage} from the same table.
 */
public interface SortedIndexStorage extends IndexStorage {
    /** Exclude lower bound. */
    int GREATER = 0;

    /** Include lower bound. */
    int GREATER_OR_EQUAL = 1;

    /** Exclude upper bound. */
    int LESS = 0;

    /** Include upper bound. */
    int LESS_OR_EQUAL = 1 << 1;

    /**
     * Returns the Index Descriptor of this storage.
     */
    StorageSortedIndexDescriptor indexDescriptor();

    /**
     * Returns a range of updatable index values between the lower bound and the upper bound, supporting read-write transactions.
     *
     * @param lowerBound Lower bound. Exclusivity is controlled by a {@link #GREATER_OR_EQUAL} or {@link #GREATER} flag.
     *      {@code null} means unbounded.
     * @param upperBound Upper bound. Exclusivity is controlled by a {@link #LESS} or {@link #LESS_OR_EQUAL} flag.
     *      {@code null} means unbounded.
     * @param flags Control flags. {@link #GREATER} | {@link #LESS} by default. Other available values
     *      are {@link #GREATER_OR_EQUAL}, {@link #LESS_OR_EQUAL}.
     * @return Cursor with fetched index rows.
     * @throws IllegalArgumentException If backwards flag is passed and backwards iteration is not supported by the storage.
     */
    PeekCursor<IndexRow> scan(
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            @MagicConstant(flagsFromClass = SortedIndexStorage.class) int flags
    );

    /**
     * Returns a range of index values between the lower bound and the upper bound, use in read-only transactions.
     *
     * @param lowerBound Lower bound. Exclusivity is controlled by a {@link #GREATER_OR_EQUAL} or {@link #GREATER} flag.
     *      {@code null} means unbounded.
     * @param upperBound Upper bound. Exclusivity is controlled by a {@link #LESS} or {@link #LESS_OR_EQUAL} flag.
     *      {@code null} means unbounded.
     * @param flags Control flags. {@link #GREATER} | {@link #LESS} by default. Other available values
     *      are {@link #GREATER_OR_EQUAL}, {@link #LESS_OR_EQUAL}.
     * @return Cursor with fetched index rows.
     * @throws IllegalArgumentException If backwards flag is passed and backwards iteration is not supported by the storage.
     */
    default Cursor<IndexRow> readOnlyScan(
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            @MagicConstant(flagsFromClass = SortedIndexStorage.class) int flags
    ) {
        return scan(lowerBound, upperBound, flags);
    }
}
