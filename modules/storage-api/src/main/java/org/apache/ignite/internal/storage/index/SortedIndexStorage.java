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

import java.util.BitSet;
import java.util.function.IntPredicate;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.Cursor;
import org.intellij.lang.annotations.MagicConstant;
import org.jetbrains.annotations.Nullable;

/**
 * Storage for a Sorted Index.
 *
 * <p>This storage serves as a sorted mapping from a subset of a table's columns (a.k.a. index columns) to a {@link SearchRow}
 * from a {@link org.apache.ignite.internal.storage.PartitionStorage} from the same table.
 *
 * @see org.apache.ignite.schema.definition.index.SortedIndexDefinition
 */
public interface SortedIndexStorage extends AutoCloseable {
    /**
     * Returns the Index Descriptor of this storage.
     */
    SortedIndexDescriptor indexDescriptor();

    /**
     * Returns a factory for creating index rows for this storage.
     */
    IndexRowFactory indexRowFactory();

    /**
     * Returns a class deserializing index columns.
     */
    IndexRowDeserializer indexRowDeserializer();

    /**
     * Adds the given index key and {@link SearchRow} to the index.
     *
     * <p>Putting a new value under the same key will overwrite the previous associated value.
     */
    void put(IndexRow row);

    /**
     * Removes the given key from the index.
     *
     * <p>Removing a non-existent key is a no-op.
     */
    void remove(IndexRow row);

    /** Exclude lower bound. */
    byte GREATER = 0;

    /** Include lower bound. */
    byte GREATER_OR_EQUAL = 1;

    /** Exclude upper bound. */
    byte LESS = 0;

    /** Include upper bound. */
    byte LESS_OR_EQUAL = 1 << 1;

    byte FORWARD = 0;

    byte BACKWARDS = 1 << 2;

    /**
     * Returns a range of index values between the lower bound (inclusive) and the upper bound (inclusive).
     */
    // TODO: add options https://issues.apache.org/jira/browse/IGNITE-16059
    Cursor<IndexRow> range(IndexRowPrefix lowerBound, IndexRowPrefix upperBound);

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
     * @param columnsProjection Bit set with column indexes to return. {@code null} means returning all available columns.
     * @param partitionFilter Partition filter predicate. {@code null} means returning data from all partitions.
     * @return Cursor with fetched index rows.
     * @throws IllegalArgumentException If backwards flag is passed and backwards iteration is not supported by the storage.
     */
    default Cursor<IndexRowEx> scan(
            @Nullable IndexRowPrefix lowerBound,
            @Nullable IndexRowPrefix upperBound,
            @MagicConstant(flagsFromClass = SortedIndexStorage.class) byte flags,
            Timestamp timestamp,
            @Nullable BitSet columnsProjection,
            @Nullable IntPredicate partitionFilter
    ) {
        throw new UnsupportedOperationException("scan");
    }

    /**
     * The sole purpose of this class is to avoid massive refactoring while changing the original IndexRow.
     */
    interface IndexRowEx {
        BinaryRow pk();

        Object value(int idx);
    }

    /**
     * Removes all data in this index and frees the associated resources.
     */
    void destroy();
}
