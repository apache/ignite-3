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

import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.util.Cursor;
import org.intellij.lang.annotations.MagicConstant;
import org.jetbrains.annotations.Nullable;

/**
 * Storage for a Sorted Index.
 *
 * <p>This storage serves as a sorted mapping from a subset of a table's columns (a.k.a. index columns) to a {@link RowId}
 * from a {@link org.apache.ignite.internal.storage.MvPartitionStorage} from the same table.
 *
 * @see org.apache.ignite.schema.definition.index.SortedIndexDefinition
 */
public interface SortedIndexStorage {
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
     * Returns the Index Descriptor of this storage.
     */
    SortedIndexDescriptor indexDescriptor();

    /**
     * Returns a factory for creating index rows for this storage.
     */
    IndexRowSerializer indexRowSerializer();

    /**
     * Returns a class deserializing index columns.
     */
    IndexRowDeserializer indexRowDeserializer();

    /**
     * Adds the given index row to the index.
     */
    void put(IndexRow row);

    /**
     * Removes the given key from the index.
     *
     * <p>Removing a non-existent key is a no-op.
     */
    void remove(IndexRow row);

    /**
     * Returns a range of index values between the lower bound and the upper bound.
     *
     * @param lowerBound Lower bound. Exclusivity is controlled by a {@link #GREATER_OR_EQUAL} or {@link #GREATER} flag.
     *      {@code null} means unbounded.
     * @param upperBound Upper bound. Exclusivity is controlled by a {@link #LESS} or {@link #LESS_OR_EQUAL} flag.
     *      {@code null} means unbounded.
     * @param flags Control flags. {@link #GREATER} | {@link #LESS} | {@link #FORWARD} by default. Other available values
     *      are {@link #GREATER_OR_EQUAL}, {@link #LESS_OR_EQUAL} and {@link #BACKWARDS}.
     * @return Cursor with fetched index rows.
     * @throws IllegalArgumentException If backwards flag is passed and backwards iteration is not supported by the storage.
     */
    Cursor<IndexRow> scan(
            @Nullable BinaryTuple lowerBound,
            @Nullable BinaryTuple upperBound,
            @MagicConstant(flagsFromClass = SortedIndexStorage.class) int flags
    );
}
