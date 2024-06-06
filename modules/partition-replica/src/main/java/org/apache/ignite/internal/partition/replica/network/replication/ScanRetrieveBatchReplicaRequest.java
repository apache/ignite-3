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

package org.apache.ignite.internal.partition.replica.network.replication;

import java.util.BitSet;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.jetbrains.annotations.Nullable;

/**
 * Scan retrieve batch replica request.
 */
public interface ScanRetrieveBatchReplicaRequest extends ReplicaRequest {
    /** Batch size. */
    int batchSize();

    /** The id uniquely determines a cursor for the transaction. */
    long scanId();

    /**
     * Gets an index to use fot the retrieve request.
     */
    @Nullable Integer indexToUse();

    /**
     * Gets a key which is used for exact comparison in the index.
     *
     * @return Key to search.
     */
    @Nullable
    BinaryTupleMessage exactKey();

    /**
     * Gets a lower bound to choose entries from {@link SortedIndexStorage}. Exclusivity is controlled by a {@link
     * SortedIndexStorage#GREATER_OR_EQUAL} or {@link SortedIndexStorage#GREATER} flag. {@code null} means unbounded.
     *
     * @return lower bound.
     */
    @Nullable
    BinaryTupleMessage lowerBoundPrefix();

    /**
     * Gets an upper bound to choose entries from {@link SortedIndexStorage}. Upper bound. Exclusivity is controlled by a {@link
     * SortedIndexStorage#LESS} or {@link SortedIndexStorage#LESS_OR_EQUAL} flag. {@code null} means unbounded.
     *
     * @return upper bound.
     */
    @Nullable
    BinaryTupleMessage upperBoundPrefix();

    /**
     * Gets control flags for {@link SortedIndexStorage}. {@link SortedIndexStorage#GREATER} | {@link SortedIndexStorage#LESS} by default.
     * Other available values are {@link SortedIndexStorage#GREATER_OR_EQUAL}, {@link SortedIndexStorage#LESS_OR_EQUAL}.
     *
     * @return Flags to determine a scan order.
     */
    int flags();

    /**
     * Gets bitset to include columns.
     *
     * @return Bitset to include columns.
     */
    @Nullable
    BitSet columnsToInclude();
}
