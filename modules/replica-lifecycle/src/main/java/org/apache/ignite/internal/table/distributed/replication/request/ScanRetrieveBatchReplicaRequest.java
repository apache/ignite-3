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

package org.apache.ignite.internal.table.distributed.replication.request;

import java.util.BitSet;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
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
     *
     * @return lower bound.
     */
    @Nullable
    BinaryTupleMessage lowerBoundPrefix();

    /**
     *
     * @return upper bound.
     */
    @Nullable
    BinaryTupleMessage upperBoundPrefix();

    /**
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
