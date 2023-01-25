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

package org.apache.ignite.internal.index;

import java.util.BitSet;
import java.util.UUID;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * An object describing an abstract index.
 *
 * <p>Provides access to the indexed data as well as all information about index itself.
 */
public interface Index<DescriptorT extends IndexDescriptor> {
    /** Returns identifier of the index. */
    UUID id();

    /** Returns name of the index. */
    String name();

    /** Returns table id index belong to. */
    UUID tableId();

    /** Returns index descriptor. */
    DescriptorT descriptor();

    /**
     * Returns cursor for the values corresponding to the given key.
     *
     * @param partId Partition id.
     * @param tx Transaction.
     * @param key Key to lookup.
     * @param columns Columns to include.
     * @return A cursor from resulting rows.
     * @deprecated IGNITE-17952 Use {@link #lookup(int, UUID, ClusterNode, long, BinaryTuple, BitSet)} instead.
     */
    @Deprecated
    Publisher<BinaryRow> lookup(int partId, @Nullable InternalTransaction tx, BinaryTuple key, @Nullable BitSet columns);

    /**
     * Returns cursor for the values corresponding to the given key.
     *
     * @param partId Partition id.
     * @param txId Transaction id.
     * @param leaderNode Raft group leader node that must handle given get request.
     * @param leaderTerm Raft group leader term.
     * @param key Key to lookup.
     * @param columns Columns to include.
     * @return A cursor from resulting rows.
     */
    Publisher<BinaryRow> lookup(
            int partId,
            UUID txId,
            ClusterNode leaderNode,
            long leaderTerm,
            BinaryTuple key,
            @Nullable BitSet columns
    );

    /**
     * Returns cursor for the values corresponding to the given key.
     *
     * @param partId Partition id.
     * @param readTimestamp Read timestamp.
     * @param recipientNode Cluster node that will handle given get request.
     * @param key Key to search.
     * @param columns Columns to include.
     * @return A cursor from resulting rows.
     */
    Publisher<BinaryRow> lookup(
            int partId,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode,
            BinaryTuple key,
            @Nullable BitSet columns
    );
}
