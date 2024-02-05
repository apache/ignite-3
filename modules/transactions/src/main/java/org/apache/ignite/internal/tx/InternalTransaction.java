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

package org.apache.ignite.internal.tx;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * An extension of a transaction for internal usage.
 */
public interface InternalTransaction extends Transaction {
    /**
     * Returns an id.
     *
     * @return The id.
     */
    UUID id();

    /**
     * Returns enlisted primary replica node associated with given replication group.
     *
     * @param tablePartitionId Table partition id.
     * @return Enlisted primary replica node and consistency token associated with given replication group.
     */
    IgniteBiTuple<ClusterNode, Long> enlistedNodeAndConsistencyToken(TablePartitionId tablePartitionId);

    /**
     * Returns a transaction state.
     *
     * @return The state.
     */
    TxState state();

    /**
     * Assigns a partition id to store the transaction state.
     *
     * @param tablePartitionId Commit partition group id.
     * @return True if the partition was assigned as committed, false otherwise.
     */
    boolean assignCommitPartition(TablePartitionId tablePartitionId);

    /**
     * Gets a partition id that stores the transaction state.
     *
     * @return Partition id.
     */
    TablePartitionId commitPartition();

    /**
     * Enlists a partition group into a transaction.
     *
     * @param tablePartitionId Table partition id to enlist.
     * @param nodeAndConsistencyToken Primary replica cluster node and consistency token to enlist for given replication group.
     * @return {@code True} if a partition is enlisted into the transaction.
     */
    IgniteBiTuple<ClusterNode, Long> enlist(TablePartitionId tablePartitionId, IgniteBiTuple<ClusterNode, Long> nodeAndConsistencyToken);

    /**
     * Returns read timestamp for the given transaction if it is a read-only one or {code null} otherwise.
     *
     * @return Read timestamp for the given transaction if it is a read-only one or {code null} otherwise.
     */
    @Nullable HybridTimestamp readTimestamp();

    /**
     * Returns a timestamp that corresponds to the starting moment of the transaction.
     * For RW transactions, this is the beginTimestamp; for RO transactions, it's {@link #readTimestamp()}.
     *
     * @return Timestamp that is used to obtain the effective schema version used inside the transaction.
     */
    HybridTimestamp startTimestamp();

    /**
     * Get the transaction coordinator inconsistent ID.
     *
     * @return Transaction coordinator inconsistent ID.
     */
    String coordinatorId();

    /**
     * Finishes a read-only transaction with a specific execution timestamp.
     *
     * @param commit Commit flag. The flag is ignored for read-only transactions.
     * @param executionTimestamp The timestamp is the time when a read-only transaction is applied to the remote node.
     * @return The future.
     */
    default CompletableFuture<Void> finish(boolean commit, HybridTimestamp executionTimestamp) {
        return commit ? commitAsync() : rollbackAsync();
    }
}
