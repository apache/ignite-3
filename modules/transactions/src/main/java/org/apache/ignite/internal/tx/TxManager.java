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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * A transaction manager.
 */
public interface TxManager extends IgniteComponent {
    /**
     * Starts a transaction coordinated by a local node.
     *
     * @return The transaction.
     */
    InternalTransaction begin();

    /**
     * Returns a transaction state.
     *
     * @param txId Transaction id.
     * @return The state or null if the state is unknown.
     */
    // TODO: IGNITE-17638 TestOnly code, let's consider using Txn state map instead of states.
    @Deprecated
    @Nullable TxState state(UUID txId);

    /**
     * Atomically changes the state of a transaction.
     *
     * @param txId Transaction id.
     * @param before Before state.
     * @param after After state.
     * @return {@code True} if a state was changed.
     */
    // TODO: IGNITE-17638 TestOnly code, let's consider using Txn state map instead of states.
    @Deprecated
    boolean changeState(UUID txId, @Nullable TxState before, TxState after);

    /**
     * Returns lock manager.
     *
     * @return Lock manager for the given transactions manager.
     * @deprecated Use lockManager directly.
     */
    @Deprecated
    public LockManager lockManager();

    /**
     * Finishes a dependant transactions.
     *
     * @param committedPartition Partition to store a transaction state.
     * @param recipientNode Recipient node.
     * @param term Raft term.
     * @param commit {@code True} if a commit requested.
     * @param groups Enlisted partition groups with raft terms.
     * @param txId Transaction id.
     */
    CompletableFuture<Void> finish(
            ReplicationGroupId committedPartition,
            ClusterNode recipientNode,
            Long term,
            boolean commit,
            Map<ClusterNode, List<IgniteBiTuple<ReplicationGroupId, Long>>> groups,
            UUID txId
    );

    /**
     * Sends cleanup request to the specified primary replica.
     *
     * @param recipientNode Primary replica to process given cleanup request.
     * @param replicationGroupIds Replication group id with raft term.
     * @param txId Transaction id.
     * @param commit {@code True} if a commit requested.
     * @param commitTimestamp Commit timestamp.
     * @return Completable future of Void.
     */
    CompletableFuture<Void> cleanup(
            ClusterNode recipientNode,
            List<IgniteBiTuple<ReplicationGroupId, Long>> replicationGroupIds,
            UUID txId,
            boolean commit,
            HybridTimestamp commitTimestamp
            );

    /**
     * Returns a number of finished transactions.
     *
     * @return A number of finished transactions.
     */
    @TestOnly
    int finished();
}
