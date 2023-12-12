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

package org.apache.ignite.internal.tx.impl;

import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestampToLong;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkMessage;
import org.jetbrains.annotations.Nullable;

/**
 * This class is responsible for interacting with the messaging layer. Sends transaction messages.
 */
public class TxMessageSender {

    private static final long RPC_TIMEOUT = 3000;

    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    /** Cluster service. */
    private final ClusterService clusterService;

    /** Replica service. */
    private final ReplicaService replicaService;

    /** A hybrid logical clock. */
    private final HybridClock clock;

    /**
     * Constructor.
     *
     * @param clusterService Cluster service.
     * @param replicaService Replica service.
     * @param clock A hybrid logical clock.
     */
    public TxMessageSender(ClusterService clusterService, ReplicaService replicaService, HybridClock clock) {
        this.clusterService = clusterService;
        this.replicaService = replicaService;
        this.clock = clock;
    }

    /**
     * Sends WriteIntentSwitch request to the specified primary replica.
     *
     * @param primaryConsistentId Primary replica to process given cleanup request.
     * @param tablePartitionId Table partition id.
     * @param txId Transaction id.
     * @param commit {@code True} if a commit requested.
     * @param commitTimestamp Commit timestamp ({@code null} if it's an abort).
     * @return Completable future of Void.
     */
    public CompletableFuture<Void> switchWriteIntents(
            String primaryConsistentId,
            TablePartitionId tablePartitionId,
            UUID txId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        return replicaService.invoke(
                primaryConsistentId,
                FACTORY.writeIntentSwitchReplicaRequest()
                        .groupId(tablePartitionId)
                        .timestampLong(clock.nowLong())
                        .txId(txId)
                        .commit(commit)
                        .commitTimestampLong(hybridTimestampToLong(commitTimestamp))
                        .build()
        );
    }

    /**
     * Sends cleanup request to the specified primary replica.
     *
     * @param primaryConsistentId Primary replica to process given cleanup request.
     * @param replicationGroupIds Table partition ids.
     * @param txId Transaction id.
     * @param commit {@code True} if a commit requested.
     * @param commitTimestamp Commit timestamp ({@code null} if it's an abort).
     * @return Completable future of Void.
     */
    public CompletableFuture<NetworkMessage> cleanup(
            String primaryConsistentId,
            Collection<ReplicationGroupId> replicationGroupIds,
            UUID txId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        return clusterService.messagingService().invoke(
                primaryConsistentId,
                FACTORY.txCleanupMessage()
                        .txId(txId)
                        .commit(commit)
                        .commitTimestampLong(hybridTimestampToLong(commitTimestamp))
                        .timestampLong(clock.nowLong())
                        .groups(replicationGroupIds)
                        .build(),
                RPC_TIMEOUT);
    }

    /**
     * Send a transactions finish request.
     *
     * @param primaryConsistentId Node id to send the request to.
     * @param commitPartition Partition to store a transaction state.
     * @param replicationGroupIds Enlisted partition groups.
     * @param txId Transaction id.
     * @param term Raft term.
     * @param commit {@code true} if a commit requested.
     * @param commitTimestamp Commit timestamp ({@code null} if it's an abort).
     * @return Completable future of Void.
     */
    public CompletableFuture<Void> finish(
            String primaryConsistentId,
            TablePartitionId commitPartition,
            Collection<ReplicationGroupId> replicationGroupIds,
            UUID txId,
            Long term,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        return replicaService.invoke(
                primaryConsistentId,
                FACTORY.txFinishReplicaRequest()
                        .txId(txId)
                        .timestampLong(clock.nowLong())
                        .groupId(commitPartition)
                        .groups(replicationGroupIds)
                        .commit(commit)
                        .commitTimestampLong(hybridTimestampToLong(commitTimestamp))
                        .enlistmentConsistencyToken(term)
                        .build());
    }
}
