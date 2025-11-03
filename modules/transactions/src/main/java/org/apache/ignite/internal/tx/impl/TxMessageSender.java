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

import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toReplicationGroupIdMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaResponse;
import org.apache.ignite.internal.replicator.message.ReplicationGroupIdMessage;
import org.apache.ignite.internal.tx.PartitionEnlistment;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TransactionResult;
import org.apache.ignite.internal.tx.message.EnlistedPartitionGroupMessage;
import org.apache.ignite.internal.tx.message.PartitionEnlistmentMessage;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxStateResponse;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicatedInfo;
import org.jetbrains.annotations.Nullable;

/**
 * This class is responsible for interacting with the messaging layer. Sends transaction messages.
 */
public class TxMessageSender {
    private static final int RPC_TIMEOUT_MILLIS = 60 * 1000;

    /** Tx messages factory. */
    private static final TxMessagesFactory TX_MESSAGES_FACTORY = new TxMessagesFactory();

    /** Replica messages factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    /** Messaging service. */
    private final MessagingService messagingService;

    /** Replica service. */
    private final ReplicaService replicaService;

    private final ClockService clockService;

    /**
     * Constructor.
     *
     * @param messagingService Messaging service.
     * @param replicaService Replica service.
     * @param clockService Clock service.
     */
    public TxMessageSender(
            MessagingService messagingService,
            ReplicaService replicaService,
            ClockService clockService
    ) {
        this.messagingService = messagingService;
        this.replicaService = replicaService;
        this.clockService = clockService;
    }

    /**
     * Sends WriteIntentSwitch request to the specified primary replica.
     *
     * @param primaryConsistentId Primary replica to process given cleanup request.
     * @param partition Partition.
     * @param txId Transaction id.
     * @param commit {@code True} if a commit requested.
     * @param commitTimestamp Commit timestamp ({@code null} if it's an abort).
     * @return Completable future of WriteIntentSwitchReplicatedInfo.
     */
    public CompletableFuture<WriteIntentSwitchReplicatedInfo> switchWriteIntents(
            String primaryConsistentId,
            EnlistedPartitionGroup partition,
            UUID txId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        return replicaService.invoke(
                primaryConsistentId,
                TX_MESSAGES_FACTORY.writeIntentSwitchReplicaRequest()
                        .groupId(toReplicationGroupIdMessage(REPLICA_MESSAGES_FACTORY, partition.groupId()))
                        .tableIds(partition.tableIds())
                        .timestamp(clockService.now())
                        .txId(txId)
                        .commit(commit)
                        .commitTimestamp(commitTimestamp)
                        .build()
        );
    }

    /**
     * Sends cleanup request to the specified primary replica.
     *
     * @param primaryConsistentId Primary replica to process given cleanup request.
     * @param enlistedPartitionGroups Partition infos.
     * @param txId Transaction id.
     * @param commit {@code True} if a commit requested.
     * @param commitTimestamp Commit timestamp ({@code null} if it's an abort).
     * @return Completable future of {@link NetworkMessage}.
     */
    public CompletableFuture<NetworkMessage> cleanup(
            String primaryConsistentId,
            @Nullable Collection<EnlistedPartitionGroup> enlistedPartitionGroups,
            UUID txId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        return messagingService.invoke(
                primaryConsistentId,
                TX_MESSAGES_FACTORY.txCleanupMessage()
                        .txId(txId)
                        .commit(commit)
                        .commitTimestamp(commitTimestamp)
                        .timestamp(clockService.now())
                        .groups(toPartitionMessages(enlistedPartitionGroups))
                        .build(),
                RPC_TIMEOUT_MILLIS);
    }

    /**
     * Send a transactions finish request.
     *
     * @param primaryConsistentId Node consistent id to send the request to.
     * @param commitPartition Partition to store a transaction state.
     * @param enlistedPartitions Enlisted partition groups.
     * @param txId Transaction id.
     * @param consistencyToken Enlistment consistency token.
     * @param commit {@code true} if a commit requested.
     * @param commitTimestamp Commit timestamp ({@code null} if it's an abort).
     * @return Completable future of {@link TransactionResult}.
     */
    public CompletableFuture<TransactionResult> finish(
            String primaryConsistentId,
            ReplicationGroupId commitPartition,
            Map<ReplicationGroupId, PartitionEnlistment> enlistedPartitions,
            UUID txId,
            Long consistencyToken,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        ReplicationGroupIdMessage commitPartitionIdMessage = toReplicationGroupIdMessage(REPLICA_MESSAGES_FACTORY, commitPartition);

        return replicaService.invoke(
                primaryConsistentId,
                TX_MESSAGES_FACTORY.txFinishReplicaRequest()
                        .txId(txId)
                        .commitPartitionId(commitPartitionIdMessage)
                        .timestamp(clockService.now())
                        .groupId(toReplicationGroupIdMessage(REPLICA_MESSAGES_FACTORY, commitPartition))
                        .groups(toEnlistedPartitionMessagesByGroupId(enlistedPartitions))
                        .commit(commit)
                        .commitTimestamp(commitTimestamp)
                        .enlistmentConsistencyToken(consistencyToken)
                        .build()
        );
    }

    /**
     * Send TxStateCommitPartitionRequest.
     *
     * @param primaryConsistentId Node id to send the request to.
     * @param txId Transaction id.
     * @param commitGrpId Partition to store a transaction state.
     * @param consistencyToken Enlistment consistency token.
     * @return Completable future of {@link TransactionMeta}.
     */
    public CompletableFuture<TransactionMeta> resolveTxStateFromCommitPartition(
            String primaryConsistentId,
            UUID txId,
            ReplicationGroupId commitGrpId,
            Long consistencyToken
    ) {
        return replicaService.invoke(
                primaryConsistentId,
                TX_MESSAGES_FACTORY.txStateCommitPartitionRequest()
                        .groupId(toReplicationGroupIdMessage(REPLICA_MESSAGES_FACTORY, commitGrpId))
                        .txId(txId)
                        .enlistmentConsistencyToken(consistencyToken)
                        .build()
        );
    }

    /**
     * Send TxStateCoordinatorRequest.
     *
     * @param coordinatorClusterNode Node to send the request to.
     * @param txId Transaction id.
     * @param timestamp Timestamp to pass to target node.
     * @return Completable future of {@link TxStateResponse}.
     */
    public CompletableFuture<TxStateResponse> resolveTxStateFromCoordinator(
            InternalClusterNode coordinatorClusterNode,
            UUID txId,
            HybridTimestamp timestamp
    ) {
        return messagingService.invoke(
                        coordinatorClusterNode,
                        TX_MESSAGES_FACTORY.txStateCoordinatorRequest()
                                .readTimestamp(timestamp)
                                .txId(txId)
                                .build(),
                        RPC_TIMEOUT_MILLIS)
                .thenApply(resp -> {
                    assert resp instanceof TxStateResponse : "Unsupported response type [type=" + resp.getClass().getSimpleName() + ']';

                    return (TxStateResponse) resp;
                });
    }

    /**
     * Send TxCleanupRecoveryRequest.
     *
     * @param primaryConsistentId Node id to send the request to.
     * @param replicationGroupId Replication group ID corresponding to a partition.
     * @return Completable future of ReplicaResponse.
     */
    public CompletableFuture<ReplicaResponse> sendRecoveryCleanup(String primaryConsistentId, ReplicationGroupId replicationGroupId) {
        return replicaService.invoke(
                primaryConsistentId,
                TX_MESSAGES_FACTORY.txCleanupRecoveryRequest()
                        .groupId(toReplicationGroupIdMessage(REPLICA_MESSAGES_FACTORY, replicationGroupId))
                        .build()
        );
    }

    public MessagingService messagingService() {
        return messagingService;
    }

    private static @Nullable List<EnlistedPartitionGroupMessage> toPartitionMessages(
            @Nullable Collection<EnlistedPartitionGroup> enlistedPartitionGroups
    ) {
        if (enlistedPartitionGroups == null) {
            return null;
        }

        var messages = new ArrayList<EnlistedPartitionGroupMessage>(enlistedPartitionGroups.size());

        for (EnlistedPartitionGroup partition : enlistedPartitionGroups) {
            messages.add(
                    TX_MESSAGES_FACTORY.enlistedPartitionGroupMessage()
                            .groupId(toReplicationGroupIdMessage(REPLICA_MESSAGES_FACTORY, partition.groupId()))
                            .tableIds(partition.tableIds())
                            .build()
            );
        }

        return messages;
    }

    private static Map<ReplicationGroupIdMessage, PartitionEnlistmentMessage> toEnlistedPartitionMessagesByGroupId(
            Map<ReplicationGroupId, PartitionEnlistment> idEnlistedPartitions
    ) {
        var messages = new HashMap<ReplicationGroupIdMessage, PartitionEnlistmentMessage>(idEnlistedPartitions.size());

        for (Map.Entry<ReplicationGroupId, PartitionEnlistment> e : idEnlistedPartitions.entrySet()) {
            PartitionEnlistment enlistedPartition = e.getValue();

            messages.put(
                    toReplicationGroupIdMessage(REPLICA_MESSAGES_FACTORY, e.getKey()),
                    TX_MESSAGES_FACTORY.partitionEnlistmentMessage()
                            .primaryConsistentId(enlistedPartition.primaryNodeConsistentId())
                            .tableIds(enlistedPartition.tableIds())
                            .build()
            );
        }

        return messages;
    }
}
