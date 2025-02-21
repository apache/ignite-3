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

import static org.apache.ignite.internal.lang.IgniteSystemProperties.enabledColocation;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toReplicationGroupIdMessage;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toTablePartitionIdMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaResponse;
import org.apache.ignite.internal.replicator.message.ReplicationGroupIdMessage;
import org.apache.ignite.internal.replicator.message.TablePartitionIdMessage;
import org.apache.ignite.internal.tx.PartitionEnlistment;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TransactionResult;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.message.EnlistedPartitionGroupMessage;
import org.apache.ignite.internal.tx.message.PartitionEnlistmentMessage;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxStateResponse;
import org.jetbrains.annotations.Nullable;

/**
 * This class is responsible for interacting with the messaging layer. Sends transaction messages.
 */
public class TxMessageSender {
    /** Tx messages factory. */
    private static final TxMessagesFactory TX_MESSAGES_FACTORY = new TxMessagesFactory();

    /** Replica messages factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    /** Messaging service. */
    private final MessagingService messagingService;

    /** Replica service. */
    private final ReplicaService replicaService;

    private final ClockService clockService;

    private final TransactionConfiguration transactionConfiguration;

    /**
     * Constructor.
     *
     * @param messagingService Messaging service.
     * @param replicaService Replica service.
     * @param clockService Clock service.
     * @param transactionConfiguration Transaction configuration.
     */
    public TxMessageSender(
            MessagingService messagingService,
            ReplicaService replicaService,
            ClockService clockService,
            TransactionConfiguration transactionConfiguration
    ) {
        this.messagingService = messagingService;
        this.replicaService = replicaService;
        this.clockService = clockService;
        this.transactionConfiguration = transactionConfiguration;
    }

    /**
     * Sends WriteIntentSwitch request to the specified primary replica.
     *
     * @param primaryConsistentId Primary replica to process given cleanup request.
     * @param partition Partition.
     * @param txId Transaction id.
     * @param commit {@code True} if a commit requested.
     * @param commitTimestamp Commit timestamp ({@code null} if it's an abort).
     * @return Completable future of ReplicaResponse.
     */
    public CompletableFuture<ReplicaResponse> switchWriteIntents(
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
                transactionConfiguration.rpcTimeout().value());
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
            TablePartitionId commitPartition,
            Map<ReplicationGroupId, PartitionEnlistment> enlistedPartitions,
            UUID txId,
            Long consistencyToken,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        TablePartitionIdMessage commitPartitionIdMessage = REPLICA_MESSAGES_FACTORY.tablePartitionIdMessage()
                .partitionId(commitPartition.partitionId())
                .tableId(commitPartition.tableId())
                .build();

        return replicaService.invoke(
                primaryConsistentId,
                TX_MESSAGES_FACTORY.txFinishReplicaRequest()
                        .txId(txId)
                        .commitPartitionId(commitPartitionIdMessage)
                        .timestamp(clockService.now())
                        // TODO Dirty hack within colocation track only. Remove after https://issues.apache.org/jira/browse/IGNITE-24343
                        .groupId(enabledColocation()
                                ? toReplicationGroupIdMessage(
                                REPLICA_MESSAGES_FACTORY, enlistedPartitions.entrySet().iterator().next().getKey())
                                : toTablePartitionIdMessage(REPLICA_MESSAGES_FACTORY, commitPartition))
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
            TablePartitionId commitGrpId,
            Long consistencyToken
    ) {
        return replicaService.invoke(
                primaryConsistentId,
                TX_MESSAGES_FACTORY.txStateCommitPartitionRequest()
                        .groupId(toTablePartitionIdMessage(REPLICA_MESSAGES_FACTORY, commitGrpId))
                        .txId(txId)
                        .enlistmentConsistencyToken(consistencyToken)
                        .build());
    }

    /**
     * Send TxStateCoordinatorRequest.
     *
     * @param primaryConsistentId Node id to send the request to.
     * @param txId Transaction id.
     * @param timestamp Timestamp to pass to target node.
     * @return Completable future of {@link TxStateResponse}.
     */
    public CompletableFuture<TxStateResponse> resolveTxStateFromCoordinator(
            String primaryConsistentId,
            UUID txId,
            HybridTimestamp timestamp
    ) {
        return messagingService.invoke(
                        primaryConsistentId,
                        TX_MESSAGES_FACTORY.txStateCoordinatorRequest()
                                .readTimestamp(timestamp)
                                .txId(txId)
                                .build(),
                        transactionConfiguration.rpcTimeout().value())
                .thenApply(resp -> {
                    assert resp instanceof TxStateResponse : "Unsupported response type [type=" + resp.getClass().getSimpleName() + ']';

                    return (TxStateResponse) resp;
                });
    }

    /**
     * Send TxCleanupRecoveryRequest.
     *
     * @param primaryConsistentId Node id to send the request to.
     * @param tablePartitionId Table partition id.
     * @return Completable future of ReplicaResponse.
     */
    public CompletableFuture<ReplicaResponse> sendRecoveryCleanup(String primaryConsistentId, TablePartitionId tablePartitionId) {
        return replicaService.invoke(
                primaryConsistentId,
                TX_MESSAGES_FACTORY.txCleanupRecoveryRequest()
                        .groupId(toTablePartitionIdMessage(REPLICA_MESSAGES_FACTORY, tablePartitionId))
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
