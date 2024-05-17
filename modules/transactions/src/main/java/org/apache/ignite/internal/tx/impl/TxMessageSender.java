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
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaResponse;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TransactionResult;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxStateResponse;
import org.jetbrains.annotations.Nullable;

/**
 * This class is responsible for interacting with the messaging layer. Sends transaction messages.
 */
public class TxMessageSender {
    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

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
     * @param zonePartitionId Zone partition id.
     * @param txId Transaction id.
     * @param commit {@code True} if a commit requested.
     * @param commitTimestamp Commit timestamp ({@code null} if it's an abort).
     * @return Completable future of ReplicaResponse.
     */
    public CompletableFuture<ReplicaResponse> switchWriteIntents(
            String primaryConsistentId,
            ZonePartitionId zonePartitionId,
            UUID txId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        return replicaService.invoke(
                primaryConsistentId,
                FACTORY.writeIntentSwitchReplicaRequest()
                        .groupId(zonePartitionId)
                        .timestampLong(clockService.nowLong())
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
     * @return Completable future of {@link NetworkMessage}.
     */
    public CompletableFuture<NetworkMessage> cleanup(
            String primaryConsistentId,
            @Nullable Collection<ReplicationGroupId> replicationGroupIds,
            UUID txId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        return messagingService.invoke(
                primaryConsistentId,
                FACTORY.txCleanupMessage()
                        .txId(txId)
                        .commit(commit)
                        .commitTimestampLong(hybridTimestampToLong(commitTimestamp))
                        .timestampLong(clockService.nowLong())
                        .groups(replicationGroupIds)
                        .build(),
                transactionConfiguration.rpcTimeout().value());
    }

    /**
     * Send a transactions finish request.
     *
     * @param primaryConsistentId Node consistent id to send the request to.
     * @param commitPartition Partition to store a transaction state.
     * @param replicationGroupIds Enlisted partition groups.
     * @param txId Transaction id.
     * @param consistencyToken Enlistment consistency token.
     * @param commit {@code true} if a commit requested.
     * @param commitTimestamp Commit timestamp ({@code null} if it's an abort).
     * @return Completable future of {@link TransactionResult}.
     */
    public CompletableFuture<TransactionResult> finish(
            String primaryConsistentId,
            ZonePartitionId commitPartition,
            Map<ReplicationGroupId, String> replicationGroupIds,
            UUID txId,
            Long consistencyToken,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        return replicaService.invoke(
                primaryConsistentId,
                FACTORY.txFinishReplicaRequest()
                        .txId(txId)
                        .timestampLong(clockService.nowLong())
                        .groupId(commitPartition)
                        .groups(replicationGroupIds)
                        .commit(commit)
                        .commitTimestampLong(hybridTimestampToLong(commitTimestamp))
                        .enlistmentConsistencyToken(consistencyToken)
                        .build());
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
            ZonePartitionId commitGrpId,
            Long consistencyToken
    ) {
        return replicaService.invoke(
                primaryConsistentId,
                FACTORY.txStateCommitPartitionRequest()
                        .groupId(commitGrpId)
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
                        FACTORY.txStateCoordinatorRequest()
                                .readTimestampLong(timestamp.longValue())
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
                FACTORY.txCleanupRecoveryRequest()
                        .groupId(tablePartitionId)
                        .build()
        );
    }

    public MessagingService messagingService() {
        return messagingService;
    }
}
