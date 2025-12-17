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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.tx.TxState.ABANDONED;
import static org.apache.ignite.internal.tx.TxState.FINISHING;
import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.apache.ignite.internal.tx.TxState.isFinalState;
import static org.apache.ignite.internal.tx.impl.PlacementDriverHelper.AWAIT_PRIMARY_REPLICA_TIMEOUT;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.TxStateMetaFinishing;
import org.apache.ignite.internal.tx.message.TransactionMetaMessage;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxStateCommitPartitionRequest;
import org.apache.ignite.internal.tx.message.TxStateCoordinatorRequest;
import org.jetbrains.annotations.Nullable;

/**
 * Helper class that allows to resolve transaction state mainly for the purpose of write intent resolution.
 */
public class TransactionStateResolver {
    /** Tx messages factory. */
    private static final TxMessagesFactory TX_MESSAGES_FACTORY = new TxMessagesFactory();

    /** Replica messages factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    // TODO https://issues.apache.org/jira/browse/IGNITE-20408 after this ticket this resolver will be no longer needed, as
    // TODO we will store coordinator as ClusterNode in local tx state map.
    /** Function that resolves a node consistent ID to a cluster node. */
    private final ClusterNodeResolver clusterNodeResolver;

    private final PlacementDriverHelper placementDriverHelper;

    private final Map<UUID, CompletableFuture<TransactionMeta>> txStateFutures = new ConcurrentHashMap<>();

    private final TxManager txManager;

    private final ClockService clockService;

    private final MessagingService messagingService;

    /**
     * Transaction message sender.
     */
    private final TxMessageSender txMessageSender;

    /**
     * The constructor.
     *
     * @param txManager Transaction manager.
     * @param clockService Clock service.
     * @param clusterNodeResolver Cluster node resolver.
     * @param messagingService Messaging service.
     * @param placementDriver Placement driver.
     */
    public TransactionStateResolver(
            TxManager txManager,
            ClockService clockService,
            ClusterNodeResolver clusterNodeResolver,
            MessagingService messagingService,
            PlacementDriver placementDriver,
            TxMessageSender txMessageSender
    ) {
        this.txManager = txManager;
        this.clockService = clockService;
        this.clusterNodeResolver = clusterNodeResolver;
        this.messagingService = messagingService;
        this.placementDriverHelper = new PlacementDriverHelper(placementDriver, clockService);
        this.txMessageSender = txMessageSender;
    }

    /**
     * This should be called in order to allow the transaction state resolver to listen to {@link TxStateCoordinatorRequest} messages.
     */
    public void start() {
        messagingService.addMessageHandler(TxMessageGroup.class, (msg, sender, correlationId) -> {
            if (msg instanceof TxStateCoordinatorRequest) {
                TxStateCoordinatorRequest req = (TxStateCoordinatorRequest) msg;

                processTxStateRequest(req)
                        .thenAccept(txStateMeta -> {
                            NetworkMessage response = TX_MESSAGES_FACTORY.txStateResponse()
                                    .txStateMeta(toTransactionMetaMessage(txStateMeta))
                                    .timestamp(clockService.now())
                                    .build();

                            messagingService.respond(sender, response, correlationId);
                        });
            }
        });
    }

    /**
     * Resolves transaction state locally, if possible, or distributively, if needed.
     *
     * @param txId Transaction id.
     * @param commitGrpId Commit partition group id.
     * @param timestamp Timestamp.
     * @param senderCurrentConsistencyToken See {@link TxStateCommitPartitionRequest#senderCurrentConsistencyToken()}.
     * @param senderGroupId See {@link TxStateCommitPartitionRequest#senderGroupId()}.
     * @return Future with the transaction state meta as a result.
     */
    public CompletableFuture<TransactionMeta> resolveTxState(
            UUID txId,
            ZonePartitionId commitGrpId,
            @Nullable HybridTimestamp timestamp,
            @Nullable Long senderCurrentConsistencyToken,
            @Nullable ZonePartitionId senderGroupId
    ) {
        return resolveTxState(
                txId,
                commitGrpId,
                timestamp,
                AWAIT_PRIMARY_REPLICA_TIMEOUT,
                SECONDS,
                senderCurrentConsistencyToken,
                senderGroupId
        );
    }

    /**
     * Resolves transaction state locally, if possible, or distributively, if needed.
     *
     * @param txId Transaction id.
     * @param commitGrpId Commit partition group id.
     * @param timestamp Timestamp.
     * @param awaitCommitPartitionAvailabilityTimeout Timeout for awaiting commit partition primary replica.
     * @param awaitCommitPartitionAvailabilityTimeUnit Time unit for awaiting commit partition primary replica timeout.
     * @param senderCurrentConsistencyToken See {@link TxStateCommitPartitionRequest#senderCurrentConsistencyToken()}.
     * @param senderGroupId See {@link TxStateCommitPartitionRequest#senderGroupId()}.
     * @return Future with the transaction state meta as a result.
     */
    public CompletableFuture<TransactionMeta> resolveTxState(
            UUID txId,
            ZonePartitionId commitGrpId,
            @Nullable HybridTimestamp timestamp,
            long awaitCommitPartitionAvailabilityTimeout,
            TimeUnit awaitCommitPartitionAvailabilityTimeUnit,
            @Nullable Long senderCurrentConsistencyToken,
            @Nullable ZonePartitionId senderGroupId
    ) {
        TxStateMeta localMeta = txManager.stateMeta(txId);

        if (localMeta != null && isFinalState(localMeta.txState())) {
            return completedFuture(localMeta);
        }

        CompletableFuture<TransactionMeta> future = txStateFutures.compute(txId, (k, v) -> {
            if (v == null) {
                v = new CompletableFuture<>();

                resolveDistributiveTxState(
                        txId,
                        localMeta,
                        commitGrpId,
                        timestamp,
                        awaitCommitPartitionAvailabilityTimeout,
                        awaitCommitPartitionAvailabilityTimeUnit,
                        senderCurrentConsistencyToken,
                        senderGroupId,
                        v
                );
            }

            return v;
        });

        future.whenComplete((v, e) -> txStateFutures.remove(txId));

        return future;
    }

    /**
     * Resolve the transaction state distributively. This method doesn't process final tx states.
     *
     * @param txId Transaction id.
     * @param localMeta Local tx meta.
     * @param commitGrpId Commit partition group id.
     * @param timestamp Timestamp to pass to target node.
     * @param awaitCommitPartitionAvailabilityTimeout Timeout for awaiting commit partition primary replica.
     * @param awaitCommitPartitionAvailabilityTimeUnit Time unit for awaiting commit partition primary replica timeout.
     * @param senderCurrentConsistencyToken See {@link TxStateCommitPartitionRequest#senderCurrentConsistencyToken()}.
     * @param senderGroupId See {@link TxStateCommitPartitionRequest#senderGroupId()}
     * @param txMetaFuture Tx meta future to complete with the result.
     */
    private void resolveDistributiveTxState(
            UUID txId,
            @Nullable TxStateMeta localMeta,
            ZonePartitionId commitGrpId,
            @Nullable HybridTimestamp timestamp,
            long awaitCommitPartitionAvailabilityTimeout,
            TimeUnit awaitCommitPartitionAvailabilityTimeUnit,
            @Nullable Long senderCurrentConsistencyToken,
            @Nullable ZonePartitionId senderGroupId,
            CompletableFuture<TransactionMeta> txMetaFuture
    ) {
        assert localMeta == null || !isFinalState(localMeta.txState()) : "Unexpected tx meta [txId" + txId + ", meta=" + localMeta + ']';

        HybridTimestamp timestamp0 = timestamp == null ? HybridTimestamp.MIN_VALUE : timestamp;

        if (localMeta == null) {
            // Fallback to commit partition path, because we don't have coordinator id.
            resolveTxStateFromCommitPartition(
                    txId,
                    commitGrpId,
                    awaitCommitPartitionAvailabilityTimeout,
                    awaitCommitPartitionAvailabilityTimeUnit,
                    txMetaFuture,
                    senderCurrentConsistencyToken,
                    senderGroupId
            );
        } else if (localMeta.txState() == PENDING) {
            resolveTxStateFromTxCoordinator(
                    txId,
                    localMeta.txCoordinatorId(),
                    commitGrpId,
                    timestamp0,
                    senderCurrentConsistencyToken,
                    senderGroupId,
                    txMetaFuture
            );
        } else if (localMeta.txState() == FINISHING) {
            assert localMeta instanceof TxStateMetaFinishing;

            ((TxStateMetaFinishing) localMeta).txFinishFuture().whenComplete((v, e) -> {
                if (e == null) {
                    txMetaFuture.complete(v);
                } else {
                    txMetaFuture.completeExceptionally(e);
                }
            });
        } else {
            assert localMeta.txState() == ABANDONED : "Unexpected transaction state [txId=" + txId + ", txStateMeta=" + localMeta + ']';

            // Still try to resolve the state from commit partition.
            resolveTxStateFromCommitPartition(txId,
                    commitGrpId,
                    senderCurrentConsistencyToken,
                    senderGroupId,
                    txMetaFuture
            );
        }
    }

    private void resolveTxStateFromTxCoordinator(
            UUID txId,
            @Nullable UUID coordinatorId,
            ZonePartitionId commitGrpId,
            HybridTimestamp timestamp,
            @Nullable Long senderCurrentConsistencyToken,
            @Nullable ZonePartitionId senderGroupId,
            CompletableFuture<TransactionMeta> txMetaFuture
    ) {
        updateLocalTxMapAfterDistributedStateResolved(txId, txMetaFuture);

        InternalClusterNode coordinator = coordinatorId == null ? null : clusterNodeResolver.getById(coordinatorId);
        if (coordinator == null) {
            // This means the coordinator node have either left the cluster or restarted.
            markAbandoned(txId);

            resolveTxStateFromCommitPartition(txId, commitGrpId, senderCurrentConsistencyToken, senderGroupId, txMetaFuture);
        } else {
            txMessageSender.resolveTxStateFromCoordinator(coordinator, txId, timestamp, senderCurrentConsistencyToken, senderGroupId)
                    .whenComplete((response, e) -> {
                        if (e == null && response.txStateMeta() != null) {
                            TransactionMetaMessage transactionMetaMessage = Objects.requireNonNull(response.txStateMeta(),
                                    "Transaction state meta must not be null after check.");
                            txMetaFuture.complete(transactionMetaMessage.asTransactionMeta());
                        } else {
                            if (e != null && e.getCause() instanceof RecipientLeftException) {
                                markAbandoned(txId);
                            }

                            resolveTxStateFromCommitPartition(
                                    txId,
                                    commitGrpId,
                                    senderCurrentConsistencyToken,
                                    senderGroupId,
                                    txMetaFuture
                            );
                        }
                    });
        }
    }

    private void resolveTxStateFromCommitPartition(
            UUID txId,
            ZonePartitionId commitGrpId,
            @Nullable Long senderCurrentConsistencyToken,
            @Nullable ZonePartitionId senderGroupId,
            CompletableFuture<TransactionMeta> txMetaFuture
    ) {
        resolveTxStateFromCommitPartition(
                txId,
                commitGrpId,
                AWAIT_PRIMARY_REPLICA_TIMEOUT,
                SECONDS,
                txMetaFuture,
                senderCurrentConsistencyToken,
                senderGroupId
        );
    }

    private void resolveTxStateFromCommitPartition(
            UUID txId,
            ZonePartitionId commitGrpId,
            long awaitPrimaryReplicaTimeout,
            TimeUnit awaitPrimaryReplicaTimeUnit,
            CompletableFuture<TransactionMeta> txMetaFuture,
            @Nullable Long senderCurrentConsistencyToken,
            @Nullable ZonePartitionId senderGroupId
    ) {
        updateLocalTxMapAfterDistributedStateResolved(txId, txMetaFuture);

        sendAndRetry(
                txMetaFuture,
                commitGrpId,
                txId,
                awaitPrimaryReplicaTimeout,
                awaitPrimaryReplicaTimeUnit,
                senderCurrentConsistencyToken,
                senderGroupId
        );
    }

    /**
     * Marks the transaction as abandoned due to the absence of coordinator.
     *
     * @param txId Transaction id.
     */
    private void markAbandoned(UUID txId) {
        txManager.updateTxMeta(txId, stateMeta -> stateMeta != null ? stateMeta.abandoned() : null);
    }

    private void updateLocalTxMapAfterDistributedStateResolved(UUID txId, CompletableFuture<TransactionMeta> future) {
        future.thenAccept(txMeta -> {
            if (txMeta instanceof TxStateMeta) {
                txManager.updateTxMeta(txId, old -> (TxStateMeta) txMeta);
            }
        });
    }

    /**
     * Tries to send a request to primary replica of the replication group.
     *
     * @param resFut Response future.
     * @param replicaGrp Replication group id.
     * @param txId Transaction id.
     * @param awaitPrimaryReplicaTimeout Primary replica await timeout.
     * @param awaitPrimaryReplicaTimeUnit Primary replica await timeout time unit.
     * @param senderCurrentConsistencyToken See {@link TxStateCommitPartitionRequest#senderCurrentConsistencyToken()}.
     * @param senderGroupId See {@link TxStateCommitPartitionRequest#senderGroupId()}
     */
    private void sendAndRetry(
            CompletableFuture<TransactionMeta> resFut,
            ZonePartitionId replicaGrp,
            UUID txId,
            long awaitPrimaryReplicaTimeout,
            TimeUnit awaitPrimaryReplicaTimeUnit,
            @Nullable Long senderCurrentConsistencyToken,
            @Nullable ZonePartitionId senderGroupId
    ) {
        placementDriverHelper.awaitPrimaryReplicaWithExceptionHandling(replicaGrp, awaitPrimaryReplicaTimeout, awaitPrimaryReplicaTimeUnit)
                .thenCompose(replicaMeta ->
                        txMessageSender.resolveTxStateFromCommitPartition(
                                replicaMeta.getLeaseholder(),
                                txId,
                                replicaGrp,
                                replicaMeta.getStartTime().longValue(),
                                senderCurrentConsistencyToken,
                                senderGroupId
                        ))
                .whenComplete((txMeta, e) -> {
                    if (e == null) {
                        assert txMeta != null : "Tx State response is null";

                        resFut.complete(txMeta);
                    } else {
                        if (e instanceof PrimaryReplicaMissException) {
                            sendAndRetry(
                                    resFut,
                                    replicaGrp,
                                    txId,
                                    awaitPrimaryReplicaTimeout,
                                    awaitPrimaryReplicaTimeUnit,
                                    senderCurrentConsistencyToken,
                                    senderGroupId
                            );
                        } else {
                            resFut.completeExceptionally(e);
                        }
                    }
                });
    }

    /**
     * Processes the transaction state requests that are used for coordinator path based write intent resolution. Can't return
     * {@link TxState#FINISHING}, it waits for actual completion instead.
     *
     * @param request Request.
     * @return Future that should be completed with transaction state meta.
     */
    private CompletableFuture<@Nullable TransactionMeta> processTxStateRequest(TxStateCoordinatorRequest request) {
        clockService.updateClock(request.readTimestamp());

        UUID txId = request.txId();

        TxStateMeta txStateMeta = txManager.stateMeta(txId);

        if (txStateMeta != null) {
            if (isFinalState(txStateMeta.txState())) {
                return completedFuture(txStateMeta);
            } else if (txStateMeta.txState() == FINISHING) {
                assert txStateMeta instanceof TxStateMetaFinishing;

                TxStateMetaFinishing txStateMetaFinishing = (TxStateMetaFinishing) txStateMeta;

                return txStateMetaFinishing.txFinishFuture();
            } else {
                InternalTransaction tx = txStateMeta.tx();
                Long currentConsistencyToken = request.senderCurrentConsistencyToken();
                ZonePartitionId groupId = request.senderGroupId() == null
                        ? null
                        : request.senderGroupId().asZonePartitionId();

                if (tx != null && !tx.isReadOnly() && currentConsistencyToken != null && groupId != null) {
                    return txManager.checkEnlistedPartitionsAndAbortIfNeeded(txStateMeta, tx, currentConsistencyToken, groupId);
                }
            }
        }

        return completedFuture(txStateMeta);
    }

    private static @Nullable TransactionMetaMessage toTransactionMetaMessage(@Nullable TransactionMeta transactionMeta) {
        return transactionMeta == null ? null : transactionMeta.toTransactionMetaMessage(REPLICA_MESSAGES_FACTORY, TX_MESSAGES_FACTORY);
    }
}
