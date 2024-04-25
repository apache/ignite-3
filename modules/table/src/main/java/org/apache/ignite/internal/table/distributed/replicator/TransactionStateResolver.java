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

package org.apache.ignite.internal.table.distributed.replicator;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.tx.TxState.ABANDONED;
import static org.apache.ignite.internal.tx.TxState.FINISHING;
import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.apache.ignite.internal.tx.TxState.isFinalState;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.TxStateMetaFinishing;
import org.apache.ignite.internal.tx.impl.PlacementDriverHelper;
import org.apache.ignite.internal.tx.impl.TxMessageSender;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxStateCoordinatorRequest;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeResolver;
import org.jetbrains.annotations.Nullable;

/**
 * Helper class that allows to resolve transaction state mainly for the purpose of write intent resolution.
 */
public class TransactionStateResolver {
    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

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
                            NetworkMessage response = FACTORY.txStateResponse()
                                    .txStateMeta(txStateMeta)
                                    .timestampLong(clockService.nowLong())
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
     * @return Future with the transaction state meta as a result.
     */
    public CompletableFuture<TransactionMeta> resolveTxState(
            UUID txId,
            TablePartitionId commitGrpId,
            @Nullable HybridTimestamp timestamp
    ) {
        TxStateMeta localMeta = txManager.stateMeta(txId);

        if (localMeta != null && isFinalState(localMeta.txState())) {
            return completedFuture(localMeta);
        }

        CompletableFuture<TransactionMeta> future = txStateFutures.compute(txId, (k, v) -> {
            if (v == null) {
                v = new CompletableFuture<>();

                resolveDistributiveTxState(txId, localMeta, commitGrpId, timestamp, v);
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
     * @param txMetaFuture Tx meta future to complete with the result.
     */
    private void resolveDistributiveTxState(
            UUID txId,
            @Nullable TxStateMeta localMeta,
            TablePartitionId commitGrpId,
            @Nullable HybridTimestamp timestamp,
            CompletableFuture<TransactionMeta> txMetaFuture
    ) {
        assert localMeta == null || !isFinalState(localMeta.txState()) : "Unexpected tx meta [txId" + txId + ", meta=" + localMeta + ']';

        HybridTimestamp timestamp0 = timestamp == null ? HybridTimestamp.MIN_VALUE : timestamp;

        if (localMeta == null) {
            // Fallback to commit partition path, because we don't have coordinator id.
            resolveTxStateFromCommitPartition(txId, commitGrpId, txMetaFuture);
        } else if (localMeta.txState() == PENDING) {
            resolveTxStateFromTxCoordinator(txId, localMeta.txCoordinatorId(), commitGrpId, timestamp0, txMetaFuture);
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
            resolveTxStateFromCommitPartition(txId, commitGrpId, txMetaFuture);
        }
    }

    private void resolveTxStateFromTxCoordinator(
            UUID txId,
            @Nullable String coordinatorId,
            TablePartitionId commitGrpId,
            HybridTimestamp timestamp,
            CompletableFuture<TransactionMeta> txMetaFuture
    ) {
        updateLocalTxMapAfterDistributedStateResolved(txId, txMetaFuture);

        ClusterNode coordinator = coordinatorId == null ? null : clusterNodeResolver.getById(coordinatorId);

        if (coordinator == null) {
            // This means the coordinator node have either left the cluster or restarted.
            markAbandoned(txId);

            resolveTxStateFromCommitPartition(txId, commitGrpId, txMetaFuture);
        } else {
            txMessageSender.resolveTxStateFromCoordinator(coordinator.name(), txId, timestamp)
                    .whenComplete((response, e) -> {
                        if (e == null) {
                            txMetaFuture.complete(response.txStateMeta());
                        } else {
                            resolveTxStateFromCommitPartition(txId, commitGrpId, txMetaFuture);
                        }
                    });
        }
    }

    private void resolveTxStateFromCommitPartition(
            UUID txId,
            TablePartitionId commitGrpId,
            CompletableFuture<TransactionMeta> txMetaFuture
    ) {
        updateLocalTxMapAfterDistributedStateResolved(txId, txMetaFuture);

        sendAndRetry(txMetaFuture, commitGrpId, txId);
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
     */
    private void sendAndRetry(CompletableFuture<TransactionMeta> resFut, TablePartitionId replicaGrp, UUID txId) {
        placementDriverHelper.awaitPrimaryReplicaWithExceptionHandling(replicaGrp)
                .thenCompose(replicaMeta ->
                        txMessageSender.resolveTxStateFromCommitPartition(
                                replicaMeta.getLeaseholder(),
                                txId,
                                replicaGrp,
                                replicaMeta.getStartTime().longValue()
                        ))
                .whenComplete((txMeta, e) -> {
                    if (e == null) {
                        assert txMeta != null : "Tx State response is null";

                        resFut.complete(txMeta);
                    } else {
                        if (e instanceof PrimaryReplicaMissException) {
                            sendAndRetry(resFut, replicaGrp, txId);
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
    private CompletableFuture<TransactionMeta> processTxStateRequest(TxStateCoordinatorRequest request) {
        clockService.updateClock(request.readTimestamp());

        UUID txId = request.txId();

        TxStateMeta txStateMeta = txManager.stateMeta(txId);

        if (txStateMeta != null && txStateMeta.txState() == FINISHING) {
            assert txStateMeta instanceof TxStateMetaFinishing;

            TxStateMetaFinishing txStateMetaFinishing = (TxStateMetaFinishing) txStateMeta;

            return txStateMetaFinishing.txFinishFuture();
        } else {
            return completedFuture(txStateMeta);
        }
    }
}
