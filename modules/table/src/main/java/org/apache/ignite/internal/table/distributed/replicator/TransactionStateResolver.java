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

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.TxStateMetaFinishing;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxStateCommitPartitionRequest;
import org.apache.ignite.internal.tx.message.TxStateCoordinatorRequest;
import org.apache.ignite.internal.tx.message.TxStateResponse;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkMessage;
import org.jetbrains.annotations.Nullable;

/**
 * Placement driver.
 */
public class TransactionStateResolver {
    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    /** Network timeout. */
    private static final long RPC_TIMEOUT = 3000;

    /** Assignment node names per replication group. */
    private final Map<ReplicationGroupId, LinkedHashSet<String>> primaryReplicaMapping = new ConcurrentHashMap<>();

    /** Replication service. */
    private final ReplicaService replicaService;

    /** Function that resolves a node consistent ID to a cluster node. */
    private final Function<String, ClusterNode> clusterNodeResolver;

    // TODO https://issues.apache.org/jira/browse/IGNITE-20408 after this ticket this resolver will be no longer needed, as
    // TODO we will store coordinator as ClusterNode in local tx state map.
    /** Function that resolves a node non-consistent ID to a cluster node. */
    private final Function<String, ClusterNode> clusterNodeResolverById;

    private final Map<UUID, CompletableFuture<TransactionMeta>> txStateFutures = new ConcurrentHashMap<>();

    private final TxManager txManager;

    private final HybridClock clock;

    private final MessagingService messagingService;

    /**
     * The constructor.
     *
     * @param replicaService Replication service.
     * @param txManager Transaction manager.
     * @param clock Node clock.
     * @param clusterNodeResolver Cluster node resolver.
     * @param clusterNodeResolverById Cluster node resolver using non-consistent id.
     * @param messagingService Messaging service.
     */
    public TransactionStateResolver(
            ReplicaService replicaService,
            TxManager txManager,
            HybridClock clock,
            Function<String, ClusterNode> clusterNodeResolver,
            Function<String, ClusterNode> clusterNodeResolverById,
            MessagingService messagingService
    ) {
        this.replicaService = replicaService;
        this.txManager = txManager;
        this.clock = clock;
        this.clusterNodeResolver = clusterNodeResolver;
        this.clusterNodeResolverById = clusterNodeResolverById;
        this.messagingService = messagingService;
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
                                    .timestampLong(clock.nowLong())
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
            ReplicationGroupId commitGrpId,
            HybridTimestamp timestamp
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
            ReplicationGroupId commitGrpId,
            HybridTimestamp timestamp,
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

            txMetaFuture.complete(localMeta);
        }
    }

    private void resolveTxStateFromTxCoordinator(
            UUID txId,
            String coordinatorId,
            ReplicationGroupId commitGrpId,
            HybridTimestamp timestamp,
            CompletableFuture<TransactionMeta> txMetaFuture
    ) {
        updateLocalTxMapAfterDistributedStateResolved(txId, txMetaFuture);

        ClusterNode coordinator = clusterNodeResolverById.apply(coordinatorId);

        if (coordinator == null) {
            // This means the coordinator node have either left the cluster or restarted.
            resolveTxStateFromCommitPartition(txId, commitGrpId, txMetaFuture);
        } else {
            CompletableFuture<TransactionMeta> coordinatorTxMetaFuture = new CompletableFuture<>();

            coordinatorTxMetaFuture.whenComplete((v, e) -> {
                assert v != null : "Unexpected result from transaction coordinator: unknown transaction state [txId=" + txId
                        + ", transactionCoordinator=" + coordinator + ']';

                if (e == null) {
                    txMetaFuture.complete(v);
                } else {
                    txMetaFuture.completeExceptionally(e);
                }
            });

            TxStateCoordinatorRequest request = FACTORY.txStateCoordinatorRequest()
                    .readTimestampLong(timestamp.longValue())
                    .txId(txId)
                    .build();

            sendAndRetry(coordinatorTxMetaFuture, coordinator, request);
        }
    }

    private void resolveTxStateFromCommitPartition(
            UUID txId,
            ReplicationGroupId commitGrpId,
            CompletableFuture<TransactionMeta> txMetaFuture
    ) {
        TxStateCommitPartitionRequest request = FACTORY.txStateCommitPartitionRequest()
                .groupId(commitGrpId)
                .txId(txId)
                .build();

        updateLocalTxMapAfterDistributedStateResolved(txId, txMetaFuture);

        sendAndRetry(txMetaFuture, commitGrpId, request);
    }

    private void updateLocalTxMapAfterDistributedStateResolved(UUID txId, CompletableFuture<TransactionMeta> future) {
        future.thenAccept(txMeta -> {
            if (txMeta != null && txMeta instanceof TxStateMeta) {
                txManager.updateTxMeta(txId, old -> (TxStateMeta) txMeta);
            }
        });
    }

    /**
     * Updates an assignment for the specific replication group.
     *
     * @param replicaGrpId Replication group id.
     * @param nodeNames Assignment node names.
     */
    public void updateAssignment(ReplicationGroupId replicaGrpId, Collection<String> nodeNames) {
        primaryReplicaMapping.put(replicaGrpId, new LinkedHashSet<>(nodeNames));
    }

    /**
     * Tries to send a request to primary replica of the replication group.
     * If the first node turns up not a primary one the logic sends the same request to a new primary node.
     *
     * @param resFut Response future.
     * @param replicaGrp Replication group id.
     * @param request Request.
     */
    private void sendAndRetry(
            CompletableFuture<TransactionMeta> resFut,
            ReplicationGroupId replicaGrp,
            TxStateCommitPartitionRequest request
    ) {
        ClusterNode nodeToSend = primaryReplicaMapping.get(replicaGrp).stream()
                .map(clusterNodeResolver)
                .filter(Objects::nonNull)
                .findFirst()
                .orElseThrow(() -> new IgniteInternalException("All replica nodes are unavailable"));

        replicaService.invoke(nodeToSend, request).thenAccept(resp -> {
            assert resp instanceof LeaderOrTxState : "Unsupported response type [type=" + resp.getClass().getSimpleName() + ']';

            LeaderOrTxState stateOrLeader = (LeaderOrTxState) resp;

            String nextNodeToSend = stateOrLeader.leaderName();

            if (nextNodeToSend == null) {
                resFut.complete(stateOrLeader.txMeta());
            } else {
                LinkedHashSet<String> newAssignment = new LinkedHashSet<>();

                newAssignment.add(nextNodeToSend);
                newAssignment.addAll(primaryReplicaMapping.get(replicaGrp));

                primaryReplicaMapping.put(replicaGrp, newAssignment);

                sendAndRetry(resFut, replicaGrp, request);
            }
        });
    }

    /**
     * Tries to send a request to the given node.
     *
     * @param resFut Response future.
     * @param node Node to send to.
     * @param request Request.
     */
    private void sendAndRetry(CompletableFuture<TransactionMeta> resFut, ClusterNode node, TxStateCoordinatorRequest request) {
        messagingService.invoke(node, request, RPC_TIMEOUT).thenAccept(resp -> {
            assert resp instanceof TxStateResponse : "Unsupported response type [type=" + resp.getClass().getSimpleName() + ']';

            TxStateResponse response = (TxStateResponse) resp;

            resFut.complete(response.txStateMeta());
        });
    }

    /**
     * Processes the transaction state requests that are used for coordinator path based write intent resolution. Can't return
     * {@link org.apache.ignite.internal.tx.TxState#FINISHING}, it waits for actual completion instead.
     *
     * @param request Request.
     * @return Future that should be completed with transaction state meta.
     */
    private CompletableFuture<TransactionMeta> processTxStateRequest(TxStateCoordinatorRequest request) {
        clock.update(request.readTimestamp());

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
