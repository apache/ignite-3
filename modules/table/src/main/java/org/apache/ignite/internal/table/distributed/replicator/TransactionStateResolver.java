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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxStateReplicaRequest;
import org.apache.ignite.internal.tx.message.TxStateRequest;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.MessagingService;
import org.jetbrains.annotations.Nullable;

/**
 * Placement driver.
 */
public class TransactionStateResolver {
    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    /** Assignment node names per replication group. */
    private final Map<ReplicationGroupId, LinkedHashSet<String>> primaryReplicaMapping = new ConcurrentHashMap<>();

    /** Replication service. */
    private final ReplicaService replicaService;

    /** Function that resolves a node consistent ID to a cluster node. */
    private final Function<String, ClusterNode> clusterNodeResolver;

    private final Map<UUID, CompletableFuture<TxStateMeta>> txStateFutures = new ConcurrentHashMap<>();

    private final Lazy<String> localNodeId;

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
     * @param localNodeIdSupplier Local node id supplier.
     */
    public TransactionStateResolver(
            ReplicaService replicaService,
            TxManager txManager,
            HybridClock clock,
            Function<String, ClusterNode> clusterNodeResolver,
            Supplier<String> localNodeIdSupplier,
            MessagingService messagingService
    ) {
        this.replicaService = replicaService;
        this.txManager = txManager;
        this.clock = clock;
        this.clusterNodeResolver = clusterNodeResolver;
        this.localNodeId = new Lazy<>(localNodeIdSupplier);

        messagingService.addMessageHandler(TxMessageGroup.class, (msg, sender, correlationId) -> {
            if (msg instanceof TxStateRequest) {
                processTxStateRequest((TxStateRequest) msg)
                        .thenAccept(txStateMeta -> {
                            Object response = REPLICA_MESSAGES_FACTORY
                                    .timestampAwareReplicaResponse()
                                    .result(result)
                                    .timestampLong(sendTimestamp.longValue())
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
    public CompletableFuture<TxStateMeta> resolveTxState(
            UUID txId,
            ReplicationGroupId commitGrpId,
            HybridTimestamp timestamp
    ) {
        TxStateMeta localMeta = txManager.stateMeta(txId);

        if (localMeta != null) {
            if (isFinalState(localMeta.txState())) {
                return completedFuture(localMeta);
            }

            // If the local node is a tx coordinator:
            // if tx state is FINISHING, we will have to wait for the actual finish, otherwise we can return the current state.
            if (localNodeId.get().equals(localMeta.txCoordinatorId()) && localMeta.txState() != FINISHING) {
                return completedFuture(localMeta);
            }
        }

        return txStateFutures.computeIfAbsent(txId, k -> {
            CompletableFuture<TxStateMeta> future = new CompletableFuture<>();

            resolveDistributiveTxState(txId, localMeta, commitGrpId, timestamp, future);

            return future
                    .thenApply(txStateMeta -> {
                        // TODO IGNITE-20034 is it needed?
                        txManager.updateTxMeta(txId, old -> txStateMeta);

                        txStateFutures.remove(txId);

                        return txStateMeta;
                    });
        });
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
            CompletableFuture<TxStateMeta> txMetaFuture
    ) {
        assert localMeta == null || !isFinalState(localMeta.txState()) : "Unexpected tx meta [txId" + txId + ", meta=" + localMeta + ']';

        HybridTimestamp timestamp0 = timestamp == null ? HybridTimestamp.MIN_VALUE : timestamp;

        if (localMeta == null) {
            // Fallback to commit partition path, because we don't have coordinator id.
            resolveTxStateFromCommitPartition(txId, commitGrpId, timestamp0, txMetaFuture);
        } else if (localMeta.txState() == PENDING) {
            resolveTxStateFromTxCoordinator(txId, localMeta.txCoordinatorId(), commitGrpId, timestamp0, txMetaFuture);
        } else if (localMeta.txState() == FINISHING) {
            // No op. This means we are on tx coordinator and the future should be completed via #notifyFinished().
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
            CompletableFuture<TxStateMeta> txMetaFuture
    ) {
        ClusterNode coordinator = clusterNodeResolver.apply(coordinatorId);

        if (coordinator == null) {
            // This means the coordinator node have either left the cluster or restarted.
            resolveTxStateFromCommitPartition(txId, commitGrpId, timestamp, txMetaFuture);
        } else {
            TxStateReplicaRequest request = FACTORY.txStateReplicaRequest()
                    .readTimestampLong(timestamp.longValue())
                    .txId(txId)
                    .build();

            sendAndRetry(txMetaFuture, coordinator, request);
        }
    }

    private void resolveTxStateFromCommitPartition(
            UUID txId,
            ReplicationGroupId commitGrpId,
            HybridTimestamp timestamp,
            CompletableFuture<TxStateMeta> txMetaFuture
    ) {
        TxStateReplicaRequest request = FACTORY.txStateReplicaRequest()
                .groupId(commitGrpId)
                .readTimestampLong(timestamp.longValue())
                .txId(txId)
                .build();

        sendAndRetry(txMetaFuture, commitGrpId, request);
    }

    /**
     * Sends a transaction state request to the primary replica.
     *
     * @param replicaGrp Replication group id.
     * @param request Status request.
     * @return Result future.
     */
    public CompletableFuture<TxMeta> sendMetaRequest(ReplicationGroupId replicaGrp, TxStateReplicaRequest request) {
        CompletableFuture<TxMeta> resFut = new CompletableFuture<>();

        //sendAndRetry(resFut, replicaGrp, request);

        return resFut;
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
    private void sendAndRetry(CompletableFuture<TxStateMeta> resFut, ReplicationGroupId replicaGrp, TxStateReplicaRequest request) {
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
                //resFut.complete(stateOrLeader.txMeta());
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
    private void sendAndRetry(CompletableFuture<TxStateMeta> resFut, ClusterNode node, TxStateReplicaRequest request) {
        replicaService.invoke(node, request).thenAccept(resp -> {
            assert resp instanceof LeaderOrTxState : "Unsupported response type [type=" + resp.getClass().getSimpleName() + ']';

            LeaderOrTxState stateOrLeader = (LeaderOrTxState) resp;

            assert stateOrLeader.leaderName() == null : "Unexpected type of result while requesting the transaction state from "
                    + "the certain node [txId=" + request.txId() + ", node=" + node + ", response=" + resp + ']';

            //resFut.complete(stateOrLeader.txMeta());
        });
    }

    /**
     * Notifies this transaction state resolver that the transaction has been finished. It is required to finish the transactions
     * that may be in {@link org.apache.ignite.internal.tx.TxState#FINISHING} state. it is intended to be called only on the
     * transaction coordinator. It doesn't update local transaction state map.
     *
     * @param txId Transaction id.
     * @param txMeta Transaction meta.
     */
    public void notifyFinished(UUID txId, TxStateMeta txMeta) {
        assert localNodeId.get().equals(txMeta.txCoordinatorId()) : "This must be called only on the transaction coordinator";

        txStateFutures.compute(txId, (k, v) -> {
            if (v != null) {
                v.complete(txMeta);
            }

            return null;
        });
    }

    private CompletableFuture<TxStateMeta> processTxStateRequest(TxStateRequest request) {
        clock.update(request.readTimestamp());

        UUID txId = request.txId();

        TxStateMeta txStateMeta = txManager.stateMeta(txId);

        if (txStateMeta.txState() == FINISHING) {
            AtomicReference<CompletableFuture<TxStateMeta>> futRef = new AtomicReference<>();

            txStateFutures.computeIfAbsent(txId, k -> {
                TxStateMeta meta = txManager.stateMeta(txId);

                if (meta.txState() != FINISHING) {
                    futRef.set(completedFuture(meta));

                    return null;
                }

                futRef.set(new CompletableFuture<>());

                return futRef.get();
            });

            return futRef.get();
        } else {
            return completedFuture(txStateMeta);
        }
    }
}
