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

package org.apache.ignite.internal.partition.replicator.handlers;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.tx.TxState.ABANDONED;
import static org.apache.ignite.internal.tx.TxState.FINISHING;
import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.apache.ignite.internal.tx.TxState.isFinalState;
import static org.apache.ignite.internal.tx.TxStateMeta.finalizeState;
import static org.apache.ignite.internal.tx.impl.PlacementDriverHelper.AWAIT_PRIMARY_REPLICA_TIMEOUT;
import static org.apache.ignite.internal.tx.impl.TxStateResolutionParameters.txStateResolutionParameters;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.partition.replicator.TxRecoveryEngine;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.TxStateMetaFinishing;
import org.apache.ignite.internal.tx.impl.PlacementDriverHelper;
import org.apache.ignite.internal.tx.impl.TxMessageSender;
import org.apache.ignite.internal.tx.message.TxStateCommitPartitionRequest;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.jetbrains.annotations.Nullable;

/**
 * Handles {@link TxStateCommitPartitionRequest}s.
 */
public class TxStateCommitPartitionReplicaRequestHandler {
    private final TxStatePartitionStorage txStatePartitionStorage;
    private final TxManager txManager;
    private final ClusterNodeResolver clusterNodeResolver;

    private final InternalClusterNode localNode;

    private final TxRecoveryEngine txRecoveryEngine;

    private final TxMessageSender txMessageSender;

    private final PlacementDriverHelper placementDriverHelper;

    /** Constructor. */
    public TxStateCommitPartitionReplicaRequestHandler(
            TxStatePartitionStorage txStatePartitionStorage,
            TxManager txManager,
            ClusterNodeResolver clusterNodeResolver,
            InternalClusterNode localNode,
            TxRecoveryEngine txRecoveryEngine,
            TxMessageSender txMessageSender,
            PlacementDriverHelper placementDriverHelper
    ) {
        this.txStatePartitionStorage = txStatePartitionStorage;
        this.txManager = txManager;
        this.clusterNodeResolver = clusterNodeResolver;
        this.localNode = localNode;
        this.txRecoveryEngine = txRecoveryEngine;
        this.txMessageSender = txMessageSender;
        this.placementDriverHelper = placementDriverHelper;
    }

    /**
     * Handles a {@link TxStateCommitPartitionRequest}.
     *
     * @param request Transaction state request.
     * @return Result future.
     */
    public CompletableFuture<TransactionMeta> handle(TxStateCommitPartitionRequest request) {
        UUID txId = request.txId();

        TxStateMeta txMeta = txManager.stateMeta(txId);

        if (txMeta != null && txMeta.txState() == FINISHING) {
            assert txMeta instanceof TxStateMetaFinishing : txMeta;

            return ((TxStateMetaFinishing) txMeta).txFinishFuture();
        } else if (txMeta == null || !isFinalState(txMeta.txState())) {
            // Try to trigger recovery, if needed. If the transaction will be aborted, the proper ABORTED state will be sent
            // in response.
            ZonePartitionId senderGroupId = request.senderGroupId().asReplicationGroupId();

            return triggerTxRecoveryOnTxStateResolutionIfNeeded(
                    txId,
                    txMeta,
                    request.readTimestamp(),
                    request.senderCurrentConsistencyToken(),
                    senderGroupId,
                    request.rowId().asRowId(),
                    request.newestCommitTimestamp()
            );
        } else {
            return completedFuture(txMeta);
        }
    }

    /**
     * Checks whether tx recovery is needed with given tx meta and triggers it of needed.
     *
     * @param txId Transaction id.
     * @param txStateMeta Transaction meta.
     * @param readTimestamp If the recovery is triggered by RO transaction, the the read timestamp of that transaction,
     *                     otherwise {@code null}.
     * @param senderCurrentConsistencyToken See {@link TxStateCommitPartitionRequest#senderCurrentConsistencyToken()}.
     * @param senderGroupId See {@link TxStateCommitPartitionRequest#senderGroupId()}.
     * @param rowId Row id.
     * @param newestCommitTimestamp Newest commit timestamp.
     * @return Tx recovery future, or completed future if the recovery isn't needed, or failed future if the recovery is not possible.
     */
    private CompletableFuture<TransactionMeta> triggerTxRecoveryOnTxStateResolutionIfNeeded(
            UUID txId,
            @Nullable TxStateMeta txStateMeta,
            @Nullable HybridTimestamp readTimestamp,
            @Nullable Long senderCurrentConsistencyToken,
            ZonePartitionId senderGroupId,
            @Nullable RowId rowId,
            @Nullable HybridTimestamp newestCommitTimestamp
    ) {
        // The state is either null or PENDING or ABANDONED, other states have been filtered out previously.lo
        assert txStateMeta == null || txStateMeta.txState() == PENDING || txStateMeta.txState() == ABANDONED
                : "Unexpected transaction state: " + txStateMeta;

        TxMeta txMetaPersistent = txStatePartitionStorage.get(txId);

        if (txMetaPersistent == null) {
            InternalClusterNode coordinator = (txStateMeta == null || txStateMeta.txCoordinatorId() == null)
                    ? null
                    : clusterNodeResolver.getById(txStateMeta.txCoordinatorId());

            if (txStateMeta == null) {
                // This means one of two:
                // - txn is not finished, volatile state is lost
                // - txn was finished, state was vacuumized
                // both mean primary replica resolution path.
                return resolveTxStateFromPrimaryReplica(
                        txId,
                        senderGroupId,
                        senderCurrentConsistencyToken,
                        readTimestamp,
                        rowId,
                        newestCommitTimestamp
                );
            } else if (txStateMeta.txState() == ABANDONED
                    || txStateMeta.txCoordinatorId() == null
                    || coordinator == null) {
                // This means the transaction is pending and we should trigger the recovery if there is no tx coordinator in topology.
                // We can assume that as the coordinator (or information about it) is missing, there is no need to wait for
                // a finish request from tx coordinator, the transaction can't be committed at all.
                return txRecoveryEngine.triggerTxRecovery(txId, localNode.id())
                        .handle((v, ex) ->
                                CompletableFuture.<TransactionMeta>completedFuture(txManager.stateMeta(txId)))
                        .thenCompose(Function.identity());
            } else if (coordinator != null) {
                // If there is coordinator in the cluster we should fallback to coordinator request. It's possible that coordinator
                // was not seen in topology on another node which requested the state from commit partition, but can be seen here.
                HybridTimestamp timestamp = readTimestamp == null ? HybridTimestamp.MIN_VALUE : readTimestamp;

                return txMessageSender.resolveTxStateFromCoordinator(
                                txStateResolutionParameters().txId(txId)
                                        .readTimestamp(timestamp)
                                        .senderGroupId(senderGroupId)
                                        .senderCurrentConsistencyToken(senderCurrentConsistencyToken)
                                        .build(),
                                coordinator
                        )
                        .handle((response, e) -> {
                            if (e == null) {
                                if (response.txStateMeta() == null) {
                                    // TODO https://issues.apache.org/jira/browse/IGNITE-27494 should be fixed correctly by
                                    // TODO WI resolution primary replica path.
                                    // This may be possible if tx cleanup command was already committed in partition's replication group,
                                    // and tx state was vacuumized on coordinator. This transaction already had a final state,
                                    // but tx cleanup was not applied on replica yet due to replication lag. To prevent switching of
                                    // write intent on replica side (because we don't know the final state), we respond with PENDING state.
                                    return completedFuture((TransactionMeta) TxStateMeta.builder(PENDING).build());
                                } else {
                                    return completedFuture(response.txStateMeta().asTransactionMeta());
                                }
                            } else {
                                if (e.getCause() instanceof RecipientLeftException) {
                                    markAbandoned(txId);
                                }

                                return txRecoveryEngine.triggerTxRecovery(txId, localNode.id())
                                        .handle((v, ex) ->
                                                CompletableFuture.<TransactionMeta>completedFuture(txManager.stateMeta(txId)))
                                        .thenCompose(Function.identity());
                            }
                        })
                        .thenCompose(Function.identity());
            } else {
                assert txStateMeta != null && txStateMeta.txState() == PENDING : "Unexpected transaction state: " + txStateMeta;

                return completedFuture(txStateMeta);
            }
        } else {
            // Recovery is not needed. Persistent meta always contains final state.
            // TODO https://issues.apache.org/jira/browse/IGNITE-27494 Add UNKNOWN state handling.
            assert isFinalState(txMetaPersistent.txState()) : "Unexpected transaction state: " + txMetaPersistent;

            return completedFuture(txMetaPersistent);
        }
    }

    private CompletableFuture<TransactionMeta> resolveTxStateFromPrimaryReplica(
            UUID txId,
            ZonePartitionId senderGroupId,
            @Nullable Long senderCurrentConsistencyToken,
            @Nullable HybridTimestamp readTimestamp,
            RowId rowId,
            @Nullable HybridTimestamp newestCommitTimestamp
    ) {
        return placementDriverHelper.awaitPrimaryReplicaWithExceptionHandling(
                        senderGroupId,
                        AWAIT_PRIMARY_REPLICA_TIMEOUT,
                        SECONDS
                )
                .thenCompose(replicaMeta -> {
                        long consistencyToken = replicaMeta.getStartTime().longValue();

                        if (senderCurrentConsistencyToken != null) {
                            if (consistencyToken == senderCurrentConsistencyToken) {
                                // This is request from actual primary
                                // (probably the primary was moved to the sender node after it did the request or whatever).
                                if (readTimestamp == null || readTimestamp == HybridTimestamp.MIN_VALUE) {
                                    // The request doesn't have read timestamp - this means it's from RW txn.
                                    // Sender node is current primary, it has the most recent state of the row, and there is WI
                                    // so it was not cleaned up on group majority - this means the txn was never finished
                                    // and can be aborted.
                                    return txRecoveryEngine.triggerTxRecovery(txId, localNode.id())
                                            .handle((v, ex) ->
                                                    CompletableFuture.<TransactionMeta>completedFuture(txManager.stateMeta(txId)))
                                            .thenCompose(Function.identity());
                                }
                            } else {
                                // The primary replica that sent the request has already expired.
                                if (readTimestamp == null || readTimestamp == HybridTimestamp.MIN_VALUE) {
                                    // The request doesn't have read timestamp - this means it's from RW txn.
                                    // Respond with error, sender must abort its operation and respond with error to client
                                    // (primary changed, the current transaction will not be able to be committed).
                                    // TODO
                                    throw new RuntimeException();
                                }
                            }
                        }

                        String primaryNode = replicaMeta.getLeaseholder();

                        return txMessageSender.resolveTxStateFromPrimaryReplica(
                                txStateResolutionParameters()
                                        .txId(txId)
                                        .senderGroupId(senderGroupId)
                                        .rowIdAndNewestCommitTimestamp(rowId, newestCommitTimestamp)
                                        .build(),
                                primaryNode,
                                consistencyToken
                        );
                })
                .whenComplete((txMeta, e) -> {
                    if (e == null && isFinalState(txMeta.txState())) {
                        // Cache tx state in case of other tx state resolution requests.
                        // Cleanup completion timestamp is set because write intents were switched on majority,
                        // so it can be vacuumized.
                        txManager.updateTxMeta(
                                txId,
                                old -> finalizeState(old, txMeta.txState(), txMeta.commitTimestamp(), coarseCurrentTimeMillis())
                        );
                    }
                });
    }

    /**
     * Marks the transaction as abandoned due to the absence of coordinator.
     *
     * @param txId Transaction id.
     */
    private void markAbandoned(UUID txId) {
        txManager.updateTxMeta(txId, stateMeta -> stateMeta != null ? stateMeta.abandoned() : null);
    }
}
