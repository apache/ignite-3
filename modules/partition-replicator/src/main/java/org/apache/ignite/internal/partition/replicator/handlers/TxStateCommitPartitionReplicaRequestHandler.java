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
import static org.apache.ignite.internal.tx.TxState.ABANDONED;
import static org.apache.ignite.internal.tx.TxState.FINISHING;
import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.apache.ignite.internal.tx.TxState.isFinalState;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.partition.replicator.TxRecoveryEngine;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.TxStateMetaFinishing;
import org.apache.ignite.internal.tx.impl.TxMessageSender;
import org.apache.ignite.internal.tx.message.TransactionMetaMessage;
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

    /** Constructor. */
    public TxStateCommitPartitionReplicaRequestHandler(
            TxStatePartitionStorage txStatePartitionStorage,
            TxManager txManager,
            ClusterNodeResolver clusterNodeResolver,
            InternalClusterNode localNode,
            TxRecoveryEngine txRecoveryEngine,
            TxMessageSender txMessageSender
    ) {
        this.txStatePartitionStorage = txStatePartitionStorage;
        this.txManager = txManager;
        this.clusterNodeResolver = clusterNodeResolver;
        this.localNode = localNode;
        this.txRecoveryEngine = txRecoveryEngine;
        this.txMessageSender = txMessageSender;
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
            ZonePartitionId zonePartitionId = request.senderGroupId() == null ? null : request.senderGroupId().asReplicationGroupId();

            return triggerTxRecoveryOnTxStateResolutionIfNeeded(
                    txId,
                    txMeta,
                    request.readTimestamp(),
                    request.senderCurrentConsistencyToken(),
                    zonePartitionId
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
     * @return Tx recovery future, or completed future if the recovery isn't needed, or failed future if the recovery is not possible.
     */
    private CompletableFuture<TransactionMeta> triggerTxRecoveryOnTxStateResolutionIfNeeded(
            UUID txId,
            @Nullable TxStateMeta txStateMeta,
            @Nullable HybridTimestamp readTimestamp,
            @Nullable Long senderCurrentConsistencyToken,
            @Nullable ZonePartitionId senderGroupId
    ) {
        // The state is either null or PENDING or ABANDONED, other states have been filtered out previously.
        assert txStateMeta == null || txStateMeta.txState() == PENDING || txStateMeta.txState() == ABANDONED
                : "Unexpected transaction state: " + txStateMeta;

        TxMeta txMeta = txStatePartitionStorage.get(txId);

        if (txMeta == null) {
            InternalClusterNode coordinator = (txStateMeta == null || txStateMeta.txCoordinatorId() == null)
                    ? null
                    : clusterNodeResolver.getById(txStateMeta.txCoordinatorId());

            // This means the transaction is pending and we should trigger the recovery if there is no tx coordinator in topology.
            if (txStateMeta == null
                    || txStateMeta.txState() == ABANDONED
                    || txStateMeta.txCoordinatorId() == null
                    || coordinator == null) {
                // This means that primary replica for commit partition has changed, since the local node doesn't have the volatile tx
                // state; and there is no final tx state in txStateStorage, or the tx coordinator left the cluster. But we can assume
                // that as the coordinator (or information about it) is missing, there is no need to wait a finish request from
                // tx coordinator, the transaction can't be committed at all.
                return txRecoveryEngine.triggerTxRecovery(txId, localNode.id())
                        .handle((v, ex) ->
                                CompletableFuture.<TransactionMeta>completedFuture(txManager.stateMeta(txId)))
                        .thenCompose(Function.identity());
            } else if (coordinator != null) {
                // If there is coordinator in the cluster we should fallback to coordinator request. It's possible that coordinator
                // was not seen in topology on another node which requested the state from commit partition, but can be seen here.
                HybridTimestamp timestamp = readTimestamp == null ? HybridTimestamp.MIN_VALUE : readTimestamp;

                return txMessageSender.resolveTxStateFromCoordinator(
                                coordinator,
                                txId,
                                timestamp,
                                senderCurrentConsistencyToken,
                                senderGroupId
                        )
                        .handle((response, e) -> {
                            if (e == null && response.txStateMeta() != null) {
                                TransactionMetaMessage transactionMetaMessage = Objects.requireNonNull(response.txStateMeta(),
                                        "Transaction state meta must not be null after check.");

                                return completedFuture(transactionMetaMessage.asTransactionMeta());
                            } else {
                                if (e != null && e.getCause() instanceof RecipientLeftException) {
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
            // Recovery is not needed.
            assert isFinalState(txMeta.txState()) : "Unexpected transaction state: " + txMeta;

            return completedFuture(txMeta);
        }
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
