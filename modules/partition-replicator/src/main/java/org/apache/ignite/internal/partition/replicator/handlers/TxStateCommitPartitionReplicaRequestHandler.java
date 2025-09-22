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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.partition.replicator.TxRecoveryEngine;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.TxStateMetaFinishing;
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

    /** Constructor. */
    public TxStateCommitPartitionReplicaRequestHandler(
            TxStatePartitionStorage txStatePartitionStorage,
            TxManager txManager,
            ClusterNodeResolver clusterNodeResolver,
            InternalClusterNode localNode,
            TxRecoveryEngine txRecoveryEngine
    ) {
        this.txStatePartitionStorage = txStatePartitionStorage;
        this.txManager = txManager;
        this.clusterNodeResolver = clusterNodeResolver;
        this.localNode = localNode;
        this.txRecoveryEngine = txRecoveryEngine;
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
            return triggerTxRecoveryOnTxStateResolutionIfNeeded(txId, txMeta);
        } else {
            return completedFuture(txMeta);
        }
    }

    /**
     * Checks whether tx recovery is needed with given tx meta and triggers it of needed.
     *
     * @param txId Transaction id.
     * @param txStateMeta Transaction meta.
     * @return Tx recovery future, or completed future if the recovery isn't needed, or failed future if the recovery is not possible.
     */
    private CompletableFuture<TransactionMeta> triggerTxRecoveryOnTxStateResolutionIfNeeded(
            UUID txId,
            @Nullable TxStateMeta txStateMeta
    ) {
        // The state is either null or PENDING or ABANDONED, other states have been filtered out previously.
        assert txStateMeta == null || txStateMeta.txState() == PENDING || txStateMeta.txState() == ABANDONED
                : "Unexpected transaction state: " + txStateMeta;

        TxMeta txMeta = txStatePartitionStorage.get(txId);

        if (txMeta == null) {
            // This means the transaction is pending and we should trigger the recovery if there is no tx coordinator in topology.
            if (txStateMeta == null
                    || txStateMeta.txState() == ABANDONED
                    || txStateMeta.txCoordinatorId() == null
                    || clusterNodeResolver.getById(txStateMeta.txCoordinatorId()) == null) {
                // This means that primary replica for commit partition has changed, since the local node doesn't have the volatile tx
                // state; and there is no final tx state in txStateStorage, or the tx coordinator left the cluster. But we can assume
                // that as the coordinator (or information about it) is missing, there is no need to wait a finish request from
                // tx coordinator, the transaction can't be committed at all.
                return txRecoveryEngine.triggerTxRecovery(txId, localNode.id())
                        .handle((v, ex) ->
                                CompletableFuture.<TransactionMeta>completedFuture(txManager.stateMeta(txId)))
                        .thenCompose(v -> v);
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
}
