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
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.tx.TransactionLogUtils.formatTxInfo;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;
import static org.apache.ignite.internal.tx.TxState.FINISHING;
import static org.apache.ignite.internal.tx.TxState.isFinalState;
import static org.apache.ignite.internal.tx.TxStateMetaFinishing.castToFinishing;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ROLLBACK_ERR;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TransactionInternalException;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.jetbrains.annotations.Nullable;

/**
 * Transaction recovery logic.
 */
public class TxRecoveryEngine {
    private final TxManager txManager;
    private final ClusterNodeResolver clusterNodeResolver;

    /** Constructor. */
    public TxRecoveryEngine(
            TxManager txManager,
            ClusterNodeResolver clusterNodeResolver
    ) {
        this.txManager = txManager;
        this.clusterNodeResolver = clusterNodeResolver;
    }

    /**
     * Abort the abandoned transaction.
     *
     * @param txId Transaction id.
     * @param commitPartitionId Commit partition id.
     * @param commitPartitionNode Commit partition node consistent id.
     * @param senderGroupId Sender group id.
     * @param senderId Sender ephemeral id.
     */
    public CompletableFuture<TransactionMeta> triggerTxRecovery(
            UUID txId,
            ZonePartitionId commitPartitionId,
            String commitPartitionNode,
            @Nullable ZonePartitionId senderGroupId,
            @Nullable UUID senderId
    ) {
        // Should not be empty.
        Map<ZonePartitionId, PendingTxPartitionEnlistment> enlistedGroupsMap = enlistedGroupsMap(
                commitPartitionId,
                commitPartitionNode,
                senderGroupId,
                senderId == null ? null : clusterNodeResolver.getById(senderId)
        );

        // If the transaction state is pending, then the transaction should be rolled back,
        // meaning that the state is changed to aborted and a corresponding cleanup request
        // is sent in a common durable manner to a partition that has initiated recovery.
        // TODO https://issues.apache.org/jira/browse/IGNITE-27386 the reason of rollback needs to be explained.
        return txManager.finish(
                        HybridTimestampTracker.emptyTracker(),
                        // Tx recovery is executed on the commit partition.
                        commitPartitionId,
                        false,
                        new TransactionInternalException(TX_ROLLBACK_ERR, format("Transaction has been aborted"
                                + " due to transaction recovery {}.", formatTxInfo(txId, txManager))),
                        true,
                        false,
                        enlistedGroupsMap,
                        txId
                )
                .handle((v, ex) -> {
                    TransactionMeta txStateMeta = txManager.stateMeta(txId);
                    CompletableFuture<TransactionMeta> res;

                    if (txStateMeta != null) {
                        if (isFinalState(txStateMeta.txState())) {
                            res = completedFuture(txStateMeta);
                        } else if (txStateMeta.txState() == FINISHING) {
                            res = castToFinishing(txId, txStateMeta).txFinishFuture();
                        } else {
                            res = failedFuture(new TransactionInternalException(TX_ROLLBACK_ERR, format("Unexpected transaction "
                                    + "state after recovery [{}, txMeta={}].", formatTxInfo(txId, txManager, false), txStateMeta)));
                        }
                    } else {
                        if (ex == null) {
                            res = completedFuture(TxStateMeta.builder(ABORTED).build());
                        } else {
                            sneakyThrow(ex);

                            res = nullCompletedFuture();
                        }
                    }

                    return res;
                })
                .thenCompose(Function.identity())
                .whenComplete((v, ex) -> {
                    // v contains the actual resolved transaction state (COMMITTED or ABORTED).
                    // We must pass it to cleanup so the correct commit flag is used.
                    // Previously, runCleanupOnNode always used commit=false (hardcoded in the 3-param
                    // TxCleanupRequestSender.cleanup), which caused data corruption when the transaction
                    // was actually COMMITTED: the abort cleanup would race with the legitimate commit
                    // cleanup, and whichever arrived first at each partition would win, producing a mix
                    // of committed and aborted rows within the same transaction.
                    boolean commit = v != null && v.txState() == COMMITTED;
                    @Nullable HybridTimestamp commitTs = v != null ? v.commitTimestamp() : null;

                    runCleanupOnNode(commitPartitionId, txId, commitPartitionNode, commit, commitTs);

                    if (senderGroupId != null && senderId != null) {
                        String senderConsistentId = clusterNodeResolver.getConsistentIdById(senderId);

                        if (senderConsistentId != null) {
                            runCleanupOnNode(senderGroupId, txId, senderConsistentId, commit, commitTs);
                        }
                    }
                });
    }

    private static Map<ZonePartitionId, PendingTxPartitionEnlistment> enlistedGroupsMap(
            ZonePartitionId commitPartitionId,
            String commitPartitionNode,
            @Nullable ZonePartitionId senderGroupId,
            @Nullable InternalClusterNode senderNode
    ) {
        if (senderGroupId == null || senderNode == null || commitPartitionId.equals(senderGroupId)) {
            return Map.of(commitPartitionId, createAbandonedTxRecoveryEnlistment(commitPartitionNode));
        } else {
            return Map.of(
                    commitPartitionId, createAbandonedTxRecoveryEnlistment(commitPartitionNode),
                    senderGroupId, createAbandonedTxRecoveryEnlistment(senderNode.name())
            );
        }
    }

    private static PendingTxPartitionEnlistment createAbandonedTxRecoveryEnlistment(String nodeName) {
        // Enlistment consistency token is not required for the rollback, so it is 0L.
        // Passing an empty set of table IDs as we don't know which tables were enlisted; this is ok as the corresponding write intents
        // can still be resolved later when reads stumble upon them.
        return new PendingTxPartitionEnlistment(nodeName, 0L);
    }

    /**
     * Run cleanup on a node.
     *
     * @param groupId Group id.
     * @param txId Transaction id.
     * @param nodeId Node id (ephemeral).
     * @param commit Whether the transaction was committed.
     * @param commitTimestamp Commit timestamp, if committed.
     */
    public CompletableFuture<Void> runCleanupOnNode(
            ZonePartitionId groupId,
            UUID txId,
            UUID nodeId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        String nodeConsistentId = clusterNodeResolver.getConsistentIdById(nodeId);

        return nodeConsistentId == null ? nullCompletedFuture()
                : runCleanupOnNode(groupId, txId, nodeConsistentId, commit, commitTimestamp);
    }

    /**
     * Run cleanup on a node.
     *
     * @param commitPartitionId Commit partition id.
     * @param txId Transaction id.
     * @param nodeName Node consistent id.
     * @param commit Whether the transaction was committed.
     * @param commitTimestamp Commit timestamp, if committed.
     */
    private CompletableFuture<Void> runCleanupOnNode(
            ZonePartitionId commitPartitionId,
            UUID txId,
            String nodeName,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        return txManager.cleanup(commitPartitionId, nodeName, txId, commit, commitTimestamp);
    }
}
