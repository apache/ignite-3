/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.tx.impl;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.FINISHING;
import static org.apache.ignite.internal.tx.TxState.isFinalState;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ROLLBACK_ERR;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TransactionInternalException;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.TxStateMetaFinishing;
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
                        false,
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
                            res = ((TxStateMetaFinishing) txStateMeta).txFinishFuture();
                        } else {
                            res = failedFuture(new TransactionInternalException(TX_ROLLBACK_ERR, format("Unexpected transaction "
                                    + "state after recovery [txId={}, txMeta={}].", txId, txStateMeta)));
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
                    runCleanupOnNode(commitPartitionId, txId, commitPartitionNode);

                    if (senderGroupId != null && senderId != null) {
                        runCleanupOnNode(senderGroupId, txId, senderId);
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
     * @param nodeId Node id (inconsistent).
     */
    public CompletableFuture<Void> runCleanupOnNode(ZonePartitionId groupId, UUID txId, UUID nodeId) {
        // Get node id of the sender to send back cleanup requests.
        String nodeConsistentId = clusterNodeResolver.getConsistentIdById(nodeId);

        return nodeConsistentId == null ? nullCompletedFuture() : runCleanupOnNode(groupId, txId, nodeConsistentId);
    }

    /**
     * Run cleanup on a node.
     *
     * @param commitPartitionId Commit partition id.
     * @param txId Transaction id.
     * @param nodeName Node consistent id.
     */
    private CompletableFuture<Void> runCleanupOnNode(ZonePartitionId commitPartitionId, UUID txId, String nodeName) {
        return txManager.cleanup(commitPartitionId, nodeName, txId);
    }
}
