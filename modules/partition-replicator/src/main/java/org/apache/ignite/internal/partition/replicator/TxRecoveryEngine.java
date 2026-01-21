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

package org.apache.ignite.internal.partition.replicator;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TxManager;

/**
 * Transaction recovery logic.
 */
public class TxRecoveryEngine {
    private final TxManager txManager;
    private final ClusterNodeResolver clusterNodeResolver;

    private final ZonePartitionId replicationGroupId;
    private final Function<InternalClusterNode, PendingTxPartitionEnlistment> abandonedTxRecoveryEnlistmentFactory;

    /** Constructor. */
    public TxRecoveryEngine(
            TxManager txManager,
            ClusterNodeResolver clusterNodeResolver,
            ZonePartitionId replicationGroupId,
            Function<InternalClusterNode, PendingTxPartitionEnlistment> abandonedTxRecoveryEnlistmentFactory
    ) {
        this.txManager = txManager;
        this.clusterNodeResolver = clusterNodeResolver;
        this.replicationGroupId = replicationGroupId;
        this.abandonedTxRecoveryEnlistmentFactory = abandonedTxRecoveryEnlistmentFactory;
    }

    /**
     * Abort the abandoned transaction.
     *
     * @param txId Transaction id.
     * @param senderId Sender inconsistent id.
     */
    public CompletableFuture<Void> triggerTxRecovery(UUID txId, UUID senderId) {
        // If the transaction state is pending, then the transaction should be rolled back,
        // meaning that the state is changed to aborted and a corresponding cleanup request
        // is sent in a common durable manner to a partition that has initiated recovery.
        return txManager.finish(
                        HybridTimestampTracker.emptyTracker(),
                        // Tx recovery is executed on the commit partition.
                        replicationGroupId,
                        false,
                        false,
                        true,
                        false,
                        Map.of(replicationGroupId, abandonedTxRecoveryEnlistmentFactory.apply(clusterNodeResolver.getById(senderId))),
                        txId
                )
                .whenComplete((v, ex) -> runCleanupOnNode(replicationGroupId, txId, senderId));
    }

    /**
     * Run cleanup on a node.
     *
     * @param commitPartitionId Commit partition id.
     * @param txId Transaction id.
     * @param nodeId Node id (inconsistent).
     */
    public CompletableFuture<Void> runCleanupOnNode(ZonePartitionId commitPartitionId, UUID txId, UUID nodeId) {
        // Get node id of the sender to send back cleanup requests.
        String nodeConsistentId = clusterNodeResolver.getConsistentIdById(nodeId);

        return nodeConsistentId == null ? nullCompletedFuture() : txManager.cleanup(commitPartitionId, nodeConsistentId, txId);
    }
}
