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
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxRecoveryMessage;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;

/**
 * The class detects transactions that are left without a coordinator but still hold locks. For that orphan transaction, the recovery
 * message is sent to the commit partition replication group.
 */
public class OrphanDetector {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(OrphanDetector.class);

    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    private static final long AWAIT_PRIMARY_REPLICA_TIMEOUT_SEC = 10;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Cluster service. */
    private final ClusterService clusterService;

    private final PlacementDriver placementDriver;

    private final HybridClock clock;

    /** The local map for tx states. */
    private ConcurrentHashMap<UUID, TxStateMeta> txStateMap;

    /**
     * The constructor.
     *
     * @param clusterService Cluster service.
     * @param placementDriver Placement driver.
     * @param clock Clock.
     */
    public OrphanDetector(ClusterService clusterService, PlacementDriver placementDriver, HybridClock clock) {
        this.clusterService = clusterService;
        this.placementDriver = placementDriver;
        this.clock = clock;
    }


    /**
     * Starts the detector.
     *
     * @param txStateMap Transaction state map.
     */
    public void start(ConcurrentHashMap<UUID, TxStateMeta> txStateMap) {
        this.txStateMap = txStateMap;
        // Subscribe to lock conflicts here.
    }

    /**
     * Stops the detector.
     */
    public void stop() {
        busyLock.block();
        // Unsubscribe from lock conflicts here.
    }

    /**
     * Sends {@link TxRecoveryMessage} if the transaction is orphaned.
     *
     * @param txId Transaction id that holds a lock.
     * @return Future to complete.
     */
    private CompletableFuture<Void> handleLockHolder(UUID txId) {
        if (busyLock.enterBusy()) {
            try {
                return handleLockHolderInternal(txId);
            } finally {
                busyLock.leaveBusy();
            }
        }

        return failedFuture(new NodeStoppingException());
    }

    /**
     * Sends {@link TxRecoveryMessage} if the transaction is orphaned.
     *
     * @param txId Transaction id that holds a lock.
     * @return Future to complete.
     */
    private CompletableFuture<Void> handleLockHolderInternal(UUID txId) {
        TxStateMeta txSate = txStateMap.get(txId);

        assert txSate != null : "The transaction is undefined in the local node [txId=" + txId + "].";

        if (clusterService.topologyService().getById(txSate.txCoordinatorId()) == null) {
            LOG.info(
                    "Conflict was found, and the coordinator of the transaction that holds a lock is not available "
                            + "[txId={}, txCrd={}].",
                    txId,
                    txSate.txCoordinatorId()
            );

            return placementDriver.awaitPrimaryReplica(
                    txSate.commitPartitionId(),
                    clock.now(),
                    AWAIT_PRIMARY_REPLICA_TIMEOUT_SEC,
                    SECONDS
            ).thenApply(replicaMeta -> {
                ClusterNode cmpPartNode = clusterService.topologyService().getByConsistentId(replicaMeta.getLeaseholder());

                clusterService.messagingService().weakSend(cmpPartNode, FACTORY.txRecoveryMessage()
                        .txId(txId)
                        .build());

                return null;
            });
        }

        return completedFuture(null);
    }
}
