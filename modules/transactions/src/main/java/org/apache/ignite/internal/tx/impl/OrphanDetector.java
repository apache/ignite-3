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
import java.util.function.Function;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxRecoveryMessage;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyService;

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

    /** Topology service. */
    private final TopologyService topologyService;

    /** Replica service. */
    private final ReplicaService replicaService;

    /** Placement driver. */
    private final PlacementDriver placementDriver;

    /** Lock manager. */
    private final LockManager lockManager;

    /** Hybrid clock. */
    private final HybridClock clock;

    /** Local transaction state storage. */
    private Function<UUID, TxStateMeta> txLocalStateStorage;

    /**
     * The constructor.
     *
     * @param topologyService Topology service.
     * @param replicaService Replica service.
     * @param placementDriver Placement driver.
     * @param lockManager Lock manager.
     * @param clock Clock.
     */
    public OrphanDetector(
            TopologyService topologyService,
            ReplicaService replicaService,
            PlacementDriver placementDriver,
            LockManager lockManager,
            HybridClock clock) {
        this.topologyService = topologyService;
        this.replicaService = replicaService;
        this.placementDriver = placementDriver;
        this.lockManager = lockManager;
        this.clock = clock;
    }

    /**
     * Starts the detector.
     *
     * @param txLocalStateStorage Local transaction state storage.
     */
    public void start(Function<UUID, TxStateMeta> txLocalStateStorage) {
        this.txLocalStateStorage = txLocalStateStorage;
        // TODO: IGNITE-20773 Subscribe to lock conflicts here.
    }

    /**
     * Stops the detector.
     */
    public void stop() {
        busyLock.block();
        // TODO: IGNITE-20773 Unsubscribe from lock conflicts here.
    }

    /**
     * Sends {@link TxRecoveryMessage} if the transaction is orphaned.
     * TODO: IGNITE-20773 Invoke the method when the lock conflict is noted.
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
        TxStateMeta txState = txLocalStateStorage.apply(txId);

        assert txState != null : "The transaction is undefined in the local node [txId=" + txId + "].";

        if (topologyService.getById(txState.txCoordinatorId()) == null) {
            LOG.info(
                    "Conflict was found, and the coordinator of the transaction that holds a lock is not available "
                            + "[txId={}, txCrd={}].",
                    txId,
                    txState.txCoordinatorId()
            );

            return placementDriver.awaitPrimaryReplica(
                    txState.commitPartitionId(),
                    clock.now(),
                    AWAIT_PRIMARY_REPLICA_TIMEOUT_SEC,
                    SECONDS
            ).thenCompose(replicaMeta -> {
                ClusterNode commitPartPrimaryNode = topologyService.getByConsistentId(replicaMeta.getLeaseholder());

                if (commitPartPrimaryNode == null) {
                    LOG.warn(
                            "The primary replica of the commit partition is not available [commitPartGrp={}, tx={}]",
                            txState.commitPartitionId(),
                            txId
                    );

                    return completedFuture(null) ;
                }

                return replicaService.invoke(commitPartPrimaryNode, FACTORY.txRecoveryMessage()
                                .groupId(txState.commitPartitionId())
                                .enlistmentConsistencyToken(replicaMeta.getStartTime().longValue())
                                .txId(txId)
                                .build());
            });
        }

        return completedFuture(null);
    }
}
