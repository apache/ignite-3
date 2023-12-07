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
import static org.apache.ignite.internal.tx.TxState.ABANDONED;
import static org.apache.ignite.internal.tx.TxState.isFinalState;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.apache.ignite.lang.ErrorGroups.Transactions.ACQUIRE_LOCK_ERR;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.TxStateMetaAbandoned;
import org.apache.ignite.internal.tx.event.LockEvent;
import org.apache.ignite.internal.tx.event.LockEventParameters;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxRecoveryMessage;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.tx.TransactionException;

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

    /** Lock conflict events listener. */
    private final EventListener<LockEventParameters> lockConflictListener = this::lockConflictListener;

    /** Hybrid clock. */
    private final HybridClock clock;

    /**
     * The time interval in milliseconds in which the orphan resolution sends the recovery message again, in case the transaction is still
     * not finalized.
     */
    private long checkTxStateInterval;

    /** Local transaction state storage. */
    private VolatileTxStateMetaStorage txLocalStateStorage;

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
            HybridClock clock
    ) {
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
     * @param checkTxStateIntervalProvider Global provider of configuration check state interval.
     */
    public void start(VolatileTxStateMetaStorage txLocalStateStorage, ConfigurationValue<Long> checkTxStateIntervalProvider) {
        this.txLocalStateStorage = txLocalStateStorage;
        this.checkTxStateInterval = checkTxStateIntervalProvider.value();

        checkTxStateIntervalProvider.listen(ctx -> {
            this.checkTxStateInterval = ctx.newValue();

            return completedFuture(null);
        });

        lockManager.listen(LockEvent.LOCK_CONFLICT, lockConflictListener);
    }

    /**
     * Stops the detector.
     */
    public void stop() {
        busyLock.block();

        lockManager.removeListener(LockEvent.LOCK_CONFLICT, lockConflictListener);
    }

    private CompletableFuture<Boolean> lockConflictListener(LockEventParameters params, Throwable e) {
        try {
            handleLockHolder(params.lockHolderTx());
        } catch (NodeStoppingException ex) {
            return failedFuture(ex);
        }

        return completedFuture(false);
    }

    /**
     * Sends {@link TxRecoveryMessage} if the transaction is orphaned.
     *
     * @param txId Transaction id that holds a lock.
     */
    private Void handleLockHolder(UUID txId) throws NodeStoppingException {
        if (busyLock.enterBusy()) {
            try {
                handleLockHolderInternal(txId);
            } finally {
                busyLock.leaveBusy();
            }
        }

        throw new NodeStoppingException();
    }

    /**
     * Sends {@link TxRecoveryMessage} if the transaction is orphaned.
     *
     * @param txId Transaction id that holds a lock.
     * @return Future to complete.
     */
    private void handleLockHolderInternal(UUID txId) {
        TxStateMeta txState = txLocalStateStorage.state(txId);

        // Transaction state for full transactions is not stored in the local map, so it can be null.
        if (txState == null || isFinalState(txState.txState()) || isTxCoordinatorAlive(txState)) {
            return;
        }

        if (isRecoveryNeeded(txId, txState)) {
            LOG.info(
                    "Conflict was found, and the coordinator of the transaction that holds a lock is not available "
                            + "[txId={}, txCrd={}].",
                    txId,
                    txState.txCoordinatorId()
            );

            sentTxRecoveryMessage(txState.commitPartitionId(), txId);
        }

        throw new TransactionException(ACQUIRE_LOCK_ERR, "The lock is held by the abandoned transaction [abandonedTx=" + txId + "].");
    }

    /**
     * Sends transaction recovery message to commit partition for particular transaction.
     *
     * @param cmpPartGrp Replication group of commit partition.
     * @param txId Transaction id.
     */
    private void sentTxRecoveryMessage(ReplicationGroupId cmpPartGrp, UUID txId) {
        placementDriver.awaitPrimaryReplica(
                cmpPartGrp,
                clock.now(),
                AWAIT_PRIMARY_REPLICA_TIMEOUT_SEC,
                SECONDS
        ).thenCompose(replicaMeta -> {
            ClusterNode commitPartPrimaryNode = topologyService.getByConsistentId(replicaMeta.getLeaseholder());

            if (commitPartPrimaryNode == null) {
                LOG.warn(
                        "The primary replica of the commit partition is not available [commitPartGrp={}, tx={}]",
                        cmpPartGrp,
                        txId
                );

                return nullCompletedFuture();
            }

            return replicaService.invoke(commitPartPrimaryNode, FACTORY.txRecoveryMessage()
                    .groupId(cmpPartGrp)
                    .enlistmentConsistencyToken(replicaMeta.getStartTime().longValue())
                    .txId(txId)
                    .build());
        }).exceptionally(throwable -> {
            if (throwable != null) {
                LOG.warn("A recovery message for the transaction was handled with the error [tx={}].", throwable, txId);
            }

            return null;
        });
    }

    /**
     * Does a life check for the transaction coordinator.
     *
     * @param txState Transaction state meta.
     * @return True when the transaction coordinator is alive, false otherwise.
     */
    private boolean isTxCoordinatorAlive(TxStateMeta txState) {
        return topologyService.getById(txState.txCoordinatorId()) != null;
    }

    /**
     * Checks whether the recovery transaction message is required to be sent or not.
     *
     * @param txId Transaction id.
     * @param txState Transaction meta state.
     * @return True when transaction recovery is needed, false otherwise.
     */
    private boolean isRecoveryNeeded(UUID txId, TxStateMeta txState) {
        if (txState == null
                || isFinalState(txState.txState())
                || isTxAbandonedNotLong(txState)) {
            return false;
        }

        TxStateMetaAbandoned txAbandonedState = txState.markAbandoned();

        TxStateMeta updatedTxState = txLocalStateStorage.updateMeta(txId, txStateMeta -> {
            if (txStateMeta != null
                    && !isFinalState(txStateMeta.txState())
                    && (txStateMeta.txState() != ABANDONED || isTxAbandonedNotLong(txStateMeta))) {
                return txAbandonedState;
            }

            return txStateMeta;
        });

        return txAbandonedState == updatedTxState;
    }

    /**
     * Checks whether the transaction state is recently marked as abandoned or not.
     *
     * @param txState Transaction state metadata.
     * @return True if the state recently updated to {@link org.apache.ignite.internal.tx.TxState#ABANDONED}.
     */
    private boolean isTxAbandonedNotLong(TxStateMeta txState) {
        if (txState.txState() != ABANDONED) {
            return false;
        }

        assert txState instanceof TxStateMetaAbandoned : "The transaction state does not match the metadata [mata=" + txState + "].";

        var txStateAbandoned = (TxStateMetaAbandoned) txState;

        return txStateAbandoned.lastAbandonedMarkerTs() + checkTxStateInterval >= coarseCurrentTimeMillis();
    }
}
