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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toZonePartitionIdMessage;
import static org.apache.ignite.internal.tx.TxState.ABANDONED;
import static org.apache.ignite.internal.tx.TxState.FINISHING;
import static org.apache.ignite.internal.tx.TxState.isFinalState;
import static org.apache.ignite.internal.util.CompletableFutures.allOf;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.apache.ignite.lang.ErrorGroups.Transactions.ACQUIRE_LOCK_ERR;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TransactionLogUtils;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.TxStateMetaAbandoned;
import org.apache.ignite.internal.tx.event.LockEvent;
import org.apache.ignite.internal.tx.event.LockEventParameters;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxRecoveryMessage;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * The class detects transactions that are left without a coordinator but still hold locks. For that orphan transaction, the recovery
 * message is sent to the commit partition replication group.
 */
public class OrphanDetector {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(OrphanDetector.class);

    /** Tx messages factory. */
    private static final TxMessagesFactory TX_MESSAGES_FACTORY = new TxMessagesFactory();

    /** Replica messages factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Topology service. */
    private final TopologyService topologyService;

    /** Replica service. */
    private final ReplicaService replicaService;

    /** Placement driver. */
    private final PlacementDriverHelper placementDriverHelper;

    /** Lock manager. */
    private final LockManager lockManager;

    /** Lock conflict events listener. */
    private final EventListener<LockEventParameters> lockConflictListener = this::lockConflictListener;

    /** The executor is used to send a transaction resolution message to the commit partition for an orphan transaction. */
    private final Executor partitionOperationsExecutor;

    private volatile Supplier<Long> checkTxStateIntervalProvider;

    /** Local transaction state storage. */
    private VolatileTxStateMetaStorage txLocalStateStorage;

    /**
     * The constructor.
     *
     * @param topologyService Topology service.
     * @param replicaService Replica service.
     * @param placementDriverHelper Placement driver helper.
     * @param lockManager Lock manager.
     * @param partitionOperationsExecutor Executor is used to start resolution procedure.
     */
    public OrphanDetector(
            TopologyService topologyService,
            ReplicaService replicaService,
            PlacementDriverHelper placementDriverHelper,
            LockManager lockManager,
            Executor partitionOperationsExecutor
    ) {
        this.topologyService = topologyService;
        this.replicaService = replicaService;
        this.placementDriverHelper = placementDriverHelper;
        this.lockManager = lockManager;
        this.partitionOperationsExecutor = partitionOperationsExecutor;
    }

    /**
     * Starts the detector.
     *
     * @param txLocalStateStorage Local transaction state storage.
     * @param checkTxStateIntervalProvider Global provider of configuration check state interval.
     */
    public void start(VolatileTxStateMetaStorage txLocalStateStorage, Supplier<Long> checkTxStateIntervalProvider) {
        this.txLocalStateStorage = txLocalStateStorage;
        this.checkTxStateIntervalProvider = checkTxStateIntervalProvider;

        lockManager.listen(LockEvent.LOCK_CONFLICT, lockConflictListener);
    }

    /**
     * Stops the detector.
     */
    public void stop() {
        busyLock.block();

        lockManager.removeListener(LockEvent.LOCK_CONFLICT, lockConflictListener);
    }

    /**
     * Sends {@link TxRecoveryMessage} if the transaction is orphaned.
     */
    private CompletableFuture<Boolean> lockConflictListener(LockEventParameters params) {
        if (busyLock.enterBusy()) {
            try {
                ArrayList<CompletableFuture<Boolean>> futs = new ArrayList<>(params.lockHolderTxs().size());

                for (UUID txId : params.lockHolderTxs()) {
                    futs.add(checkTxOrphanedInternal(txId));
                }

                return allOf(futs).thenApply(unused -> false);
            } finally {
                busyLock.leaveBusy();
            }
        }

        return falseCompletedFuture();
    }

    /**
     * Sends {@link TxRecoveryMessage} if the transaction is orphaned.
     *
     * @param txId Transaction id that holds a lock.
     * @return Future to complete.
     */
    private CompletableFuture<Boolean> checkTxOrphanedInternal(UUID txId) {
        TxStateMeta txState = txLocalStateStorage.state(txId);

        // Transaction state for full transactions is not stored in the local map, so it can be null.
        if (txState == null || isFinalState(txState.txState()) || isTxCoordinatorAlive(txState)) {
            return falseCompletedFuture();
        }

        if (makeTxAbandoned(txId, txState)) {
            LOG.info(
                    "Conflict was found, and the coordinator of the transaction that holds a lock is not available "
                            + "[txId={}, txCrd={}].",
                    txId,
                    txState.txCoordinatorId()
            );

            // We can path the work to another thread without any condition, because it is a very rare scenario in which the transaction
            // coordinator left topology.
            partitionOperationsExecutor.execute(() -> sendTxRecoveryMessage(txState.commitPartitionId(), txId));
        }

        // TODO: https://issues.apache.org/jira/browse/IGNITE-21153
        return failedFuture(
                new TransactionException(ACQUIRE_LOCK_ERR, "The lock is held by the abandoned transaction [abandonedTxId=" + txId + "]."));
    }

    /**
     * Sends transaction recovery message to commit partition for particular transaction.
     *
     * @param cmpPartGrp Replication group of commit partition.
     * @param txId Transaction id.
     */
    private void sendTxRecoveryMessage(ZonePartitionId cmpPartGrp, UUID txId) {
        placementDriverHelper.awaitPrimaryReplicaWithExceptionHandling(cmpPartGrp)
                .thenCompose(replicaMeta -> {
                    InternalClusterNode commitPartPrimaryNode = topologyService.getByConsistentId(replicaMeta.getLeaseholder());

                    if (commitPartPrimaryNode == null) {
                        LOG.warn(
                                "The primary replica of the commit partition is not available [commitPartGrp={}, tx={}]",
                                cmpPartGrp,
                                txId
                        );

                        return nullCompletedFuture();
                    }

                    return replicaService.invoke(commitPartPrimaryNode, TX_MESSAGES_FACTORY.txRecoveryMessage()
                            .groupId(toZonePartitionIdMessage(REPLICA_MESSAGES_FACTORY, cmpPartGrp))
                            .enlistmentConsistencyToken(replicaMeta.getStartTime().longValue())
                            .txId(txId)
                            .build());
                }).exceptionally(throwable -> {
                    if (throwable != null) {
                        LOG.warn("A recovery message for the transaction was handled with the error [{}].",
                                throwable, TransactionLogUtils.formatTxInfo(txId, txLocalStateStorage));
                    }

                    return null;
                });
    }

    /**
     * Performs a life check for the transaction coordinator.
     *
     * @param txState Transaction state meta.
     * @return True when the transaction coordinator is alive, false otherwise.
     */
    private boolean isTxCoordinatorAlive(TxStateMeta txState) {
        return txState.txCoordinatorId() != null && topologyService.getById(txState.txCoordinatorId()) != null;
    }

    /**
     * Set TX state to {@link org.apache.ignite.internal.tx.TxState#ABANDONED}.
     *
     * @param txId Transaction id.
     * @param txState Transaction meta state.
     * @return True when TX state was set to ABANDONED.
     */
    private boolean makeTxAbandoned(UUID txId, TxStateMeta txState) {
        if (!isRecoveryNeeded(txState)) {
            return false;
        }

        TxStateMetaAbandoned txAbandonedState = txState.abandoned();

        TxStateMeta updatedTxState = txLocalStateStorage.updateMeta(txId, txStateMeta -> {
            if (isRecoveryNeeded(txStateMeta)) {
                return txAbandonedState;
            }

            return txStateMeta;
        });

        return txAbandonedState == updatedTxState;
    }

    /**
     * Checks whether the recovery transaction message should to be sent.
     *
     * @param txState Transaction meta state.
     * @return True when transaction recovery is needed, false otherwise.
     */
    private boolean isRecoveryNeeded(@Nullable TxStateMeta txState) {
        return txState != null
                && !isFinalState(txState.txState())
                && txState.txState() != FINISHING
                && !isTxAbandonedRecently(txState);
    }

    /**
     * Checks whether the transaction state is marked as abandoned recently (less than {@link #checkTxStateIntervalProvider} millis ago).
     *
     * @param txState Transaction state metadata.
     * @return True if the state recently updated to {@link org.apache.ignite.internal.tx.TxState#ABANDONED}.
     */
    private boolean isTxAbandonedRecently(TxStateMeta txState) {
        if (txState.txState() != ABANDONED) {
            return false;
        }

        assert txState instanceof TxStateMetaAbandoned : "The transaction state does not match the metadata [mata=" + txState + "].";

        var txStateAbandoned = (TxStateMetaAbandoned) txState;

        return txStateAbandoned.lastAbandonedMarkerTs() + checkTxStateIntervalProvider.get() >= coarseCurrentTimeMillis();
    }
}
