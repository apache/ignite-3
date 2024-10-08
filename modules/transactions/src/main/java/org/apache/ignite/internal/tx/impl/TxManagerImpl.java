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

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.function.Function.identity;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.tx.TransactionIds.beginTimestamp;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;
import static org.apache.ignite.internal.tx.TxState.FINISHING;
import static org.apache.ignite.internal.tx.TxState.isFinalState;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_READ_ONLY_TOO_OLD_ERR;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.replicator.message.ErrorReplicaResponse;
import org.apache.ignite.internal.replicator.message.ReplicaMessageGroup;
import org.apache.ignite.internal.replicator.message.ReplicaResponse;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LocalRwTxCounter;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.MismatchingTransactionOutcomeInternalException;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TransactionResult;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxPriority;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.TxStateMetaFinishing;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.TransactionInflights.ReadWriteTxContext;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicatedInfo;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * A transaction manager implementation.
 *
 * <p>Uses 2PC for atomic commitment and 2PL for concurrency control.
 */
public class TxManagerImpl implements TxManager, NetworkMessageHandler {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TxManagerImpl.class);

    /** Transaction configuration. */
    private final TransactionConfiguration txConfig;

    /** Lock manager. */
    private final LockManager lockManager;

    /** Executor that runs async write intent switch actions. */
    private final ExecutorService writeIntentSwitchPool;

    private final ClockService clockService;

    /** Generates transaction IDs. */
    private final TransactionIdGenerator transactionIdGenerator;

    /** The local state storage. */
    private final VolatileTxStateMetaStorage txStateVolatileStorage = new VolatileTxStateMetaStorage();

    /** Low watermark. */
    private final LowWatermark lowWatermark;

    private final PlacementDriver placementDriver;

    private final PlacementDriverHelper placementDriverHelper;

    private final LongSupplier idleSafeTimePropagationPeriodMsSupplier;

    /** Prevents double stopping of the tracker. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * Total number of started transaction.
     * TODO: IGNITE-21440 Implement transaction metrics.
     */
    private final LongAdder startedTxs = new LongAdder();

    /**
     * Total number of finished transaction.
     * TODO: IGNITE-21440 Implement transaction metrics.
     */
    private final LongAdder finishedTxs = new LongAdder();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Detector of transactions that lost the coordinator. */
    private final OrphanDetector orphanDetector;

    /** Topology service. */
    private final TopologyService topologyService;

    /** Messaging service. */
    private final MessagingService messagingService;

    /** Local node network identity. This id is available only after the network has started. */
    private volatile UUID localNodeId;

    /** Server cleanup processor. */
    private final TxCleanupRequestHandler txCleanupRequestHandler;

    /** Cleanup request sender. */
    private final TxCleanupRequestSender txCleanupRequestSender;

    /** Transaction message sender. */
    private final TxMessageSender txMessageSender;

    private final EventListener<PrimaryReplicaEventParameters> primaryReplicaExpiredListener;

    private final EventListener<PrimaryReplicaEventParameters> primaryReplicaElectedListener;

    /** Counter of read-write transactions that were created and completed locally on the node. */
    private final LocalRwTxCounter localRwTxCounter;

    private final Executor partitionOperationsExecutor;

    private final TransactionInflights transactionInflights;

    private final ReplicaService replicaService;

    private volatile PersistentTxStateVacuumizer persistentTxStateVacuumizer;

    /**
     * Test-only constructor.
     *
     * @param txConfig Transaction configuration.
     * @param clusterService Cluster service.
     * @param replicaService Replica service.
     * @param lockManager Lock manager.
     * @param clockService Clock service.
     * @param transactionIdGenerator Used to generate transaction IDs.
     * @param placementDriver Placement driver.
     * @param idleSafeTimePropagationPeriodMsSupplier Used to get idle safe time propagation period in ms.
     * @param localRwTxCounter Counter of read-write transactions that were created and completed locally on the node.
     * @param resourcesRegistry Resources registry.
     * @param transactionInflights Transaction inflights.
     * @param lowWatermark Low watermark.
     */
    @TestOnly
    public TxManagerImpl(
            TransactionConfiguration txConfig,
            ClusterService clusterService,
            ReplicaService replicaService,
            LockManager lockManager,
            ClockService clockService,
            TransactionIdGenerator transactionIdGenerator,
            PlacementDriver placementDriver,
            LongSupplier idleSafeTimePropagationPeriodMsSupplier,
            LocalRwTxCounter localRwTxCounter,
            RemotelyTriggeredResourceRegistry resourcesRegistry,
            TransactionInflights transactionInflights,
            LowWatermark lowWatermark
    ) {
        this(
                clusterService.nodeName(),
                txConfig,
                clusterService.messagingService(),
                clusterService.topologyService(),
                replicaService,
                lockManager,
                clockService,
                transactionIdGenerator,
                placementDriver,
                idleSafeTimePropagationPeriodMsSupplier,
                localRwTxCounter,
                ForkJoinPool.commonPool(),
                resourcesRegistry,
                transactionInflights,
                lowWatermark
        );
    }

    /**
     * The constructor.
     *
     * @param txConfig Transaction configuration.
     * @param messagingService Messaging service.
     * @param topologyService Topology service.
     * @param replicaService Replica service.
     * @param lockManager Lock manager.
     * @param clockService Clock service.
     * @param transactionIdGenerator Used to generate transaction IDs.
     * @param placementDriver Placement driver.
     * @param idleSafeTimePropagationPeriodMsSupplier Used to get idle safe time propagation period in ms.
     * @param localRwTxCounter Counter of read-write transactions that were created and completed locally on the node.
     * @param partitionOperationsExecutor Executor on which partition operations will be executed, if needed.
     * @param resourcesRegistry Resources registry.
     * @param transactionInflights Transaction inflights.
     * @param lowWatermark Low watermark.
     */
    public TxManagerImpl(
            String nodeName,
            TransactionConfiguration txConfig,
            MessagingService messagingService,
            TopologyService topologyService,
            ReplicaService replicaService,
            LockManager lockManager,
            ClockService clockService,
            TransactionIdGenerator transactionIdGenerator,
            PlacementDriver placementDriver,
            LongSupplier idleSafeTimePropagationPeriodMsSupplier,
            LocalRwTxCounter localRwTxCounter,
            Executor partitionOperationsExecutor,
            RemotelyTriggeredResourceRegistry resourcesRegistry,
            TransactionInflights transactionInflights,
            LowWatermark lowWatermark
    ) {
        this.txConfig = txConfig;
        this.lockManager = lockManager;
        this.clockService = clockService;
        this.transactionIdGenerator = transactionIdGenerator;
        this.placementDriver = placementDriver;
        this.idleSafeTimePropagationPeriodMsSupplier = idleSafeTimePropagationPeriodMsSupplier;
        this.topologyService = topologyService;
        this.messagingService = messagingService;
        this.primaryReplicaExpiredListener = this::primaryReplicaExpiredListener;
        this.primaryReplicaElectedListener = this::primaryReplicaElectedListener;
        this.localRwTxCounter = localRwTxCounter;
        this.partitionOperationsExecutor = partitionOperationsExecutor;
        this.transactionInflights = transactionInflights;
        this.lowWatermark = lowWatermark;
        this.replicaService = replicaService;

        placementDriverHelper = new PlacementDriverHelper(placementDriver, clockService);

        int cpus = Runtime.getRuntime().availableProcessors();

        writeIntentSwitchPool = new ThreadPoolExecutor(
                cpus,
                cpus,
                100,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                IgniteThreadFactory.create(nodeName, "tx-async-write-intent", LOG, STORAGE_READ, STORAGE_WRITE)
        );

        orphanDetector = new OrphanDetector(
                topologyService,
                replicaService,
                placementDriverHelper,
                lockManager,
                partitionOperationsExecutor
        );

        txMessageSender = new TxMessageSender(messagingService, replicaService, clockService, txConfig);

        var writeIntentSwitchProcessor = new WriteIntentSwitchProcessor(placementDriverHelper, txMessageSender, topologyService);

        txCleanupRequestHandler = new TxCleanupRequestHandler(
                messagingService,
                lockManager,
                clockService,
                writeIntentSwitchProcessor,
                resourcesRegistry
        );

        txCleanupRequestSender =
                new TxCleanupRequestSender(txMessageSender, placementDriverHelper, txStateVolatileStorage);
    }

    private CompletableFuture<Boolean> primaryReplicaEventListener(
            PrimaryReplicaEventParameters eventParameters,
            Consumer<TablePartitionId> action
    ) {
        return inBusyLock(busyLock, () -> {
            if (!(eventParameters.groupId() instanceof TablePartitionId)) {
                return falseCompletedFuture();
            }

            TablePartitionId groupId = (TablePartitionId) eventParameters.groupId();

            action.accept(groupId);

            return falseCompletedFuture();
        });
    }

    private CompletableFuture<Boolean> primaryReplicaElectedListener(PrimaryReplicaEventParameters eventParameters) {
        return primaryReplicaEventListener(eventParameters, groupId -> {
            if (localNodeId.equals(eventParameters.leaseholderId())) {
                String localNodeName = topologyService.localMember().name();

                txMessageSender.sendRecoveryCleanup(localNodeName, groupId);
            }
        });
    }

    private CompletableFuture<Boolean> primaryReplicaExpiredListener(PrimaryReplicaEventParameters eventParameters) {
        return primaryReplicaEventListener(eventParameters, transactionInflights::cancelWaitingInflights);
    }

    @Override
    public InternalTransaction begin(HybridTimestampTracker timestampTracker) {
        return begin(timestampTracker, false, TxPriority.NORMAL);
    }

    @Override
    public InternalTransaction begin(HybridTimestampTracker timestampTracker, boolean readOnly) {
        return begin(timestampTracker, readOnly, TxPriority.NORMAL);
    }

    @Override
    public InternalTransaction begin(HybridTimestampTracker timestampTracker, boolean readOnly, TxPriority priority) {
        HybridTimestamp beginTimestamp = readOnly ? clockService.now() : createBeginTimestampWithIncrementRwTxCounter();
        UUID txId = transactionIdGenerator.transactionIdFor(beginTimestamp, priority);

        startedTxs.add(1);

        if (!readOnly) {
            txStateVolatileStorage.initialize(txId, localNodeId);

            return new ReadWriteTransactionImpl(this, timestampTracker, txId, localNodeId);
        }

        HybridTimestamp observableTimestamp = timestampTracker.get();

        HybridTimestamp readTimestamp = observableTimestamp != null
                ? HybridTimestamp.max(observableTimestamp, currentReadTimestamp(beginTimestamp))
                : currentReadTimestamp(beginTimestamp);

        boolean lockAcquired = lowWatermark.tryLock(txId, readTimestamp);
        if (!lockAcquired) {
            throw new IgniteInternalException(
                    TX_READ_ONLY_TOO_OLD_ERR,
                    "Timestamp of read-only transaction must be greater than the low watermark: [txTimestamp={}, lowWatermark={}]",
                    readTimestamp,
                    lowWatermark.getLowWatermark());
        }

        try {
            CompletableFuture<Void> txFuture = new CompletableFuture<>();
            txFuture.whenComplete((unused, throwable) -> lowWatermark.unlock(txId));

            return new ReadOnlyTransactionImpl(this, timestampTracker, txId, localNodeId, readTimestamp, txFuture);
        } catch (Throwable t) {
            lowWatermark.unlock(txId);
            throw t;
        }
    }

    /**
     * Current read timestamp, for calculation of read timestamp of read-only transactions.
     *
     * @param beginTx Begin transaction timestamp.
     * @return Current read timestamp.
     */
    private HybridTimestamp currentReadTimestamp(HybridTimestamp beginTx) {
        return beginTx.subtractPhysicalTime(
                idleSafeTimePropagationPeriodMsSupplier.getAsLong() + clockService.maxClockSkewMillis()
        );
    }

    @Override
    public @Nullable TxStateMeta stateMeta(UUID txId) {
        return inBusyLock(busyLock, () -> txStateVolatileStorage.state(txId));
    }

    @Override
    public @Nullable <T extends TxStateMeta> T updateTxMeta(UUID txId, Function<@Nullable TxStateMeta, TxStateMeta> updater) {
        return txStateVolatileStorage.updateMeta(txId, updater);
    }

    @Override
    public void finishFull(HybridTimestampTracker timestampTracker, UUID txId, boolean commit) {
        TxState finalState;

        finishedTxs.add(1);

        if (commit) {
            timestampTracker.update(clockService.now());

            finalState = COMMITTED;
        } else {
            finalState = ABORTED;
        }

        updateTxMeta(txId, old ->
                new TxStateMeta(
                        finalState,
                        old == null ? null : old.txCoordinatorId(),
                        old == null ? null : old.commitPartitionId(),
                        old == null ? null : old.commitTimestamp()
                ));

        decrementRwTxCount(txId);
    }

    private @Nullable HybridTimestamp commitTimestamp(boolean commit) {
        return commit ? clockService.now() : null;
    }

    @Override
    public CompletableFuture<Void> finish(
            HybridTimestampTracker observableTimestampTracker,
            TablePartitionId commitPartition,
            boolean commitIntent,
            Map<TablePartitionId, IgniteBiTuple<ClusterNode, Long>> enlistedGroups,
            UUID txId
    ) {
        LOG.debug("Finish [commit={}, txId={}, groups={}].", commitIntent, txId, enlistedGroups);

        finishedTxs.add(1);

        assert enlistedGroups != null;

        if (enlistedGroups.isEmpty()) {
            // If there are no enlisted groups, just update local state - we already marked the tx as finished.
            updateTxMeta(txId, old -> new TxStateMeta(
                    commitIntent ? COMMITTED : ABORTED, localNodeId, commitPartition, commitTimestamp(commitIntent)
            ));

            decrementRwTxCount(txId);

            return nullCompletedFuture();
        }

        // Here we put finishing state meta into the local map, so that all concurrent operations trying to read tx state
        // with using read timestamp could see that this transaction is finishing (e.g. see TransactionStateResolver#processTxStateRequest).
        // None of them are now able to update node's clock with read timestamp and we can create the commit timestamp that is greater
        // than all the read timestamps processed before.
        // Every concurrent operation will now use a finish future from the finishing state meta and get only final transaction
        // state after the transaction is finished.

        // First we check the current tx state to guarantee txFinish idempotence.
        TxStateMeta txMeta = stateMeta(txId);

        TxStateMetaFinishing finishingStateMeta =
                txMeta == null
                        ? new TxStateMetaFinishing(null, commitPartition)
                        : txMeta.finishing();

        TxStateMeta stateMeta = updateTxMeta(txId, oldMeta -> finishingStateMeta);

        // Means we failed to CAS the state, someone else did it.
        if (finishingStateMeta != stateMeta) {
            // If the state is FINISHING then someone else hase in in the middle of finishing this tx.
            if (stateMeta.txState() == FINISHING) {
                return ((TxStateMetaFinishing) stateMeta).txFinishFuture()
                        .thenCompose(meta -> checkTxOutcome(commitIntent, txId, meta));
            } else {
                // The TX has already been finished. Check whether it finished with the same outcome.
                return checkTxOutcome(commitIntent, txId, stateMeta);
            }
        }

        ReadWriteTxContext txContext = transactionInflights.lockTxForNewUpdates(txId, enlistedGroups);

        // Wait for commit acks first, then proceed with the finish request.
        return txContext.performFinish(commitIntent, commit ->
                prepareFinish(
                        observableTimestampTracker,
                        commitPartition,
                        commit,
                        enlistedGroups,
                        txId,
                        finishingStateMeta.txFinishFuture()
                )
        ).thenAccept(unused -> {
            if (localNodeId.equals(finishingStateMeta.txCoordinatorId())) {
                decrementRwTxCount(txId);
            }
        }).whenComplete((unused, throwable) -> transactionInflights.removeTxContext(txId));
    }

    private static CompletableFuture<Void> checkTxOutcome(boolean commit, UUID txId, TransactionMeta stateMeta) {
        if ((stateMeta.txState() == COMMITTED) == commit) {
            return nullCompletedFuture();
        }

        return failedFuture(new MismatchingTransactionOutcomeInternalException(
                "Failed to change the outcome of a finished transaction [txId=" + txId + ", txState=" + stateMeta.txState() + "].",
                new TransactionResult(stateMeta.txState(), stateMeta.commitTimestamp()))
        );
    }

    private CompletableFuture<Void> prepareFinish(
            HybridTimestampTracker observableTimestampTracker,
            TablePartitionId commitPartition,
            boolean commit,
            Map<TablePartitionId, IgniteBiTuple<ClusterNode, Long>> enlistedGroups,
            UUID txId,
            CompletableFuture<TransactionMeta> txFinishFuture
    ) {
        HybridTimestamp commitTimestamp = commitTimestamp(commit);
        // In case of commit it's required to check whether current primaries are still the same that were enlisted and whether
        // given primaries are not expired or, in other words, whether commitTimestamp is less or equal to the enlisted primaries
        // expiration timestamps.
        CompletableFuture<Void> verificationFuture =
                commit ? verifyCommitTimestamp(enlistedGroups, commitTimestamp) : nullCompletedFuture();

        return verificationFuture.handle(
                        (unused, throwable) -> {
                            boolean verifiedCommit = throwable == null && commit;

                            Map<TablePartitionId, String> replicationGroupIds = enlistedGroups.entrySet().stream()
                                    .collect(Collectors.toMap(
                                            Entry::getKey,
                                            entry -> entry.getValue().get1().name()
                                    ));

                            return durableFinish(
                                    observableTimestampTracker,
                                    commitPartition,
                                    verifiedCommit,
                                    replicationGroupIds,
                                    txId,
                                    commitTimestamp,
                                    txFinishFuture);
                        })
                .thenCompose(identity())
                // Verification future is added in order to share the proper verification exception with the client.
                .thenCompose(r -> verificationFuture);
    }

    /**
     * Durable finish request.
     */
    private CompletableFuture<Void> durableFinish(
            HybridTimestampTracker observableTimestampTracker,
            TablePartitionId commitPartition,
            boolean commit,
            Map<TablePartitionId, String> replicationGroupIds,
            UUID txId,
            HybridTimestamp commitTimestamp,
            CompletableFuture<TransactionMeta> txFinishFuture
    ) {
        return inBusyLockAsync(busyLock, () -> placementDriverHelper.awaitPrimaryReplicaWithExceptionHandling(commitPartition)
                .thenCompose(meta ->
                        makeFinishRequest(
                                observableTimestampTracker,
                                commitPartition,
                                meta.getLeaseholder(),
                                meta.getStartTime().longValue(),
                                commit,
                                replicationGroupIds,
                                txId,
                                commitTimestamp,
                                txFinishFuture
                        ))
                .handle((res, ex) -> {
                    if (ex != null) {
                        Throwable cause = ExceptionUtils.unwrapCause(ex);

                        if (cause instanceof MismatchingTransactionOutcomeInternalException) {
                            MismatchingTransactionOutcomeInternalException transactionException =
                                    (MismatchingTransactionOutcomeInternalException) cause;

                            TransactionResult result = transactionException.transactionResult();

                            TxStateMeta updatedMeta = updateTxMeta(txId, old ->
                                    new TxStateMeta(
                                            result.transactionState(),
                                            old == null ? null : old.txCoordinatorId(),
                                            commitPartition,
                                            result.commitTimestamp(),
                                            old == null ? null : old.initialVacuumObservationTimestamp(),
                                            old == null ? null : old.cleanupCompletionTimestamp()
                                    )
                            );

                            txFinishFuture.complete(updatedMeta);

                            return CompletableFuture.<Void>failedFuture(cause);
                        }

                        if (TransactionFailureHandler.isRecoverable(cause)) {
                            LOG.warn("Failed to finish Tx. The operation will be retried [txId={}].", ex, txId);

                            return supplyAsync(() -> durableFinish(
                                    observableTimestampTracker,
                                    commitPartition,
                                    commit,
                                    replicationGroupIds,
                                    txId,
                                    commitTimestamp,
                                    txFinishFuture
                            ), partitionOperationsExecutor).thenCompose(identity());
                        } else {
                            LOG.warn("Failed to finish Tx [txId={}].", ex, txId);

                            return CompletableFuture.<Void>failedFuture(cause);
                        }
                    }

                    return CompletableFutures.<Void>nullCompletedFuture();
                })
                .thenCompose(identity()));
    }

    private CompletableFuture<Void> makeFinishRequest(
            HybridTimestampTracker observableTimestampTracker,
            TablePartitionId commitPartition,
            String primaryConsistentId,
            Long enlistmentConsistencyToken,
            boolean commit,
            Map<TablePartitionId, String> replicationGroupIds,
            UUID txId,
            HybridTimestamp commitTimestamp,
            CompletableFuture<TransactionMeta> txFinishFuture
    ) {
        LOG.debug("Finish [partition={}, node={}, enlistmentConsistencyToken={} commit={}, txId={}, groups={}",
                commitPartition, primaryConsistentId, enlistmentConsistencyToken, commit, txId, replicationGroupIds);

        return txMessageSender.finish(
                        primaryConsistentId,
                        commitPartition,
                        replicationGroupIds,
                        txId,
                        enlistmentConsistencyToken,
                        commit,
                        commitTimestamp
                )
                .thenAccept(txResult -> {
                    validateTxFinishedAsExpected(commit, txId, txResult);

                    TxStateMeta updatedMeta = updateTxMeta(txId, old ->
                            new TxStateMeta(
                                    txResult.transactionState(),
                                    localNodeId,
                                    old == null ? null : old.commitPartitionId(),
                                    txResult.commitTimestamp(),
                                    old == null ? null : old.initialVacuumObservationTimestamp(),
                                    old == null ? null : old.cleanupCompletionTimestamp()
                            ));

                    assert isFinalState(updatedMeta.txState()) :
                            "Unexpected transaction state [id=" + txId + ", state=" + updatedMeta.txState() + "].";

                    txFinishFuture.complete(updatedMeta);

                    if (commit) {
                        observableTimestampTracker.update(commitTimestamp);
                    }
                });
    }

    private static void validateTxFinishedAsExpected(boolean commit, UUID txId, TransactionResult txResult) {
        if (commit != (txResult.transactionState() == COMMITTED)) {
            LOG.error("Failed to finish a transaction that is already finished [txId={}, expectedState={}, actualState={}].",
                    txId,
                    commit ? COMMITTED : ABORTED,
                    txResult.transactionState()
            );

            throw new MismatchingTransactionOutcomeInternalException(
                    "Failed to change the outcome of a finished transaction [txId=" + txId + ", txState=" + txResult.transactionState()
                            + "].",
                    txResult
            );
        }
    }

    @Override
    public int finished() {
        return finishedTxs.intValue();
    }

    @Override
    public int pending() {
        return startedTxs.intValue() - finishedTxs.intValue();
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return inBusyLockAsync(busyLock, () -> {
            localNodeId = topologyService.localMember().id();

            messagingService.addMessageHandler(ReplicaMessageGroup.class, this);

            persistentTxStateVacuumizer = new PersistentTxStateVacuumizer(replicaService, topologyService.localMember(), clockService,
                    placementDriver);

            txStateVolatileStorage.start();

            orphanDetector.start(txStateVolatileStorage, txConfig.abandonedCheckTs());

            txCleanupRequestSender.start();

            txCleanupRequestHandler.start();

            placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, primaryReplicaExpiredListener);

            placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, primaryReplicaElectedListener);

            return nullCompletedFuture();
        });
    }

    @Override
    public void beforeNodeStop() {
        orphanDetector.stop();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        busyLock.block();

        txStateVolatileStorage.stop();

        txCleanupRequestHandler.stop();

        placementDriver.removeListener(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, primaryReplicaExpiredListener);

        placementDriver.removeListener(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, primaryReplicaElectedListener);

        shutdownAndAwaitTermination(writeIntentSwitchPool, 10, TimeUnit.SECONDS);

        return nullCompletedFuture();
    }

    @Override
    public LockManager lockManager() {
        return lockManager;
    }

    @Override
    public CompletableFuture<Void> cleanup(
            TablePartitionId commitPartitionId,
            Map<TablePartitionId, String> enlistedPartitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        return txCleanupRequestSender.cleanup(commitPartitionId, enlistedPartitions, commit, commitTimestamp, txId);
    }

    @Override
    public CompletableFuture<Void> cleanup(
            TablePartitionId commitPartitionId,
            Collection<TablePartitionId> enlistedPartitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        return txCleanupRequestSender.cleanup(commitPartitionId, enlistedPartitions, commit, commitTimestamp, txId);
    }

    @Override
    public CompletableFuture<Void> cleanup(TablePartitionId commitPartitionId, String node, UUID txId) {
        return txCleanupRequestSender.cleanup(commitPartitionId, node, txId);
    }

    @Override
    public CompletableFuture<Void> vacuum() {
        if (persistentTxStateVacuumizer == null) {
            return nullCompletedFuture(); // Not started yet.
        }

        long vacuumObservationTimestamp = System.currentTimeMillis();

        return txStateVolatileStorage.vacuum(vacuumObservationTimestamp, txConfig.txnResourceTtl().value(),
                persistentTxStateVacuumizer::vacuumPersistentTxStates);
    }

    @Override
    public CompletableFuture<Void> executeWriteIntentSwitchAsync(Runnable runnable) {
        return runAsync(runnable, writeIntentSwitchPool);
    }

    void completeReadOnlyTransactionFuture(TxIdAndTimestamp txIdAndTimestamp) {
        finishedTxs.add(1);

        UUID txId = txIdAndTimestamp.getTxId();

        transactionInflights.markReadOnlyTxFinished(txId);
    }

    @Override
    public void onReceived(NetworkMessage message, ClusterNode sender, @Nullable Long correlationId) {
        if (!(message instanceof ReplicaResponse) || correlationId != null) {
            return;
        }

        // Ignore error responses here. A transaction will be rolled back in other place.
        if (message instanceof ErrorReplicaResponse) {
            return;
        }

        // Process directly sent response.
        ReplicaResponse response = (ReplicaResponse) message;

        Object result = response.result();

        if (result instanceof UUID) {
            transactionInflights.removeInflight((UUID) result);
        }

        if (result instanceof WriteIntentSwitchReplicatedInfo) {
            txCleanupRequestHandler.writeIntentSwitchReplicated((WriteIntentSwitchReplicatedInfo) result);
        }
    }

    /**
     * Check whether previously enlisted primary replicas aren't expired and that commit timestamp is less or equal than primary replicas
     * expiration timestamp. Given method will either complete result future with void or {@link PrimaryReplicaExpiredException}
     *
     * @param enlistedGroups enlisted primary replicas map from groupId to enlistment consistency token.
     * @param commitTimestamp Commit timestamp.
     * @return Verification future.
     */
    private CompletableFuture<Void> verifyCommitTimestamp(
            Map<TablePartitionId, IgniteBiTuple<ClusterNode, Long>> enlistedGroups,
            HybridTimestamp commitTimestamp
    ) {
        var verificationFutures = new CompletableFuture[enlistedGroups.size()];
        int cnt = -1;

        for (Map.Entry<TablePartitionId, IgniteBiTuple<ClusterNode, Long>> enlistedGroup : enlistedGroups.entrySet()) {
            TablePartitionId groupId = enlistedGroup.getKey();
            Long expectedEnlistmentConsistencyToken = enlistedGroup.getValue().get2();

            verificationFutures[++cnt] = placementDriver.getPrimaryReplica(groupId, commitTimestamp)
                    .thenAccept(currentPrimaryReplica -> {
                        if (currentPrimaryReplica == null
                                || !expectedEnlistmentConsistencyToken.equals(currentPrimaryReplica.getStartTime().longValue())
                        ) {
                            throw new PrimaryReplicaExpiredException(
                                    groupId,
                                    expectedEnlistmentConsistencyToken,
                                    commitTimestamp,
                                    currentPrimaryReplica
                            );
                        } else {
                            assert commitTimestamp.compareTo(currentPrimaryReplica.getExpirationTime()) <= 0 :
                                    format(
                                            "Commit timestamp is greater than primary replica expiration timestamp:"
                                                    + " [groupId = {}, commit timestamp = {}, primary replica expiration timestamp = {}]",
                                            groupId, commitTimestamp, currentPrimaryReplica.getExpirationTime());
                        }
                    });
        }

        return allOf(verificationFutures);
    }

    static class TransactionFailureHandler {
        private static final Set<Class<? extends Throwable>> RECOVERABLE = Set.of(
                TimeoutException.class,
                IOException.class,
                ReplicationException.class,
                ReplicationTimeoutException.class,
                PrimaryReplicaMissException.class
        );

        /**
         * Check if the provided exception is recoverable. A recoverable transaction is the one that we can send a 'retry' request for.
         *
         * @param throwable Exception to test.
         * @return {@code true} if recoverable, {@code false} otherwise.
         */
        static boolean isRecoverable(Throwable throwable) {
            if (throwable == null) {
                return false;
            }

            Throwable candidate = ExceptionUtils.unwrapCause(throwable);

            for (Class<? extends Throwable> recoverableClass : RECOVERABLE) {
                if (recoverableClass.isAssignableFrom(candidate.getClass())) {
                    return true;
                }
            }

            return false;
        }
    }

    private HybridTimestamp createBeginTimestampWithIncrementRwTxCounter() {
        return localRwTxCounter.inUpdateRwTxCountLock(() -> {
            HybridTimestamp beginTs = clockService.now();

            localRwTxCounter.incrementRwTxCount(beginTs);

            return beginTs;
        });
    }

    private void decrementRwTxCount(UUID txId) {
        localRwTxCounter.inUpdateRwTxCountLock(() -> {
            localRwTxCounter.decrementRwTxCount(beginTimestamp(txId));

            return null;
        });
    }
}
