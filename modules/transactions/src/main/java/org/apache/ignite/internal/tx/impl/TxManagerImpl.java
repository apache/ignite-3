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
import static org.apache.ignite.internal.tx.TransactionIds.beginTimestamp;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;
import static org.apache.ignite.internal.tx.TxState.FINISHING;
import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.apache.ignite.internal.tx.TxState.isFinalState;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_PRIMARY_REPLICA_EXPIRED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_READ_ONLY_TOO_OLD_ERR;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.replicator.message.ErrorReplicaResponse;
import org.apache.ignite.internal.replicator.message.ReplicaMessageGroup;
import org.apache.ignite.internal.replicator.message.ReplicaResponse;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LocalRwTxCounter;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.MismatchingTransactionOutcomeException;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TransactionResult;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxPriority;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.TxStateMetaFinishing;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyService;
import org.jetbrains.annotations.Nullable;

/**
 * A transaction manager implementation.
 *
 * <p>Uses 2PC for atomic commitment and 2PL for concurrency control.
 */
public class TxManagerImpl implements TxManager, NetworkMessageHandler {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TxManagerImpl.class);

    /** Hint for maximum concurrent txns. */
    private static final int MAX_CONCURRENT_TXNS = 1024;

    /** Transaction configuration. */
    private final TransactionConfiguration txConfig;

    /** Lock manager. */
    private final LockManager lockManager;

    /** Executor that runs async transaction cleanup actions. */
    private final ExecutorService cleanupExecutor;

    /** A hybrid logical clock. */
    private final HybridClock clock;

    /** Generates transaction IDs. */
    private final TransactionIdGenerator transactionIdGenerator;

    /** The local state storage. */
    private final VolatileTxStateMetaStorage txStateVolatileStorage = new VolatileTxStateMetaStorage();

    /** Txn contexts. */
    private final ConcurrentHashMap<UUID, TxContext> txCtxMap = new ConcurrentHashMap<>(MAX_CONCURRENT_TXNS);

    /** Future of a read-only transaction by it {@link TxIdAndTimestamp}. */
    private final ConcurrentNavigableMap<TxIdAndTimestamp, CompletableFuture<Void>> readOnlyTxFutureById = new ConcurrentSkipListMap<>(
            Comparator.comparing(TxIdAndTimestamp::getReadTimestamp).thenComparing(TxIdAndTimestamp::getTxId)
    );

    /**
     * Low watermark, does not allow creating read-only transactions less than or equal to this value, {@code null} means it has never been
     * updated yet.
     */
    private final AtomicReference<HybridTimestamp> lowWatermark = new AtomicReference<>();

    private final PlacementDriver placementDriver;

    private final PlacementDriverHelper placementDriverHelper;

    private final LongSupplier idleSafeTimePropagationPeriodMsSupplier;

    /** Prevents double stopping of the tracker. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Detector of transactions that lost the coordinator. */
    private final OrphanDetector orphanDetector;

    /** Topology service. */
    private final TopologyService topologyService;

    /** Cluster service. */
    private final MessagingService messagingService;

    /** Local node network identity. This id is available only after the network has started. */
    private String localNodeId;

    /** Server cleanup processor. */
    private final TxCleanupRequestHandler txCleanupRequestHandler;

    /** Cleanup request sender. */
    private final TxCleanupRequestSender txCleanupRequestSender;

    /** Transaction message sender. */
    private final TxMessageSender txMessageSender;

    private final EventListener<PrimaryReplicaEventParameters> primaryReplicaEventListener;

    /** Counter of read-write transactions that were created and completed locally on the node. */
    private final LocalRwTxCounter localRwTxCounter;

    /**
     * The constructor.
     *
     * @param txConfig Transaction configuration.
     * @param clusterService Cluster service.
     * @param replicaService Replica service.
     * @param lockManager Lock manager.
     * @param clock A hybrid logical clock.
     * @param transactionIdGenerator Used to generate transaction IDs.
     * @param placementDriver Placement driver.
     * @param idleSafeTimePropagationPeriodMsSupplier Used to get idle safe time propagation period in ms.
     * @param localRwTxCounter Counter of read-write transactions that were created and completed locally on the node.
     */
    public TxManagerImpl(
            TransactionConfiguration txConfig,
            ClusterService clusterService,
            ReplicaService replicaService,
            LockManager lockManager,
            HybridClock clock,
            TransactionIdGenerator transactionIdGenerator,
            PlacementDriver placementDriver,
            LongSupplier idleSafeTimePropagationPeriodMsSupplier,
            LocalRwTxCounter localRwTxCounter
    ) {
        this.txConfig = txConfig;
        this.lockManager = lockManager;
        this.clock = clock;
        this.transactionIdGenerator = transactionIdGenerator;
        this.placementDriver = placementDriver;
        this.idleSafeTimePropagationPeriodMsSupplier = idleSafeTimePropagationPeriodMsSupplier;
        this.topologyService = clusterService.topologyService();
        this.messagingService = clusterService.messagingService();
        this.primaryReplicaEventListener = this::primaryReplicaEventListener;
        this.localRwTxCounter = localRwTxCounter;

        placementDriverHelper = new PlacementDriverHelper(placementDriver, clock);

        int cpus = Runtime.getRuntime().availableProcessors();

        cleanupExecutor = new ThreadPoolExecutor(
                cpus,
                cpus,
                100,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                NamedThreadFactory.create(clusterService.nodeName(), "tx-async-cleanup", LOG)
        );

        orphanDetector = new OrphanDetector(topologyService, replicaService, placementDriverHelper, lockManager);

        txMessageSender = new TxMessageSender(messagingService, replicaService, clock);

        var writeIntentSwitchProcessor = new WriteIntentSwitchProcessor(placementDriverHelper, txMessageSender, topologyService);

        txCleanupRequestHandler = new TxCleanupRequestHandler(messagingService, lockManager, clock, writeIntentSwitchProcessor);

        txCleanupRequestSender = new TxCleanupRequestSender(txMessageSender, placementDriverHelper, writeIntentSwitchProcessor);
    }

    private CompletableFuture<Boolean> primaryReplicaEventListener(PrimaryReplicaEventParameters eventParameters, Throwable err) {
        return inBusyLock(busyLock, () -> {
            if (!(eventParameters.groupId() instanceof TablePartitionId)) {
                return falseCompletedFuture();
            }

            TablePartitionId groupId = (TablePartitionId) eventParameters.groupId();

            for (Map.Entry<UUID, TxContext> ctxEntry : txCtxMap.entrySet()) {
                TxContext txContext = ctxEntry.getValue();

                if (txContext.isTxFinishing()) {
                    IgniteBiTuple<ClusterNode, Long> nodeAndToken = txContext.enlistedGroups.get(groupId);

                    if (nodeAndToken != null) {
                        txContext.cancelWaitingInflights(groupId, nodeAndToken.get2());
                    }
                }
            }

            return falseCompletedFuture();
        });
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
        HybridTimestamp beginTimestamp = readOnly ? clock.now() : createBeginTimestampWithIncrementRwTxCounter();
        UUID txId = transactionIdGenerator.transactionIdFor(beginTimestamp, priority);
        updateTxMeta(txId, old -> new TxStateMeta(PENDING, localNodeId, null, null));

        if (!readOnly) {
            return new ReadWriteTransactionImpl(this, timestampTracker, txId, localNodeId);
        }

        HybridTimestamp observableTimestamp = timestampTracker.get();

        HybridTimestamp readTimestamp = observableTimestamp != null
                ? HybridTimestamp.max(observableTimestamp, currentReadTimestamp(beginTimestamp))
                : currentReadTimestamp(beginTimestamp);

        TxIdAndTimestamp txIdAndTimestamp = new TxIdAndTimestamp(readTimestamp, txId);

        CompletableFuture<Void> txFuture = new CompletableFuture<>();

        CompletableFuture<Void> oldFuture = readOnlyTxFutureById.put(txIdAndTimestamp, txFuture);
        assert oldFuture == null : "previous transaction has not completed yet: " + txIdAndTimestamp;

        HybridTimestamp lowWatermark = this.lowWatermark.get();

        if (lowWatermark != null && readTimestamp.compareTo(lowWatermark) <= 0) {
            // "updateLowWatermark" method updates "this.lowWatermark" field, and only then scans "this.readOnlyTxFutureById" for old
            // transactions to wait. In order for that code to work safely, we have to make sure that no "too old" transactions will be
            // created here in "begin" method after "this.lowWatermark" is already updated. The simplest way to achieve that is to check
            // LW after we add transaction to the map (adding transaction to the map before reading LW value, of course).
            readOnlyTxFutureById.remove(txIdAndTimestamp);

            // Completing the future is necessary, because "updateLowWatermark" method may already wait for it if race condition happened.
            txFuture.complete(null);

            throw new IgniteInternalException(
                    TX_READ_ONLY_TOO_OLD_ERR,
                    "Timestamp of read-only transaction must be greater than the low watermark: [txTimestamp={}, lowWatermark={}]",
                    readTimestamp,
                    lowWatermark
            );
        }

        return new ReadOnlyTransactionImpl(this, timestampTracker, txId, localNodeId, readTimestamp);
    }

    /**
     * Current read timestamp, for calculation of read timestamp of read-only transactions.
     *
     * @param beginTx Begin transaction timestamp.
     * @return Current read timestamp.
     */
    private HybridTimestamp currentReadTimestamp(HybridTimestamp beginTx) {
        return beginTx.subtractPhysicalTime(
                idleSafeTimePropagationPeriodMsSupplier.getAsLong() + HybridTimestamp.CLOCK_SKEW
        );
    }

    @Override
    public @Nullable TxStateMeta stateMeta(UUID txId) {
        return inBusyLock(busyLock, () -> txStateVolatileStorage.state(txId));
    }

    @Override
    public @Nullable <T extends TxStateMeta> T updateTxMeta(UUID txId, Function<TxStateMeta, TxStateMeta> updater) {
        return txStateVolatileStorage.updateMeta(txId, updater);
    }

    @Override
    public void finishFull(HybridTimestampTracker timestampTracker, UUID txId, boolean commit) {
        TxState finalState;

        if (commit) {
            timestampTracker.update(clock.now());

            finalState = COMMITTED;
        } else {
            finalState = ABORTED;
        }

        updateTxMeta(txId, old -> new TxStateMeta(finalState, old.txCoordinatorId(), old.commitPartitionId(), old.commitTimestamp()));

        decrementRwTxCount(txId);
    }

    private @Nullable HybridTimestamp commitTimestamp(boolean commit) {
        return commit ? clock.now() : null;
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

        TxContext txContext = lockTxForNewUpdates(txId, enlistedGroups);

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
        });
    }

    private TxContext lockTxForNewUpdates(UUID txId, Map<TablePartitionId, IgniteBiTuple<ClusterNode, Long>> enlistedGroups) {
        return txCtxMap.compute(txId, (uuid, tuple0) -> {
            if (tuple0 == null) {
                tuple0 = new TxContext(placementDriver); // No writes enlisted.
            }

            assert !tuple0.isTxFinishing() : "Transaction is already finished [id=" + uuid + "].";

            tuple0.finishTx(enlistedGroups);

            return tuple0;
        });
    }

    private static CompletableFuture<Void> checkTxOutcome(boolean commit, UUID txId, TransactionMeta stateMeta) {
        if ((stateMeta.txState() == COMMITTED) == commit) {
            return nullCompletedFuture();
        }

        return failedFuture(new MismatchingTransactionOutcomeException(
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

                            Map<ReplicationGroupId, String> replicationGroupIds = enlistedGroups.entrySet().stream()
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
                .thenCompose(Function.identity())
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
            Map<ReplicationGroupId, String> replicationGroupIds,
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

                        if (cause instanceof MismatchingTransactionOutcomeException) {
                            MismatchingTransactionOutcomeException transactionException = (MismatchingTransactionOutcomeException) cause;

                            TransactionResult result = transactionException.transactionResult();

                            TxStateMeta updatedMeta = updateTxMeta(txId, old ->
                                    new TxStateMeta(
                                            result.transactionState(),
                                            old.txCoordinatorId(),
                                            commitPartition,
                                            result.commitTimestamp()
                                    )
                            );

                            txFinishFuture.complete(updatedMeta);

                            return CompletableFuture.<Void>failedFuture(cause);
                        }

                        if (TransactionFailureHandler.isRecoverable(cause)) {
                            LOG.warn("Failed to finish Tx. The operation will be retried [txId={}].", ex, txId);

                            return durableFinish(
                                    observableTimestampTracker,
                                    commitPartition,
                                    commit,
                                    replicationGroupIds,
                                    txId,
                                    commitTimestamp,
                                    txFinishFuture
                            );
                        } else {
                            LOG.warn("Failed to finish Tx [txId={}].", ex, txId);

                            return CompletableFuture.<Void>failedFuture(cause);
                        }
                    }

                    return CompletableFutures.<Void>nullCompletedFuture();
                })
                .thenCompose(Function.identity()));
    }

    private CompletableFuture<Void> makeFinishRequest(
            HybridTimestampTracker observableTimestampTracker,
            TablePartitionId commitPartition,
            String primaryConsistentId,
            Long enlistmentConsistencyToken,
            boolean commit,
            Map<ReplicationGroupId, String> replicationGroupIds,
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
                                    old.commitPartitionId(),
                                    txResult.commitTimestamp()
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

            throw new MismatchingTransactionOutcomeException(
                    "Failed to change the outcome of a finished transaction [txId=" + txId + ", txState=" + txResult.transactionState()
                            + "].",
                    txResult
            );
        }
    }

    @Override
    public int finished() {
        return inBusyLock(busyLock, () -> (int) txStateVolatileStorage.states().stream()
                .filter(e -> isFinalState(e.txState()))
                .count());
    }

    @Override
    public int pending() {
        return inBusyLock(busyLock, () -> (int) txStateVolatileStorage.states().stream()
                .filter(e -> e.txState() == PENDING)
                .count());
    }

    @Override
    public CompletableFuture<Void> start() {
        localNodeId = topologyService.localMember().id();

        messagingService.addMessageHandler(ReplicaMessageGroup.class, this);

        txStateVolatileStorage.start();

        orphanDetector.start(txStateVolatileStorage, txConfig.abandonedCheckTs());

        txCleanupRequestHandler.start();

        placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, primaryReplicaEventListener);

        return nullCompletedFuture();
    }

    @Override
    public void beforeNodeStop() {
        orphanDetector.stop();
        txStateVolatileStorage.stop();
    }

    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        txCleanupRequestHandler.stop();

        placementDriver.removeListener(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, primaryReplicaEventListener);

        shutdownAndAwaitTermination(cleanupExecutor, 10, TimeUnit.SECONDS);
    }

    @Override
    public LockManager lockManager() {
        return lockManager;
    }

    @Override
    public CompletableFuture<Void> cleanup(
            Map<TablePartitionId, String> enlistedPartitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        return txCleanupRequestSender.cleanup(enlistedPartitions, commit, commitTimestamp, txId);
    }

    @Override
    public CompletableFuture<Void> cleanup(String node, UUID txId) {
        return txCleanupRequestSender.cleanup(node, txId);
    }

    @Override
    public CompletableFuture<Void> executeCleanupAsync(Runnable runnable) {
        return runAsync(runnable, cleanupExecutor);
    }

    @Override
    public CompletableFuture<?> executeCleanupAsync(Supplier<CompletableFuture<?>> action) {
        return supplyAsync(action, cleanupExecutor).thenCompose(f -> f);
    }

    CompletableFuture<Void> completeReadOnlyTransactionFuture(TxIdAndTimestamp txIdAndTimestamp) {
        CompletableFuture<Void> readOnlyTxFuture = readOnlyTxFutureById.remove(txIdAndTimestamp);

        assert readOnlyTxFuture != null : txIdAndTimestamp;

        readOnlyTxFuture.complete(null);

        return readOnlyTxFuture;
    }

    @Override
    public CompletableFuture<Void> updateLowWatermark(HybridTimestamp newLowWatermark) {
        lowWatermark.updateAndGet(previousLowWatermark -> {
            if (previousLowWatermark == null) {
                return newLowWatermark;
            }

            assert newLowWatermark.compareTo(previousLowWatermark) > 0 :
                    "lower watermark should be growing: [previous=" + previousLowWatermark + ", new=" + newLowWatermark + ']';

            return newLowWatermark;
        });

        TxIdAndTimestamp upperBound = new TxIdAndTimestamp(newLowWatermark, new UUID(Long.MAX_VALUE, Long.MAX_VALUE));

        List<CompletableFuture<Void>> readOnlyTxFutures = List.copyOf(readOnlyTxFutureById.headMap(upperBound, true).values());

        return allOf(readOnlyTxFutures.toArray(CompletableFuture[]::new));
    }

    @Override
    public boolean addInflight(UUID txId) {
        boolean[] res = {true};

        txCtxMap.compute(txId, (uuid, tuple) -> {
            if (tuple == null) {
                tuple = new TxContext(placementDriver);
            }

            if (tuple.isTxFinishing()) {
                res[0] = false;
                return tuple;
            } else {
                //noinspection NonAtomicOperationOnVolatileField
                tuple.inflights++;
            }

            return tuple;
        });

        return res[0];
    }

    @Override
    public void removeInflight(UUID txId) {
        TxContext tuple = txCtxMap.compute(txId, (uuid, ctx) -> {
            assert ctx != null && ctx.inflights > 0 : ctx;

            //noinspection NonAtomicOperationOnVolatileField
            ctx.inflights--;

            return ctx;
        });

        // Avoid completion under lock.
        tuple.onRemovedInflights();
    }

    @Override
    public void onReceived(NetworkMessage message, String senderConsistentId, @Nullable Long correlationId) {
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
            removeInflight((UUID) result);
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
                                    IgniteStringFormatter.format(
                                            "Commit timestamp is greater than primary replica expiration timestamp:"
                                                    + " [groupId = {}, commit timestamp = {}, primary replica expiration timestamp = {}]",
                                            groupId, commitTimestamp, currentPrimaryReplica.getExpirationTime());
                        }
                    });
        }

        return allOf(verificationFutures);
    }

    private static class TxContext {
        volatile long inflights = 0; // Updated under lock.
        private final CompletableFuture<Void> waitRepFut = new CompletableFuture<>();
        private final PlacementDriver placementDriver;
        volatile CompletableFuture<Void> finishInProgressFuture = null;
        volatile Map<TablePartitionId, IgniteBiTuple<ClusterNode, Long>> enlistedGroups;

        private TxContext(PlacementDriver placementDriver) {
            this.placementDriver = placementDriver;
        }

        CompletableFuture<Void> performFinish(boolean commit, Function<Boolean, CompletableFuture<Void>> finishAction) {
            waitReadyToFinish(commit)
                    .whenComplete((ignoredReadyToFinish, readyException) -> finishAction.apply(commit && readyException == null)
                            .whenComplete((ignoredFinishActionResult, finishException) ->
                                    completeFinishInProgressFuture(commit, readyException, finishException))
                    );

            return finishInProgressFuture;
        }

        private void completeFinishInProgressFuture(
                boolean commit,
                @Nullable Throwable readyToFinishException,
                @Nullable Throwable finishException
        ) {
            if (readyToFinishException == null) {
                if (finishException == null) {
                    finishInProgressFuture.complete(null);
                } else {
                    finishInProgressFuture.completeExceptionally(finishException);
                }
            } else {
                if (commit && readyToFinishException instanceof PrimaryReplicaExpiredException) {
                    finishInProgressFuture.completeExceptionally(new MismatchingTransactionOutcomeException(
                            TX_PRIMARY_REPLICA_EXPIRED_ERR,
                            "Failed to commit the transaction.",
                            new TransactionResult(ABORTED, null),
                            readyToFinishException
                    ));
                } else {
                    finishInProgressFuture.completeExceptionally(readyToFinishException);
                }
            }
        }

        private CompletableFuture<Void> waitReadyToFinish(boolean commit) {
            if (commit) {
                for (Map.Entry<TablePartitionId, IgniteBiTuple<ClusterNode, Long>> e : enlistedGroups.entrySet()) {
                    ReplicaMeta replicaMeta = placementDriver.currentLease(e.getKey());

                    Long enlistmentConsistencyToken = e.getValue().get2();

                    if (replicaMeta == null || !enlistmentConsistencyToken.equals(replicaMeta.getStartTime().longValue())) {
                        return failedFuture(new PrimaryReplicaExpiredException(e.getKey(), enlistmentConsistencyToken, null, replicaMeta));
                    }
                }

                return waitNoInflights();
            } else {
                return nullCompletedFuture();
            }
        }

        private CompletableFuture<Void> waitNoInflights() {
            if (inflights == 0) {
                waitRepFut.complete(null);
            }
            return waitRepFut;
        }

        private void cancelWaitingInflights(TablePartitionId groupId, Long enlistmentConsistencyToken) {
            waitRepFut.completeExceptionally(new PrimaryReplicaExpiredException(groupId, enlistmentConsistencyToken, null, null));
        }

        void onRemovedInflights() {
            if (inflights == 0 && finishInProgressFuture != null) {
                waitRepFut.complete(null);
            }
        }

        void finishTx(Map<TablePartitionId, IgniteBiTuple<ClusterNode, Long>> enlistedGroups) {
            this.enlistedGroups = enlistedGroups;
            finishInProgressFuture = new CompletableFuture<>();
        }

        boolean isTxFinishing() {
            return finishInProgressFuture != null;
        }

        @Override
        public String toString() {
            return "TxContext [inflights=" + inflights + ", waitRepFut=" + waitRepFut + ", finishFut=" + finishInProgressFuture + ']';
        }
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
            HybridTimestamp beginTs = clock.now();

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
