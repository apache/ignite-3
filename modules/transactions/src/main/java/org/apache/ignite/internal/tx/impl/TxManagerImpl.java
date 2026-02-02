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

import static java.lang.Math.toIntExact;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.tx.InternalTransaction.USE_CONFIGURED_TIMEOUT_DEFAULT;
import static org.apache.ignite.internal.tx.TransactionIds.beginTimestamp;
import static org.apache.ignite.internal.tx.TransactionLogUtils.formatTxInfo;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;
import static org.apache.ignite.internal.tx.TxState.FINISHING;
import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.apache.ignite.internal.tx.TxState.isFinalState;
import static org.apache.ignite.internal.tx.TxStateMeta.builder;
import static org.apache.ignite.internal.tx.TxStateMeta.recordExceptionInfo;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemPropertyView;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.failure.handlers.NoOpFailureHandler;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicatorRecoverableExceptions;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ErrorReplicaResponse;
import org.apache.ignite.internal.replicator.message.ReplicaMessageGroup;
import org.apache.ignite.internal.replicator.message.ReplicaResponse;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViewProvider;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.tx.DelayedAckException;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.InternalTxOptions;
import org.apache.ignite.internal.tx.LocalRwTxCounter;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.MismatchingTransactionOutcomeInternalException;
import org.apache.ignite.internal.tx.OutdatedReadOnlyTransactionInternalException;
import org.apache.ignite.internal.tx.PartitionEnlistment;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TransactionResult;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.TxStateMetaFinishing;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.DeadlockPreventionPolicyImpl.TxIdComparators;
import org.apache.ignite.internal.tx.impl.TransactionInflights.ReadWriteTxContext;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicatedInfo;
import org.apache.ignite.internal.tx.metrics.ResourceVacuumMetrics;
import org.apache.ignite.internal.tx.metrics.TransactionMetricsSource;
import org.apache.ignite.internal.tx.views.LocksViewProvider;
import org.apache.ignite.internal.tx.views.TransactionsViewProvider;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * A transaction manager implementation.
 *
 * <p>Uses 2PC for atomic commitment and 2PL for concurrency control.
 */
public class TxManagerImpl implements TxManager, NetworkMessageHandler, SystemViewProvider {
    private static final String ABANDONED_CHECK_TS_PROP = "txnAbandonedCheckTs";

    private static final long ABANDONED_CHECK_TS_PROP_DEFAULT_VALUE = 5_000;

    private static final String LOCK_RETRY_COUNT_PROP = "txnLockRetryCount";

    private static final int LOCK_RETRY_COUNT_PROP_DEFAULT_VALUE = 3;

    public static final String RESOURCE_TTL_PROP = "txnResourceTtl";

    private static final int RESOURCE_TTL_PROP_DEFAULT_VALUE = 30 * 1000;

    private static final TxIdComparators DEFAULT_TX_ID_COMPARATOR = TxIdComparators.NATURAL;

    private static final long DEFAULT_LOCK_TIMEOUT = 0;

    /** Expiration trigger frequency. */
    private static final long EXPIRE_FREQ_MILLIS = 1000;

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TxManagerImpl.class);

    /** Transaction configuration. */
    private final TransactionConfiguration txConfig;

    /** Transaction configuration. */
    private final SystemDistributedConfiguration systemCfg;

    /** Lock manager. */
    private final LockManager lockManager;

    /** Executor that runs async write intent switch actions. */
    private final ExecutorService writeIntentSwitchPool;

    private final ClockService clockService;

    /** Generates transaction IDs. */
    private final TransactionIdGenerator transactionIdGenerator;

    /** The local state storage. */
    private final VolatileTxStateMetaStorage txStateVolatileStorage;

    /** Low watermark. */
    private final LowWatermark lowWatermark;

    private final PlacementDriver placementDriver;

    private final PlacementDriverHelper placementDriverHelper;

    private final LongSupplier idleSafeTimePropagationPeriodMsSupplier;

    /** Prevents double stopping of the tracker. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

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

    private final ScheduledExecutorService commonScheduler;

    private final FailureProcessor failureProcessor;

    private final TransactionsViewProvider txViewProvider = new TransactionsViewProvider();

    private volatile PersistentTxStateVacuumizer persistentTxStateVacuumizer;

    private final TransactionExpirationRegistry transactionExpirationRegistry;

    private volatile @Nullable ScheduledFuture<?> transactionExpirationJobFuture;

    private volatile int lockRetryCount = 0;

    private final MetricManager metricsManager;

    private final TransactionMetricsSource txMetrics;

    private volatile boolean isStopping;

    private final ConcurrentLinkedQueue<CompletableFuture<?>> stopFuts = new ConcurrentLinkedQueue<>();

    /**
     * Test-only constructor.
     *
     * @param txConfig Transaction configuration.
     * @param systemCfg System configuration.
     * @param clusterService Cluster service.
     * @param replicaService Replica service.
     * @param lockManager Lock manager.
     * @param txStateVolatileStorage The local state storage.
     * @param clockService Clock service.
     * @param transactionIdGenerator Used to generate transaction IDs.
     * @param placementDriver Placement driver.
     * @param idleSafeTimePropagationPeriodMsSupplier Used to get idle safe time propagation period in ms.
     * @param localRwTxCounter Counter of read-write transactions that were created and completed locally on the node.
     * @param resourcesRegistry Resources registry.
     * @param transactionInflights Transaction inflights.
     * @param lowWatermark Low watermark.
     * @param metricManager Metric manager.
     */
    @TestOnly
    public TxManagerImpl(
            TransactionConfiguration txConfig,
            SystemDistributedConfiguration systemCfg,
            ClusterService clusterService,
            ReplicaService replicaService,
            LockManager lockManager,
            VolatileTxStateMetaStorage txStateVolatileStorage,
            ClockService clockService,
            TransactionIdGenerator transactionIdGenerator,
            PlacementDriver placementDriver,
            LongSupplier idleSafeTimePropagationPeriodMsSupplier,
            LocalRwTxCounter localRwTxCounter,
            RemotelyTriggeredResourceRegistry resourcesRegistry,
            TransactionInflights transactionInflights,
            LowWatermark lowWatermark,
            ScheduledExecutorService commonScheduler,
            MetricManager metricManager
    ) {
        this(
                clusterService.nodeName(),
                txConfig,
                systemCfg,
                clusterService.messagingService(),
                clusterService.topologyService(),
                replicaService,
                lockManager,
                txStateVolatileStorage,
                clockService,
                transactionIdGenerator,
                placementDriver,
                idleSafeTimePropagationPeriodMsSupplier,
                localRwTxCounter,
                ForkJoinPool.commonPool(),
                resourcesRegistry,
                transactionInflights,
                lowWatermark,
                commonScheduler,
                new FailureManager(new NoOpFailureHandler()),
                metricManager
        );
    }

    /**
     * The constructor.
     *
     * @param txConfig Transaction configuration.
     * @param systemCfg System configuration.
     * @param messagingService Messaging service.
     * @param topologyService Topology service.
     * @param replicaService Replica service.
     * @param lockManager Lock manager.
     * @param txStateVolatileStorage The local state storage.
     * @param clockService Clock service.
     * @param transactionIdGenerator Used to generate transaction IDs.
     * @param placementDriver Placement driver.
     * @param idleSafeTimePropagationPeriodMsSupplier Used to get idle safe time propagation period in ms.
     * @param localRwTxCounter Counter of read-write transactions that were created and completed locally on the node.
     * @param partitionOperationsExecutor Executor on which partition operations will be executed, if needed.
     * @param resourcesRegistry Resources registry.
     * @param transactionInflights Transaction inflights.
     * @param lowWatermark Low watermark.
     * @param metricManager Metric manager.
     */
    public TxManagerImpl(
            String nodeName,
            TransactionConfiguration txConfig,
            SystemDistributedConfiguration systemCfg,
            MessagingService messagingService,
            TopologyService topologyService,
            ReplicaService replicaService,
            LockManager lockManager,
            VolatileTxStateMetaStorage txStateVolatileStorage,
            ClockService clockService,
            TransactionIdGenerator transactionIdGenerator,
            PlacementDriver placementDriver,
            LongSupplier idleSafeTimePropagationPeriodMsSupplier,
            LocalRwTxCounter localRwTxCounter,
            Executor partitionOperationsExecutor,
            RemotelyTriggeredResourceRegistry resourcesRegistry,
            TransactionInflights transactionInflights,
            LowWatermark lowWatermark,
            ScheduledExecutorService commonScheduler,
            FailureProcessor failureProcessor,
            MetricManager metricManager
    ) {
        this.txConfig = txConfig;
        this.systemCfg = systemCfg;
        this.lockManager = lockManager;
        this.txStateVolatileStorage = txStateVolatileStorage;
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
        this.commonScheduler = commonScheduler;
        this.failureProcessor = failureProcessor;
        this.metricsManager = metricManager;

        placementDriverHelper = new PlacementDriverHelper(placementDriver, clockService);

        int cpus = Runtime.getRuntime().availableProcessors();

        writeIntentSwitchPool = Executors.newFixedThreadPool(
                cpus,
                IgniteThreadFactory.create(nodeName, "tx-async-write-intent", LOG, STORAGE_READ, STORAGE_WRITE)
        );

        orphanDetector = new OrphanDetector(
                topologyService,
                replicaService,
                placementDriverHelper,
                lockManager,
                partitionOperationsExecutor
        );

        txMessageSender = new TxMessageSender(messagingService, replicaService, clockService);

        var writeIntentSwitchProcessor = new WriteIntentSwitchProcessor(placementDriverHelper, txMessageSender,
                topologyService, txStateVolatileStorage);

        txCleanupRequestHandler = new TxCleanupRequestHandler(
                messagingService,
                lockManager,
                clockService,
                writeIntentSwitchProcessor,
                resourcesRegistry,
                writeIntentSwitchPool,
                txStateVolatileStorage
        );

        transactionExpirationRegistry = new TransactionExpirationRegistry(txStateVolatileStorage);

        txCleanupRequestSender = new TxCleanupRequestSender(
                txMessageSender,
                placementDriverHelper,
                txStateVolatileStorage,
                writeIntentSwitchPool,
                commonScheduler
        );

        txMetrics = new TransactionMetricsSource(clockService);
    }

    private CompletableFuture<Boolean> primaryReplicaEventListener(
            PrimaryReplicaEventParameters eventParameters,
            Consumer<ZonePartitionId> action
    ) {
        assert eventParameters.groupId() instanceof ZonePartitionId :
                "Invalid replication group type: " + eventParameters.groupId().getClass();

        action.accept((ZonePartitionId) eventParameters.groupId());

        return falseCompletedFuture();
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
    public InternalTransaction beginImplicit(HybridTimestampTracker timestampTracker, boolean readOnly, String txLabel) {
        if (readOnly) {
            return new ReadOnlyImplicitTransactionImpl(timestampTracker, clockService.current());
        }

        HybridTimestamp beginTimestamp = createBeginTimestampWithIncrementRwTxCounter();
        var tx = beginReadWriteTransaction(timestampTracker, beginTimestamp, true, InternalTxOptions.defaults());

        txStateVolatileStorage.initialize(tx, txLabel);
        txMetrics.onTransactionStarted();

        return tx;
    }

    @Override
    public InternalTransaction beginExplicit(HybridTimestampTracker timestampTracker, boolean readOnly, InternalTxOptions txOptions) {
        InternalTransaction tx;

        if (readOnly) {
            HybridTimestamp beginTimestamp = clockService.now(); // Tick to generate new unique id.
            tx = beginReadOnlyTransaction(timestampTracker, beginTimestamp, txOptions);
        } else {
            HybridTimestamp beginTimestamp = createBeginTimestampWithIncrementRwTxCounter();
            tx = beginReadWriteTransaction(timestampTracker, beginTimestamp, false, txOptions);
        }

        txStateVolatileStorage.initialize(tx, txOptions.txLabel());
        txMetrics.onTransactionStarted();

        return tx;
    }

    private ReadWriteTransactionImpl beginReadWriteTransaction(
            HybridTimestampTracker timestampTracker,
            HybridTimestamp beginTimestamp,
            boolean implicit,
            InternalTxOptions options
    ) {
        UUID txId = transactionIdGenerator.transactionIdFor(beginTimestamp, options.priority());

        long timeout = getTimeoutOrDefault(options, txConfig.readWriteTimeoutMillis().value());

        var transaction = new ReadWriteTransactionImpl(
                this,
                timestampTracker,
                txId,
                localNodeId,
                implicit,
                timeout
        );

        // Implicit transactions are finished as soon as their operation/query is finished, they cannot be abandoned, so there is
        // no need to register them.
        // TODO: https://issues.apache.org/jira/browse/IGNITE-24229 - schedule expiration for multi-key implicit transactions?
        if (!implicit) {
            // TODO IGNITE-26531 Unregister is not called on finish.
            transactionExpirationRegistry.register(transaction);

            if (isStopping) {
                transaction.fail(new TransactionException(Common.NODE_STOPPING_ERR,
                        format("Failed to finish the transaction because a node is stopping: {}.",
                                formatTxInfo(txId, txStateVolatileStorage))));
            }
        }

        return transaction;
    }

    private static long getTimeoutOrDefault(InternalTxOptions options, long defaultValue) {
        return options.timeoutMillis() == USE_CONFIGURED_TIMEOUT_DEFAULT ? defaultValue : options.timeoutMillis();
    }

    private ReadOnlyTransactionImpl beginReadOnlyTransaction(
            HybridTimestampTracker timestampTracker,
            HybridTimestamp beginTimestamp,
            InternalTxOptions options
    ) {
        UUID txId = transactionIdGenerator.transactionIdFor(beginTimestamp, options.priority());

        HybridTimestamp readTimestamp = options.readTimestamp();

        if (readTimestamp == null) {
            HybridTimestamp observableTimestamp = timestampTracker.get();

            readTimestamp = observableTimestamp != null
                    ? HybridTimestamp.max(observableTimestamp, currentReadTimestamp(beginTimestamp))
                    : currentReadTimestamp(beginTimestamp);
        }

        boolean lockAcquired = lowWatermark.tryLock(txId, readTimestamp);
        if (!lockAcquired) {
            throw new OutdatedReadOnlyTransactionInternalException(
                    "Attempted to read data below the garbage collection watermark",
                    readTimestamp,
                    lowWatermark.getLowWatermark());
        }

        try {
            CompletableFuture<Void> txFuture = new CompletableFuture<>();

            long timeout = getTimeoutOrDefault(options, txConfig.readOnlyTimeoutMillis().value());

            var transaction = new ReadOnlyTransactionImpl(
                    this, timestampTracker, txId, localNodeId, timeout, readTimestamp, txFuture
            );

            transactionExpirationRegistry.register(transaction);

            txFuture.whenComplete((unused, throwable) -> {
                lowWatermark.unlock(txId);

                transactionExpirationRegistry.unregister(transaction);
            });

            return transaction;
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
        return beginTx.subtractPhysicalTime(idleSafeTimePropagationPeriodMsSupplier.getAsLong() + clockService.maxClockSkewMillis());
    }

    @Override
    public @Nullable TxStateMeta stateMeta(UUID txId) {
        return txStateVolatileStorage.state(txId);
    }

    @Override
    public CompletableFuture<@Nullable TransactionMeta> checkEnlistedPartitionsAndAbortIfNeeded(
            TxStateMeta txMeta,
            InternalTransaction tx,
            long currentEnlistmentConsistencyToken,
            ZonePartitionId senderGroupId
    ) {
        PendingTxPartitionEnlistment enlistment = tx.enlistedPartition(senderGroupId);

        if (enlistment != null && enlistment.consistencyToken() != currentEnlistmentConsistencyToken) {
            // Remote partition already has different consistency token, so we can't commit this transaction anyway.
            // Even when graceful primary replica switch is done, we can get here only if the write intent that requires resolution
            // is not under lock.
            return tx.rollbackWithExceptionAsync(
                    new PrimaryReplicaExpiredException(senderGroupId, enlistment.consistencyToken(), null, null))
                    .thenApply(unused -> {
                        TxStateMeta newMeta = stateMeta(tx.id());

                        assert isFinalState(newMeta.txState());

                        return newMeta;
                    });
        }

        return completedFuture(txMeta);
    }

    @TestOnly
    public Collection<TxStateMeta> states() {
        return txStateVolatileStorage.states();
    }

    @Override
    public @Nullable <T extends TxStateMeta> T updateTxMeta(UUID txId, Function<@Nullable TxStateMeta, TxStateMeta> updater) {
        return txStateVolatileStorage.updateMeta(txId, updater);
    }

    @Override
    public void finishFull(
            HybridTimestampTracker timestampTracker,
            UUID txId,
            @Nullable HybridTimestamp ts,
            boolean commit,
            boolean timeoutExceeded
    ) {
        TxState finalState;

        if (commit) {
            assert ts != null : "RW transaction commit timestamp cannot be null.";

            timestampTracker.update(ts);

            finalState = COMMITTED;
        } else {
            finalState = ABORTED;
        }

        updateTxMeta(txId, old -> builder(old, finalState)
                .commitTimestamp(ts)
                .finishedDueToTimeout(timeoutExceeded)
                .build()
        );

        txMetrics.onReadWriteTransactionFinished(txId, finalState == COMMITTED);

        decrementRwTxCount(txId);
    }

    private @Nullable HybridTimestamp commitTimestamp(boolean commit) {
        return commit ? clockService.now() : null;
    }

    @Override
    public CompletableFuture<Void> finish(
            HybridTimestampTracker observableTimestampTracker,
            @Nullable ZonePartitionId commitPartition,
            boolean commitIntent,
            boolean timeout,
            boolean recovery,
            boolean noRemoteWrites,
            Map<ZonePartitionId, PendingTxPartitionEnlistment> enlistedGroups,
            UUID txId
    ) {
        LOG.debug("Finish [commit={}, {}, groups={}, commitPartId={}].", commitIntent,
                formatTxInfo(txId, txStateVolatileStorage, false), enlistedGroups, commitPartition);

        assert enlistedGroups != null;

        if (enlistedGroups.isEmpty()) {
            // If there are no enlisted groups, just update local state - we already marked the tx as finished.
            updateTxMeta(txId, old -> builder(old, commitIntent ? COMMITTED : ABORTED)
                    .txCoordinatorId(localNodeId)
                    .commitPartitionId(commitPartition)
                    .commitTimestamp(commitTimestamp(commitIntent))
                    .finishedDueToTimeout(timeout)
                    .build()
            );

            txMetrics.onReadWriteTransactionFinished(txId, commitIntent);

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
                        ? new TxStateMetaFinishing(null, commitPartition, timeout, null)
                        : txMeta.finishing(timeout);

        TxStateMeta stateMeta = updateTxMeta(txId, oldMeta -> finishingStateMeta);

        // Means we failed to CAS the state, someone else did it.
        if (finishingStateMeta != stateMeta) {
            // If the state is FINISHING then someone else is in the middle of finishing this tx.
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
                        finishingStateMeta.txFinishFuture(),
                        txContext.isNoWrites() && noRemoteWrites && !recovery
                )
        ).whenComplete((unused, throwable) -> {
            if (throwable != null) {
                updateTxMeta(txId, old -> recordExceptionInfo(old, throwable));
            }

            if (localNodeId.equals(finishingStateMeta.txCoordinatorId())) {
                txMetrics.onReadWriteTransactionFinished(txId, commitIntent && throwable == null);

                decrementRwTxCount(txId);
            }

            transactionInflights.removeTxContext(txId);
        });
    }

    private CompletableFuture<Void> checkTxOutcome(boolean commit, UUID txId, TransactionMeta stateMeta) {
        if ((stateMeta.txState() == COMMITTED) == commit) {
            return nullCompletedFuture();
        }

        return failedFuture(new MismatchingTransactionOutcomeInternalException(
                format("Failed to change the outcome of a finished transaction [{}, txState={}].",
                        formatTxInfo(txId, txStateVolatileStorage, false), stateMeta.txState()),
                new TransactionResult(stateMeta.txState(), stateMeta.commitTimestamp()))
        );
    }

    private CompletableFuture<Void> prepareFinish(
            HybridTimestampTracker observableTimestampTracker,
            @Nullable ZonePartitionId commitPartition,
            boolean commit,
            Map<ZonePartitionId, PendingTxPartitionEnlistment> enlistedGroups,
            UUID txId,
            CompletableFuture<TransactionMeta> txFinishFuture,
            boolean unlockOnly
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

                            Map<ZonePartitionId, PartitionEnlistment> groups = enlistedGroups.entrySet().stream()
                                    .collect(toMap(Entry::getKey, Entry::getValue));

                            if (unlockOnly) {
                                return txCleanupRequestSender.cleanup(null, groups, verifiedCommit, commitTimestamp, txId)
                                        .thenAccept(ignored -> {
                                            // Don't keep useless state.
                                            TxStateMeta previous = txStateVolatileStorage.state(txId);
                                            txStateVolatileStorage.updateMeta(txId, old -> null);

                                            TxStateMeta meta = builder(verifiedCommit ? COMMITTED : ABORTED)
                                                    .txCoordinatorId(localNodeId)
                                                    .commitTimestamp(commitTimestamp)
                                                    .cleanupCompletionTimestamp(System.currentTimeMillis())
                                                    .txLabel(previous == null ? null : previous.txLabel())
                                                    .build();

                                            txFinishFuture.complete(meta);
                                        });
                            }

                            return durableFinish(
                                    observableTimestampTracker,
                                    commitPartition,
                                    verifiedCommit,
                                    groups,
                                    txId,
                                    commitTimestamp,
                                    txFinishFuture);
                        })
                .thenCompose(identity())
                // Verification future is added in order to share the proper verification exception with the client.
                .thenCompose(r -> verificationFuture);
    }

    private <T> CompletableFuture<T> trackFuture(CompletableFuture<T> fut) {
        if (fut.isDone()) {
            return fut;
        }

        if (isStopping) {
            // Track finish future.
            stopFuts.add(fut);
        }

        return fut;
    }

    /**
     * Durable finish request.
     */
    private CompletableFuture<Void> durableFinish(
            HybridTimestampTracker observableTimestampTracker,
            ZonePartitionId commitPartition,
            boolean commit,
            Map<ZonePartitionId, PartitionEnlistment> enlistedPartitions,
            UUID txId,
            HybridTimestamp commitTimestamp,
            CompletableFuture<TransactionMeta> txFinishFuture
    ) {
        return trackFuture(placementDriverHelper.awaitPrimaryReplicaWithExceptionHandling(commitPartition)
                .thenCompose(meta ->
                        sendFinishRequest(
                                observableTimestampTracker,
                                commitPartition,
                                meta.getLeaseholder(),
                                meta.getStartTime().longValue(),
                                commit,
                                enlistedPartitions,
                                txId,
                                commitTimestamp,
                                txFinishFuture
                        ))
                .handle((res, ex) -> {
                    if (ex != null) {
                        Throwable cause = ExceptionUtils.unwrapRootCause(ex);

                        if (cause instanceof MismatchingTransactionOutcomeInternalException) {
                            MismatchingTransactionOutcomeInternalException transactionException =
                                    (MismatchingTransactionOutcomeInternalException) cause;

                            TransactionResult result = transactionException.transactionResult();

                            TxStateMeta updatedMeta = updateTxMeta(txId, old -> builder(old, result.transactionState())
                                    .commitPartitionId(commitPartition)
                                    .commitTimestamp(result.commitTimestamp())
                                    .build()
                            );

                            txFinishFuture.complete(updatedMeta);

                            return CompletableFuture.<Void>failedFuture(cause);
                        }

                        if (ReplicatorRecoverableExceptions.isRecoverable(cause)) {
                            LOG.debug("Failed to finish Tx. The operation will be retried {}.", ex,
                                    formatTxInfo(txId, txStateVolatileStorage));

                            return supplyAsync(() -> durableFinish(
                                    observableTimestampTracker,
                                    commitPartition,
                                    commit,
                                    enlistedPartitions,
                                    txId,
                                    commitTimestamp,
                                    txFinishFuture
                            ), partitionOperationsExecutor).thenCompose(identity());
                        } else {
                            LOG.warn("Failed to finish Tx {}.", ex,
                                    formatTxInfo(txId, txStateVolatileStorage));

                            return CompletableFuture.<Void>failedFuture(cause);
                        }
                    }

                    return CompletableFutures.<Void>nullCompletedFuture();
                })
                .thenCompose(identity()));
    }

    private CompletableFuture<Void> sendFinishRequest(
            HybridTimestampTracker observableTimestampTracker,
            ZonePartitionId commitPartition,
            String primaryConsistentId,
            Long enlistmentConsistencyToken,
            boolean commit,
            Map<ZonePartitionId, PartitionEnlistment> enlistedPartitions,
            UUID txId,
            HybridTimestamp commitTimestamp,
            CompletableFuture<TransactionMeta> txFinishFuture
    ) {
        LOG.debug("Finish [partition={}, node={}, enlistmentConsistencyToken={}, commit={}, {}, groups={}",
                commitPartition, primaryConsistentId, enlistmentConsistencyToken, commit,
                formatTxInfo(txId, txStateVolatileStorage, false), enlistedPartitions);

        return txMessageSender.finish(
                        primaryConsistentId,
                        commitPartition,
                        enlistedPartitions,
                        txId,
                        enlistmentConsistencyToken,
                        commit,
                        commitTimestamp
                )
                .thenAccept(txResult -> {
                    validateTxFinishedAsExpected(commit, txId, txResult);

                    TxStateMeta updatedMeta = updateTxMeta(txId, old -> builder(old, txResult.transactionState())
                            .txCoordinatorId(localNodeId)
                            .commitTimestamp(txResult.commitTimestamp())
                            .build()
                    );

                    assert isFinalState(updatedMeta.txState()) :
                            "Unexpected transaction state [id=" + txId + ", state=" + updatedMeta.txState() + "].";

                    txFinishFuture.complete(updatedMeta);

                    if (commit) {
                        observableTimestampTracker.update(commitTimestamp);
                    }
                });
    }

    private void validateTxFinishedAsExpected(boolean commit, UUID txId, TransactionResult txResult) {
        if (commit != (txResult.transactionState() == COMMITTED)) {
            LOG.error("Failed to finish a transaction that is already finished [{}, expectedState={}, actualState={}].",
                    formatTxInfo(txId, txStateVolatileStorage, false),
                    commit ? COMMITTED : ABORTED,
                    txResult.transactionState()
            );

            throw new MismatchingTransactionOutcomeInternalException(
                    format("Failed to change the outcome of a finished transaction [{}, txState={}].",
                            formatTxInfo(txId, txStateVolatileStorage, false), txResult.transactionState()),
                    txResult
            );
        }
    }

    @Override
    public int finished() {
        return (int) txMetrics.finishedTransactions();
    }

    @Override
    public int pending() {
        return (int) txMetrics.activeTransactions();
    }

    @Override
    public InternalTransaction beginRemote(
            UUID txId,
            ZonePartitionId commitPartId,
            UUID coord,
            long token,
            long timeout,
            Consumer<Throwable> cb
    ) {
        assert commitPartId.zoneId() >= 0 && commitPartId.partitionId() >= 0 : "Illegal condition for direct mapping: " + commitPartId;

        // Switch to default timeout if needed.
        timeout = timeout == USE_CONFIGURED_TIMEOUT_DEFAULT ? txConfig.readWriteTimeoutMillis().value() : timeout;

        // Adjust the timeout so local expiration happens after coordinator expiration.
        var tx = new RemoteReadWriteTransaction(txId, commitPartId, coord, token, topologyService.localMember(),
                timeout + clockService.maxClockSkewMillis()) {
            boolean isTimeout = false;
            TxState txState = PENDING;

            @Override
            public TxState state() {
                return txState;
            }

            @Override
            public CompletableFuture<Void> rollbackTimeoutExceededAsync() {
                isTimeout = true;

                // Directly mapped entries become abandoned on local tx timeout.
                // Release locks to allow write intent resolution on abandoned path.
                // Can be safely retried multiple times, because releaseAll is idempotent.
                partitionOperationsExecutor.execute(() -> lockManager.releaseAll(txId));

                return nullCompletedFuture();
            }

            @Override
            public boolean isRolledBackWithTimeoutExceeded() {
                return isTimeout;
            }

            @Override
            public CompletableFuture<Void> kill() {
                txState = ABORTED; // We use ABORTED state for remote txn to indicate no write enlistment.

                return nullCompletedFuture();
            }

            @Override
            public void processDelayedAck(Object ignored, @Nullable Throwable err) {
                try {
                    cb.accept(err);
                } catch (Throwable t) {
                    // We can't do anything with the exception, only log it.
                    LOG.error("Failed to process delayed ack {}", t, formatTxInfo(txId, txStateVolatileStorage));
                }
            }
        };

        transactionExpirationRegistry.register(tx);

        return tx;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        var deadlockPreventionPolicy = new DeadlockPreventionPolicyImpl(DEFAULT_TX_ID_COMPARATOR, DEFAULT_LOCK_TIMEOUT);
        txStateVolatileStorage.start();

        // TODO https://issues.apache.org/jira/browse/IGNITE-23539
        lockManager.start(deadlockPreventionPolicy);

        localNodeId = topologyService.localMember().id();

        messagingService.addMessageHandler(ReplicaMessageGroup.class, this);

        persistentTxStateVacuumizer = new PersistentTxStateVacuumizer(
                replicaService,
                topologyService.localMember(),
                clockService,
                placementDriver,
                failureProcessor
        );

        txViewProvider.init(localNodeId, txStateVolatileStorage.statesMap());

        orphanDetector.start(txStateVolatileStorage,
                () -> longProperty(systemCfg, ABANDONED_CHECK_TS_PROP, ABANDONED_CHECK_TS_PROP_DEFAULT_VALUE));

        txCleanupRequestSender.start();

        txCleanupRequestHandler.start();

        placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, primaryReplicaExpiredListener);

        placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, primaryReplicaElectedListener);

        transactionExpirationJobFuture = commonScheduler.scheduleAtFixedRate(this::expireTransactionsUpToNow,
                EXPIRE_FREQ_MILLIS, EXPIRE_FREQ_MILLIS, MILLISECONDS);

        lockRetryCount = toIntExact(longProperty(systemCfg, LOCK_RETRY_COUNT_PROP, LOCK_RETRY_COUNT_PROP_DEFAULT_VALUE));

        metricsManager.registerSource(txMetrics);
        metricsManager.enable(txMetrics);

        return nullCompletedFuture();
    }

    private void expireTransactionsUpToNow() {
        HybridTimestamp expirationTime = null;

        try {
            expirationTime = clockService.current();
            transactionExpirationRegistry.expireUpTo(expirationTime.getPhysical());
        } catch (Throwable t) {
            failureProcessor.process(new FailureContext(t, String.format("Could not expire transactions up to %s", expirationTime)));
        }
    }

    @Override
    public void beforeNodeStop() {
        isStopping = true;
        orphanDetector.stop();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        // Wait all pending finish futures.
        List<CompletableFuture<?>> toWait = new ArrayList<>();

        for (CompletableFuture<?> stopFut : stopFuts) {
            if (!stopFut.isDone()) {
                toWait.add(stopFut);
            }
        }

        stopFuts.clear();

        LOG.debug("Waiting for tx finish futures on shutdown [cnt=" + toWait.size() + ']');

        return CompletableFutures.allOf(toWait).handle((r, e) -> {
            txStateVolatileStorage.stop();

            txCleanupRequestHandler.stop();

            placementDriver.removeListener(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, primaryReplicaExpiredListener);

            placementDriver.removeListener(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, primaryReplicaElectedListener);

            ScheduledFuture<?> expirationJobFuture = transactionExpirationJobFuture;
            if (expirationJobFuture != null) {
                expirationJobFuture.cancel(false);
            }

            transactionExpirationRegistry.abortAllRegistered();

            shutdownAndAwaitTermination(writeIntentSwitchPool, 10, TimeUnit.SECONDS);

            return null;
        });
    }

    @Override
    public LockManager lockManager() {
        return lockManager;
    }

    @Override
    public CompletableFuture<Void> cleanup(
            @Nullable ZonePartitionId commitPartitionId,
            Map<ZonePartitionId, PartitionEnlistment> enlistedPartitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        return txCleanupRequestSender.cleanup(commitPartitionId, enlistedPartitions, commit, commitTimestamp, txId);
    }

    @Override
    public CompletableFuture<Void> cleanup(
            ZonePartitionId commitPartitionId,
            Collection<EnlistedPartitionGroup> enlistedPartitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        return txCleanupRequestSender.cleanup(commitPartitionId, enlistedPartitions, commit, commitTimestamp, txId);
    }

    @Override
    public CompletableFuture<Void> cleanup(ZonePartitionId commitPartitionId, String node, UUID txId) {
        return txCleanupRequestSender.cleanup(commitPartitionId, node, txId);
    }

    @Override
    public CompletableFuture<Void> vacuum(ResourceVacuumMetrics resourceVacuumMetrics) {
        if (persistentTxStateVacuumizer == null) {
            return nullCompletedFuture(); // Not started yet.
        }

        long vacuumObservationTimestamp = System.currentTimeMillis();

        return txStateVolatileStorage.vacuum(
                vacuumObservationTimestamp,
                longProperty(systemCfg, RESOURCE_TTL_PROP, RESOURCE_TTL_PROP_DEFAULT_VALUE),
                persistentTxStateVacuumizer::vacuumPersistentTxStates,
                resourceVacuumMetrics
        );
    }

    @Override
    public CompletableFuture<Boolean> kill(UUID txId) {
        TxStateMeta state = txStateVolatileStorage.state(txId);

        if (state != null && state.tx() != null) {
            // TODO: IGNITE-24382 Kill implicit read-write transaction.
            if (!state.tx().isReadOnly() && state.tx().implicit()) {
                return falseCompletedFuture();
            }

            return state.tx().kill().thenApply(unused -> true);
        }

        return falseCompletedFuture();
    }

    @Override
    public int lockRetryCount() {
        return lockRetryCount;
    }

    @Override
    public CompletableFuture<Void> executeWriteIntentSwitchAsync(Runnable runnable) {
        return runAsync(runnable, writeIntentSwitchPool);
    }

    void onCompleteReadOnlyTransaction(boolean commitIntent, TxIdAndTimestamp txIdAndTimestamp) {
        UUID txId = txIdAndTimestamp.getTxId();

        txMetrics.onReadOnlyTransactionFinished(txId, commitIntent);

        transactionInflights.markReadOnlyTxFinished(txId);
    }

    @Override
    public void onReceived(NetworkMessage message, InternalClusterNode sender, @Nullable Long correlationId) {
        if (!(message instanceof ReplicaResponse) || correlationId != null) {
            return;
        }

        // Only removing the failed inflight here. A transaction will be rolled back in other place.
        if (message instanceof ErrorReplicaResponse) {
            ErrorReplicaResponse response = (ErrorReplicaResponse) message;

            Throwable err = response.throwable();

            Throwable cause = ExceptionUtils.unwrapCause(err);

            if (cause instanceof DelayedAckException) {
                // Keep compatibility.
                DelayedAckException err0 = (DelayedAckException) cause;

                transactionInflights.removeInflight(err0.txId(), err0);
            }

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
            Map<ZonePartitionId, PendingTxPartitionEnlistment> enlistedGroups,
            HybridTimestamp commitTimestamp
    ) {
        var verificationFutures = new CompletableFuture[enlistedGroups.size()];
        int cnt = -1;

        for (Map.Entry<ZonePartitionId, PendingTxPartitionEnlistment> enlistedGroup : enlistedGroups.entrySet()) {
            ZonePartitionId groupId = enlistedGroup.getKey();
            long expectedEnlistmentConsistencyToken = enlistedGroup.getValue().consistencyToken();

            verificationFutures[++cnt] = placementDriver.getPrimaryReplica(groupId, commitTimestamp)
                    .thenAccept(currentPrimaryReplica -> {
                        if (currentPrimaryReplica == null
                                || expectedEnlistmentConsistencyToken != currentPrimaryReplica.getStartTime().longValue()
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

    @Override
    public List<SystemView<?>> systemViews() {
        LocksViewProvider lockViewProvider = new LocksViewProvider(lockManager::locks);

        return List.of(
                txViewProvider.get(),
                lockViewProvider.get()
        );
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

    private static long longProperty(SystemDistributedConfiguration systemProperties, String name, long defaultValue) {
        SystemPropertyView property = systemProperties.properties().value().get(name);

        return property == null ? defaultValue : Long.parseLong(property.propertyValue());
    }

    @TestOnly
    public void clearLocalRwTxCounter() {
        localRwTxCounter.clear();
    }
}
