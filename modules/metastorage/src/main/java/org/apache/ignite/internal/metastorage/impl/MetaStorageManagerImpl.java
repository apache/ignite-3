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

package org.apache.ignite.internal.metastorage.impl;

import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.function.Function.identity;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.IgniteUtils.failOrConsume;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.MetaStorageInfo;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.disaster.system.message.ResetClusterMessage;
import org.apache.ignite.internal.disaster.system.repair.MetastorageRepair;
import org.apache.ignite.internal.disaster.system.storage.MetastorageRepairStorage;
import org.apache.ignite.internal.disaster.system.storage.NoOpMetastorageRepairStorage;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.failure.handlers.NoOpFailureHandler;
import org.apache.ignite.internal.future.OrderingFuture;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.CompactionRevisionUpdateListener;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.RevisionUpdateListener;
import org.apache.ignite.internal.metastorage.Revisions;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.command.CompactionCommand;
import org.apache.ignite.internal.metastorage.command.response.RevisionsInfo;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.impl.raft.MetaStorageSnapshotStorageFactory;
import org.apache.ignite.internal.metastorage.metrics.MetaStorageMetricSource;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.NotificationEnqueuedListener;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker.TrackingToken;
import org.apache.ignite.internal.metastorage.server.WatchEventHandlingCallback;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.metastorage.server.time.ClusterTime;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.IndexWithTerm;
import org.apache.ignite.internal.raft.LeaderElectionListener;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.impl.RaftException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * MetaStorage manager.
 *
 * <p>Responsible for:
 * <ul>
 *     <li>Handling cluster init message.</li>
 *     <li>Managing Meta storage lifecycle including instantiation Meta storage raft group.</li>
 *     <li>Providing corresponding Meta storage service proxy interface</li>
 * </ul>
 */
public class MetaStorageManagerImpl implements MetaStorageManager, MetastorageGroupMaintenance {
    private static final IgniteLogger LOG = Loggers.forClass(MetaStorageManagerImpl.class);

    private final ClusterService clusterService;

    /** Raft manager that is used for metastorage raft group handling. */
    private final RaftManager raftMgr;

    private final ClusterManagementGroupManager cmgMgr;

    private final LogicalTopologyService logicalTopologyService;

    /** Meta storage service. */
    private final CompletableFuture<MetaStorageServiceImpl> metaStorageSvcFut = new CompletableFuture<>();

    /** Actual storage for Meta storage. */
    private final KeyValueStorage storage;

    private final HybridClock clock;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean isStopped = new AtomicBoolean();

    /**
     * Future which completes when MetaStorage manager finished local recovery. The value of the future is the revisions which must be used
     * for state recovery by other components.
     */
    private final CompletableFuture<Revisions> recoveryFinishedFuture = new CompletableFuture<>();

    /**
     * Future that gets completed after {@link #deployWatches} method has been called.
     */
    private final CompletableFuture<Void> deployWatchesFuture = new CompletableFuture<>();

    private final ClusterTimeImpl clusterTime;

    private final TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory;

    private final MetricManager metricManager;

    private final MetaStorageMetricSource metaStorageMetricSource;

    private final MetastorageRepairStorage metastorageRepairStorage;
    private final MetastorageRepair metastorageRepair;

    private final Executor ioExecutor;

    private final FailureProcessor failureProcessor;

    private volatile long appliedRevision = 0;

    private volatile SystemDistributedConfiguration systemConfiguration;

    private final List<ElectionListener> electionListeners = new CopyOnWriteArrayList<>();

    private final RaftGroupOptionsConfigurer raftGroupOptionsConfigurer;

    private final MetaStorageLearnerManager learnerManager;

    /** Gets completed when a Raft node (that is, the server Raft component of the group) is started for Metastorage. */
    private final CompletableFuture<Void> raftNodeStarted = new CompletableFuture<>();

    /** Gets completed when a Raft service (that is, the Raft client for talking with the group) is started for Metastorage. */
    private final OrderingFuture<RaftGroupService> raftServiceFuture = new OrderingFuture<>();

    /**
     * State of changing Raft group peers (aka voting set members). Currently only used for forceful members reset during repair.
     *
     * <p>Access to it is guarded by {@link #peersChangeMutex}.
     */
    private @Nullable PeersChangeState peersChangeState;

    /** Guards access to {@link #peersChangeState}. */
    private final Object peersChangeMutex = new Object();

    /**
     * Index and term of last Raft group config update applied to the Raft client.
     */
    private final AtomicReference<IndexWithTerm> lastHandledIndexWithTerm = new AtomicReference<>(new IndexWithTerm(0, 0));

    /** Tracks only reads from the leader, local reads are tracked by the storage itself. */
    private final ReadOperationForCompactionTracker readOperationFromLeaderForCompactionTracker;

    private final MetastorageDivergencyValidator divergencyValidator = new MetastorageDivergencyValidator();

    private final RecoveryRevisionsListenerImpl recoveryRevisionsListener;

    /**
     * The constructor.
     *
     * @param clusterService Cluster network service.
     * @param cmgMgr Cluster management service Manager.
     * @param logicalTopologyService Logical topology service.
     * @param raftMgr Raft manager.
     * @param storage Storage. This component owns this resource and will manage its lifecycle.
     * @param clock A hybrid logical clock.
     * @param metricManager Metric manager.
     * @param raftGroupOptionsConfigurer Configures MS RAFT options.
     * @param readOperationForCompactionTracker Read operation tracker for metastorage compaction.
     * @param ioExecutor Executor to which I/O operations can be offloaded from network threads.
     * @param failureProcessor Failure processor to use when reporting failures.
     */
    public MetaStorageManagerImpl(
            ClusterService clusterService,
            ClusterManagementGroupManager cmgMgr,
            LogicalTopologyService logicalTopologyService,
            RaftManager raftMgr,
            KeyValueStorage storage,
            HybridClock clock,
            TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory,
            MetricManager metricManager,
            MetastorageRepairStorage metastorageRepairStorage,
            MetastorageRepair metastorageRepair,
            RaftGroupOptionsConfigurer raftGroupOptionsConfigurer,
            ReadOperationForCompactionTracker readOperationForCompactionTracker,
            Executor ioExecutor,
            FailureProcessor failureProcessor
    ) {
        this.clusterService = clusterService;
        this.raftMgr = raftMgr;
        this.cmgMgr = cmgMgr;
        this.logicalTopologyService = logicalTopologyService;
        this.storage = storage;
        this.clock = clock;
        this.clusterTime = new ClusterTimeImpl(clusterService.nodeName(), busyLock, clock, failureProcessor);
        this.metaStorageMetricSource = new MetaStorageMetricSource(clusterTime);
        this.topologyAwareRaftGroupServiceFactory = topologyAwareRaftGroupServiceFactory;
        this.metricManager = metricManager;
        this.metastorageRepairStorage = metastorageRepairStorage;
        this.metastorageRepair = metastorageRepair;
        this.raftGroupOptionsConfigurer = raftGroupOptionsConfigurer;
        this.readOperationFromLeaderForCompactionTracker = readOperationForCompactionTracker;
        this.ioExecutor = ioExecutor;
        this.failureProcessor = failureProcessor;

        learnerManager = new MetaStorageLearnerManager(busyLock, logicalTopologyService, failureProcessor, metaStorageSvcFut);

        recoveryRevisionsListener = new RecoveryRevisionsListenerImpl(busyLock, recoveryFinishedFuture);
        storage.setRecoveryRevisionsListener(recoveryRevisionsListener);
    }

    /**
     * Constructor for tests, that allows to pass Meta Storage configuration.
     */
    @TestOnly
    public MetaStorageManagerImpl(
            ClusterService clusterService,
            ClusterManagementGroupManager cmgMgr,
            LogicalTopologyService logicalTopologyService,
            RaftManager raftMgr,
            KeyValueStorage storage,
            HybridClock clock,
            TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory,
            MetricManager metricManager,
            SystemDistributedConfiguration systemConfiguration,
            RaftGroupOptionsConfigurer raftGroupOptionsConfigurer
    ) {
        this(
                clusterService,
                cmgMgr,
                logicalTopologyService,
                raftMgr,
                storage,
                clock,
                topologyAwareRaftGroupServiceFactory,
                metricManager,
                systemConfiguration,
                raftGroupOptionsConfigurer,
                new ReadOperationForCompactionTracker()
        );
    }

    /**
     * Constructor for tests, that allows to pass Meta Storage configuration.
     */
    @TestOnly
    public MetaStorageManagerImpl(
            ClusterService clusterService,
            ClusterManagementGroupManager cmgMgr,
            LogicalTopologyService logicalTopologyService,
            RaftManager raftMgr,
            KeyValueStorage storage,
            HybridClock clock,
            TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory,
            MetricManager metricManager,
            SystemDistributedConfiguration systemConfiguration,
            RaftGroupOptionsConfigurer raftGroupOptionsConfigurer,
            ReadOperationForCompactionTracker tracker
    ) {
        this(
                clusterService,
                cmgMgr,
                logicalTopologyService,
                raftMgr,
                storage,
                clock,
                topologyAwareRaftGroupServiceFactory,
                metricManager,
                new NoOpMetastorageRepairStorage(),
                (nodes, mgReplicationFactor) -> nullCompletedFuture(),
                raftGroupOptionsConfigurer,
                tracker,
                ForkJoinPool.commonPool(),
                new FailureManager(new NoOpFailureHandler())
        );

        configure(systemConfiguration);
    }

    /** Adds new listener to notify with election events. */
    public void addElectionListener(ElectionListener listener) {
        electionListeners.add(listener);
    }

    /** Registers a notification enqueued listener. */
    public void registerNotificationEnqueuedListener(NotificationEnqueuedListener listener) {
        storage.registerNotificationEnqueuedListener(listener);
    }

    private CompletableFuture<?> recover(MetaStorageService service) {
        return inBusyLockAsync(busyLock, () -> {
            service.currentRevisions()
                    .thenAccept(targetRevisions -> {
                        assert targetRevisions != null;

                        LOG.info("Performing MetaStorage recovery: [from={}, to={}]", storage.revisions(), targetRevisions);

                        recoveryRevisionsListener.setTargetRevisions(targetRevisions.toRevisions());
                    }).whenComplete((res, throwable) -> {
                        if (throwable != null) {
                            recoveryFinishedFuture.completeExceptionally(throwable);
                        }
                    });

            return recoveryFinishedFuture
                    .thenAccept(revisions -> {
                        long recoveryRevision = revisions.revision();

                        appliedRevision = recoveryRevision;

                        if (recoveryRevision > 0) {
                            clusterTime.updateSafeTime(storage.timestampByRevision(recoveryRevision));
                        }
                    })
                    .whenComplete((revisions, throwable) -> {
                        storage.setRecoveryRevisionsListener(null);

                        if (throwable != null) {
                            LOG.info("Recovery failed", throwable);
                        } else {
                            LOG.info("Finished MetaStorage recovery");
                        }
                    });
        });
    }

    private CompletableFuture<MetaStorageServiceImpl> reenterIfNeededAndInitializeMetaStorage(
            MetaStorageInfo metaStorageInfo,
            UUID currentClusterId
    ) {
        return CompletableFuture.supplyAsync(() -> {
            // Doing it in I/O executor because we do blocking reads here, and we can originally be in a network thread.
            if (thisNodeDidNotWitnessMetaStorageRepair(metaStorageInfo, currentClusterId)) {
                return tryReenteringMetastorage(metaStorageInfo.metaStorageNodes(), currentClusterId)
                        .thenCompose(unused -> initializeMetastorage(metaStorageInfo));
            } else {
                return initializeMetastorage(metaStorageInfo);
            }
        }, ioExecutor).thenCompose(identity());
    }

    /**
     * Returns whether this node did not witness a Metastorage Repair in the current cluster incarnation. 'Did not witness' here means
     * that the node neither participated in the repair (that is, provided information about its Metastorage group index+term to the repair
     * conductor) nor it was migrated to the repaired cluster after the repair and passed through Metastorage vaidation (for divergence)
     * and re-entry procedure.
     *
     * @param metaStorageInfo Information about Metastorage.
     * @param currentClusterId Current cluster ID.
     */
    private boolean thisNodeDidNotWitnessMetaStorageRepair(MetaStorageInfo metaStorageInfo, UUID currentClusterId) {
        UUID locallyWitnessedRepairClusterId = metastorageRepairStorage.readWitnessedMetastorageRepairClusterId();

        return metaStorageInfo.metastorageRepairedInThisClusterIncarnation()
                && !Objects.equals(locallyWitnessedRepairClusterId, currentClusterId);
    }

    private CompletableFuture<Void> tryReenteringMetastorage(Set<String> metastorageNodes, UUID currentClusterId) {
        LOG.info("Trying to reenter Metastorage group");

        return validateMetastorageForDivergence(metastorageNodes)
                .thenRunAsync(() -> prepareMetaStorageReentry(currentClusterId), ioExecutor);
    }

    private CompletableFuture<Void> validateMetastorageForDivergence(Set<String> metastorageNodes) {
        long localRevision = storage.revision();

        if (localRevision == 0) {
            // No revisions, so local Metastorage could not diverge.
            return nullCompletedFuture();
        }

        long localChecksum = storage.checksum(localRevision);

        return doWithOneOffRaftGroupService(PeersAndLearners.fromConsistentIds(metastorageNodes), raftClient -> {
            return createMetaStorageService(raftClient).checksum(localRevision)
                    .thenAccept(leaderChecksumInfo -> {
                        LOG.info(
                                "Validating Metastorage for divergence [localRevision={}, localChecksum={}, leaderChecksumInfo={}",
                                localRevision, localChecksum, leaderChecksumInfo
                        );

                        divergencyValidator.validate(localRevision, localChecksum, leaderChecksumInfo);

                        LOG.info("Metastorage did not diverge, proceeding");
                    });
        });
    }

    private void prepareMetaStorageReentry(UUID currentClusterId) {
        LOG.info("Preparing storages for reentry [clusterId={}]", currentClusterId);

        try {
            destroyRaftAndStateMachineStorages();
        } catch (NodeStoppingException e) {
            throw new RuntimeException(e);
        }

        saveWitnessedMetastorageRepairClusterIdLocally(currentClusterId);
    }

    private void destroyRaftAndStateMachineStorages() throws NodeStoppingException {
        raftMgr.destroyRaftNodeStorages(raftNodeId(), raftGroupOptionsConfigurer);

        storage.clear();
    }

    private void saveWitnessedMetastorageRepairClusterIdLocally(UUID currentClusterId) {
        assert currentClusterId != null;

        metastorageRepairStorage.saveWitnessedMetastorageRepairClusterId(currentClusterId);
    }

    private CompletableFuture<MetaStorageServiceImpl> initializeMetastorage(MetaStorageInfo metaStorageInfo) {
        String thisNodeName = clusterService.nodeName();

        CompletableFuture<? extends RaftGroupService> localRaftServiceFuture;
        try {
            localRaftServiceFuture = metaStorageInfo.metaStorageNodes().contains(thisNodeName)
                    ? startVotingNode(metaStorageInfo)
                    : startLearnerNode(metaStorageInfo);
        } catch (NodeStoppingException e) {
            return failedFuture(e);
        }

        return localRaftServiceFuture
                .thenApply(raftService -> {
                    raftServiceFuture.complete(raftService);

                    return createMetaStorageService(raftService);
                });
    }

    private MetaStorageServiceImpl createMetaStorageService(RaftGroupService raftService) {
        return new MetaStorageServiceImpl(
                clusterService.nodeName(),
                raftService,
                busyLock,
                clock,
                clusterService.topologyService().localMember().id()
        );
    }

    private CompletableFuture<? extends RaftGroupService> startVotingNode(
            MetaStorageInfo metaStorageInfo
    ) throws NodeStoppingException {
        PeersAndLearners configuration = PeersAndLearners.fromConsistentIds(metaStorageInfo.metaStorageNodes());
        Peer localPeer = configuration.peer(clusterService.nodeName());
        assert localPeer != null;

        return startRaftNode(configuration, localPeer, metaStorageInfo);
    }

    private CompletableFuture<? extends RaftGroupService> startLearnerNode(
            MetaStorageInfo metaStorageInfo
    ) throws NodeStoppingException {
        String thisNodeName = clusterService.nodeName();
        PeersAndLearners configuration = PeersAndLearners.fromConsistentIds(metaStorageInfo.metaStorageNodes(), Set.of(thisNodeName));
        Peer localPeer = configuration.learner(thisNodeName);
        assert localPeer != null;

        return startRaftNode(configuration, localPeer, metaStorageInfo);
    }

    private CompletableFuture<? extends RaftGroupService> startRaftNode(
            PeersAndLearners configuration,
            Peer localPeer,
            MetaStorageInfo metaStorageInfo
    ) {
        SystemDistributedConfiguration currentSystemConfiguration = systemConfiguration;
        assert currentSystemConfiguration != null : "System configuration has not been set";

        CompletableFuture<TopologyAwareRaftGroupService> serviceFuture = CompletableFuture.supplyAsync(() -> {
            TopologyAwareRaftGroupService service = startRaftNodeItself(configuration, localPeer, metaStorageInfo);

            raftNodeStarted.complete(null);

            return service;
        }, ioExecutor);

        return serviceFuture.thenApply(service -> {
            service.subscribeLeader(createLeaderElectionListener(currentSystemConfiguration));
            return service;
        });
    }

    private TopologyAwareRaftGroupService startRaftNodeItself(
            PeersAndLearners configuration,
            Peer localPeer,
            MetaStorageInfo metaStorageInfo
    ) {
        MetaStorageListener raftListener = new MetaStorageListener(
                storage,
                clock,
                clusterTime,
                this::onConfigurationCommitted,
                metaStorageMetricSource::onIdempotentCacheSizeChange
        );

        try {
            return raftMgr.startSystemRaftGroupNodeAndWaitNodeReady(
                    raftNodeId(localPeer),
                    configuration,
                    raftListener,
                    RaftGroupEventsListener.noopLsnr,
                    topologyAwareRaftGroupServiceFactory,
                    options -> {
                        raftGroupOptionsConfigurer.configure(options);

                        RaftGroupOptions groupOptions = (RaftGroupOptions) options;
                        groupOptions.externallyEnforcedConfigIndex(metaStorageInfo.metastorageRepairingConfigIndex());
                        groupOptions.snapshotStorageFactory(new MetaStorageSnapshotStorageFactory(storage));
                    }
            );
        } catch (NodeStoppingException e) {
            throw ExceptionUtils.sneakyThrow(e);
        }
    }

    private LeaderElectionListener createLeaderElectionListener(SystemDistributedConfiguration configuration) {
        // We use the "deployWatchesFuture" to guarantee that the Configuration Manager will be started
        // when the underlying code tries to read Meta Storage configuration. This is a consequence of having a circular
        // dependency between these two components.
        return new MetaStorageLeaderElectionListener(
                busyLock,
                clusterService,
                logicalTopologyService,
                failureProcessor,
                metaStorageSvcFut,
                learnerManager,
                clusterTime,
                // We use the "deployWatchesFuture" to guarantee that the Configuration Manager will be started
                // when the underlying code tries to read Meta Storage configuration. This is a consequence of having a circular
                // dependency between these two components.
                deployWatchesFuture.thenApply(v -> configuration),
                electionListeners,
                this::peersChangeStateExists
        );
    }

    private boolean peersChangeStateExists() {
        synchronized (peersChangeMutex) {
            return peersChangeState != null;
        }
    }

    private RaftNodeId raftNodeId() {
        return raftNodeId(new Peer(clusterService.nodeName()));
    }

    private static RaftNodeId raftNodeId(Peer localPeer) {
        return new RaftNodeId(MetastorageGroupId.INSTANCE, localPeer);
    }

    private void onConfigurationCommitted(RaftGroupConfiguration configuration) {
        LOG.info("MS configuration committed {}", configuration);

        // TODO: IGNITE-23210 - use thenAccept() when implemented.
        raftServiceFuture
                .handle((raftService, ex) -> {
                    if (ex != null) {
                        throw ExceptionUtils.sneakyThrow(ex);
                    }

                    updateRaftClientConfigIfEventIsNotStale(configuration, raftService);

                    handlePeersChange(configuration, raftService);

                    return null;
                })
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        failureProcessor.process(new FailureContext(ex, "Error while handling ConfigurationCommitted event"));
                    }
                });
    }

    private void updateRaftClientConfigIfEventIsNotStale(RaftGroupConfiguration configuration, RaftGroupService raftService) {
        IndexWithTerm newIndexWithTerm = new IndexWithTerm(configuration.index(), configuration.term());

        lastHandledIndexWithTerm.updateAndGet(existingIndexWithTerm -> {
            if (newIndexWithTerm.compareTo(existingIndexWithTerm) > 0) {
                LOG.info("Updating raftService config to {}", configuration);

                raftService.updateConfiguration(PeersAndLearners.fromConsistentIds(
                        Set.copyOf(configuration.peers()),
                        Set.copyOf(configuration.learners())
                ));

                return newIndexWithTerm;
            } else {
                LOG.info("Skipping update for stale config {}, actual is {}", newIndexWithTerm, existingIndexWithTerm);

                return existingIndexWithTerm;
            }
        });
    }

    private void handlePeersChange(RaftGroupConfiguration configuration, RaftGroupService raftService) {
        synchronized (peersChangeMutex) {
            if (peersChangeState == null || configuration.term() <= peersChangeState.termBeforeChange) {
                return;
            }

            PeersChangeState currentState = peersChangeState;

            if (thisNodeIsEstablishedAsLonelyLeader(configuration)) {
                LOG.info("Lonely leader has been established, changing voting set to target set: {}", currentState.targetPeers);

                PeersAndLearners newConfig = PeersAndLearners.fromConsistentIds(currentState.targetPeers);
                // TODO: https://issues.apache.org/jira/browse/IGNITE-26854.
                raftService.changePeersAndLearners(newConfig, configuration.term(), 0)
                        .whenComplete((res, ex) -> {
                            if (ex != null) {
                                Throwable unwrapped = ExceptionUtils.unwrapCause(ex);

                                if (unwrapped instanceof RaftException && ((RaftException) unwrapped).raftError() == RaftError.ECATCHUP) {
                                    // Some node has left, it's not a reason to fail our node; just log the error.
                                    LOG.error("Error while changing voting set to {}", ex, currentState.targetPeers);
                                } else if (!hasCause(ex, NodeStoppingException.class)) {
                                    String errorMessage = IgniteStringFormatter.format(
                                            "Error while changing voting set to {}",
                                            currentState.targetPeers
                                    );
                                    failureProcessor.process(new FailureContext(ex, errorMessage));
                                }
                            } else {
                                LOG.info("Changed voting set successfully to {}", currentState.targetPeers);
                            }
                        });
            } else if (targetVotingSetIsEstablished(configuration, currentState)) {
                LOG.info("Target voting set has been established, unpausing secondary duties");

                peersChangeState = null;

                // Update learners to account for updates we could miss while the secondary duties were paused.
                learnerManager.updateLearners(configuration.term())
                        .whenComplete((res, ex) -> {
                            if (ex != null) {
                                String errorMessage = String.format(
                                        "Error while updating learners as a reaction to commit of %s",
                                        configuration
                                );
                                failureProcessor.process(new FailureContext(ex, errorMessage));
                            }
                        });
            }
        }
    }

    private boolean thisNodeIsEstablishedAsLonelyLeader(RaftGroupConfiguration configuration) {
        return configuration.peers().size() == 1 && clusterService.nodeName().equals(configuration.peers().get(0));
    }

    private static boolean targetVotingSetIsEstablished(RaftGroupConfiguration configuration, PeersChangeState currentState) {
        return Set.copyOf(configuration.peers()).equals(currentState.targetPeers);
    }

    /**
     * Sets the Meta Storage configuration.
     *
     * <p>This method is needed to avoid the cyclic dependency between the Meta Storage and distributed configuration (built on top of the
     * Meta Storage).
     *
     * <p>This method <b>must</b> always be called <b>before</b> calling {@link #startAsync}.
     */
    public final void configure(SystemDistributedConfiguration configuration) {
        this.systemConfiguration = configuration;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        storage.start();

        // Safe because we haven't started raft nodes yet and so no one has to update storage locally.
        recoveryRevisionsListener.onUpdate(storage.revisions());

        cmgMgr.metaStorageInfo()
                .thenCombine(cmgMgr.clusterState(), MetaStorageInfoAndClusterState::new)
                .thenCompose(infoAndState -> {
                    LOG.info("Metastorage info on start is {}", infoAndState.metaStorageInfo);

                    if (!busyLock.enterBusy()) {
                        return failedFuture(new NodeStoppingException());
                    }

                    try {
                        return reenterIfNeededAndInitializeMetaStorage(
                                infoAndState.metaStorageInfo,
                                infoAndState.clusterState.clusterTag().clusterId()
                        );
                    } finally {
                        busyLock.leaveBusy();
                    }
                })
                .thenCompose(service -> repairMetastorageIfNeeded().thenApply(unused -> service))
                .thenCompose(service -> recover(service).thenApply(rev -> service))
                .whenComplete((service, e) -> {
                    if (e != null) {
                        metaStorageSvcFut.completeExceptionally(e);
                        recoveryFinishedFuture.completeExceptionally(e);
                    } else {
                        assert service != null;

                        metaStorageSvcFut.complete(service);
                    }
                });

        metricManager.registerSource(metaStorageMetricSource);
        metricManager.enable(metaStorageMetricSource);

        return nullCompletedFuture();
    }

    private CompletableFuture<Void> repairMetastorageIfNeeded() {
        ResetClusterMessage resetClusterMessage = metastorageRepairStorage.readVolatileResetClusterMessage();
        if (resetClusterMessage == null) {
            return nullCompletedFuture();
        }
        if (!resetClusterMessage.metastorageRepairRequested()) {
            return nullCompletedFuture();
        }
        if (!clusterService.nodeName().equals(resetClusterMessage.conductor())) {
            return nullCompletedFuture();
        }

        return metastorageRepair.repair(
                requireNonNull(resetClusterMessage.participatingNodes()),
                requireNonNull(resetClusterMessage.metastorageReplicationFactor())
        );
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!isStopped.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        storage.stopCompaction();

        busyLock.block();

        deployWatchesFuture.completeExceptionally(new NodeStoppingException());

        recoveryFinishedFuture.completeExceptionally(new NodeStoppingException());

        try {
            IgniteUtils.closeAllManually(
                    () -> metricManager.unregisterSource(metaStorageMetricSource),
                    clusterTime,
                    () -> failOrConsume(metaStorageSvcFut, new NodeStoppingException(), MetaStorageServiceImpl::close),
                    () -> raftMgr.stopRaftNodes(MetastorageGroupId.INSTANCE),
                    storage
            );
        } catch (Exception e) {
            return failedFuture(e);
        }

        return nullCompletedFuture();
    }

    private static void cleanupMetaStorageServiceFuture(CompletableFuture<MetaStorageServiceImpl> future) {
        future.completeExceptionally(new NodeStoppingException());

        if (future.isCancelled() || future.isCompletedExceptionally()) {
            return;
        }

        assert future.isDone();

        MetaStorageServiceImpl res = future.join();

        assert res != null;

        res.close();
    }

    @Override
    public long appliedRevision() {
        return appliedRevision;
    }

    @Override
    public CompletableFuture<Long> currentRevision() {
        return metaStorageSvcFut.thenCompose(MetaStorageService::currentRevisions)
                .thenApply(RevisionsInfo::revision);
    }

    @Override
    public void registerPrefixWatch(ByteArray key, WatchListener listener) {
        storage.watchRange(key.bytes(), storage.nextKey(key.bytes()), appliedRevision() + 1, listener);
    }

    @Override
    public void registerExactWatch(ByteArray key, WatchListener listener) {
        storage.watchExact(key.bytes(), appliedRevision() + 1, listener);
    }

    @Override
    public void registerRangeWatch(ByteArray keyFrom, @Nullable ByteArray keyTo, WatchListener listener) {
        storage.watchRange(keyFrom.bytes(), keyTo == null ? null : keyTo.bytes(), appliedRevision() + 1, listener);
    }

    @Override
    public void unregisterWatch(WatchListener lsnr) {
        storage.removeWatch(lsnr);
    }

    @Override
    public CompletableFuture<Void> deployWatches() {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return recoveryFinishedFuture
                    .thenAccept(revisions -> inBusyLock(busyLock, () -> {
                        storage.startWatches(revisions.revision() + 1, new WatchEventHandlingCallback() {
                            @Override
                            public void onSafeTimeAdvanced(HybridTimestamp newSafeTime) {
                                MetaStorageManagerImpl.this.onSafeTimeAdvanced(newSafeTime);
                            }

                            @Override
                            public void onRevisionApplied(long revision) {
                                MetaStorageManagerImpl.this.onRevisionApplied(revision);
                            }
                        });
                    }))
                    .whenComplete((v, e) -> {
                        if (e == null) {
                            deployWatchesFuture.complete(null);
                        } else {
                            deployWatchesFuture.completeExceptionally(e);
                        }
                    });
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Entry> get(ByteArray key) {
        return inBusyLockAsync(
                busyLock,
                () -> withTrackReadOperationFromLeaderFuture(
                        storage.revision(),
                        () -> metaStorageSvcFut.thenCompose(svc -> svc.get(key))
                )
        );
    }

    @Override
    public CompletableFuture<Entry> get(ByteArray key, long revUpperBound) {
        return inBusyLockAsync(
                busyLock,
                () -> withTrackReadOperationFromLeaderFuture(
                        revUpperBound,
                        () -> metaStorageSvcFut.thenCompose(svc -> svc.get(key, revUpperBound))
                )
        );
    }

    @Override
    public Entry getLocally(ByteArray key) {
        return inBusyLock(busyLock, () -> storage.get(key.bytes()));
    }

    @Override
    public Entry getLocally(ByteArray key, long revUpperBound) {
        return inBusyLock(busyLock, () -> storage.get(key.bytes(), revUpperBound));
    }

    @Override
    public Cursor<Entry> getLocally(ByteArray startKey, ByteArray endKey, long revUpperBound) {
        return inBusyLock(busyLock, () -> storage.range(startKey.bytes(), endKey == null ? null : endKey.bytes(), revUpperBound));
    }

    @Override
    public List<Entry> getAllLocally(List<ByteArray> keys) {
        var k = new ArrayList<byte[]>(keys.size());

        for (int i = 0; i < keys.size(); i++) {
            k.add(keys.get(i).bytes());
        }

        return inBusyLock(busyLock, () -> storage.getAll(k));
    }

    @Override
    public Cursor<Entry> prefixLocally(ByteArray keyPrefix, long revUpperBound) {
        return inBusyLock(busyLock, () -> {
            byte[] rangeStart = keyPrefix.bytes();
            byte[] rangeEnd = storage.nextKey(rangeStart);

            return storage.range(rangeStart, rangeEnd, revUpperBound);
        });
    }

    @Override
    public HybridTimestamp timestampByRevisionLocally(long revision) {
        return inBusyLock(busyLock, () -> storage.timestampByRevision(revision));
    }

    @Override
    public CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys) {
        return inBusyLock(
                busyLock,
                () -> withTrackReadOperationFromLeaderFuture(
                        storage.revision(),
                        () -> metaStorageSvcFut.thenCompose(svc -> svc.getAll(keys))
                )
        );
    }

    @Override
    public CompletableFuture<Void> put(ByteArray key, byte[] val) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.put(key, val));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Void> putAll(Map<ByteArray, byte[]> vals) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.putAll(vals));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Void> remove(ByteArray key) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.remove(key));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Void> removeAll(Set<ByteArray> keys) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.removeAll(keys));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Void> removeByPrefix(ByteArray prefix) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.removeByPrefix(prefix));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Boolean> invoke(Condition cond, Operation success, Operation failure) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.invoke(cond, success, failure));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Boolean> invoke(Condition cond, List<Operation> success, List<Operation> failure) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.invoke(cond, success, failure));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<StatementResult> invoke(Iif iif) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.invoke(iif));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public Publisher<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo) {
        if (!busyLock.enterBusy()) {
            return new NodeStoppingPublisher<>();
        }

        try {
            return withTrackReadOperationFromLeaderPublisher(
                    storage.revision(),
                    () -> new CompletableFuturePublisher<>(metaStorageSvcFut.thenApply(svc -> svc.range(keyFrom, keyTo, false)))
            );
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public Publisher<Entry> prefix(ByteArray keyPrefix) {
        return prefix(keyPrefix, LATEST_REVISION);
    }

    @Override
    public Publisher<Entry> prefix(ByteArray keyPrefix, long revUpperBound) {
        if (!busyLock.enterBusy()) {
            return new NodeStoppingPublisher<>();
        }

        try {
            return withTrackReadOperationFromLeaderPublisher(
                    revUpperBound,
                    () -> new CompletableFuturePublisher<>(metaStorageSvcFut.thenApply(svc -> svc.prefix(keyPrefix, revUpperBound)))
            );
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void onSafeTimeAdvanced(HybridTimestamp time) {
        assert time != null;

        if (!busyLock.enterBusy()) {
            LOG.info("Skipping advancing Safe Time because the node is stopping");

            return;
        }

        try {
            clusterTime.updateSafeTime(time);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Saves processed Meta Storage revision to the {@link #appliedRevision}.
     */
    private void onRevisionApplied(long revision) {
        appliedRevision = revision;
    }

    @Override
    public ClusterTime clusterTime() {
        return clusterTime;
    }

    @Override
    public CompletableFuture<Revisions> recoveryFinishedFuture() {
        return recoveryFinishedFuture;
    }

    @Override
    public CompletableFuture<IndexWithTerm> raftNodeIndex() {
        return raftNodeStarted.thenApply(unused -> inBusyLock(busyLock, () -> {
            RaftNodeId nodeId = raftNodeId();

            IndexWithTerm indexWithTerm;
            try {
                indexWithTerm = raftMgr.raftNodeIndex(nodeId);
            } catch (NodeStoppingException e) {
                throw new CompletionException(e);
            }

            assert indexWithTerm != null : "Attempt to get index and term when Raft node is not started yet or already stopped): " + nodeId;

            return indexWithTerm;
        }));
    }

    @Override
    public void initiateForcefulVotersChange(long termBeforeChange, Set<String> targetVotingSet) {
        inBusyLock(busyLock, () -> {
            synchronized (peersChangeMutex) {
                if (peersChangeState != null) {
                    throw new IgniteInternalException(
                            INTERNAL_ERR,
                            "Peers change is under way [state=" + peersChangeState + "]."
                    );
                }

                // If the target voting set matches the 'lonely leader' voting set, we don't need second step (that is, switching to
                // the target set), so we don't establish the peers change state.
                peersChangeState = targetVotingSet.size() > 1 ? new PeersChangeState(termBeforeChange, targetVotingSet) : null;

                RaftNodeId raftNodeId = raftNodeId();
                PeersAndLearners newConfiguration = PeersAndLearners.fromPeers(Set.of(raftNodeId.peer()), emptySet());

                // TODO: https://issues.apache.org/jira/browse/IGNITE-26854.
                ((Loza) raftMgr).resetPeers(raftNodeId, newConfiguration, 0);
            }
        });
    }

    private <T> CompletableFuture<T> doWithOneOffRaftGroupService(
            PeersAndLearners raftClientConfiguration,
            Function<RaftGroupService, CompletableFuture<T>> action
    ) {
        try {
            RaftGroupService raftGroupService = raftMgr.startRaftGroupService(MetastorageGroupId.INSTANCE, raftClientConfiguration, true);

            return action.apply(raftGroupService)
                    // This callback should be executed asynchronously due to
                    // its code might be done under a busyLock of the raftGroupService,
                    // and so, it results in a deadlock on shutting down the service.
                    // TODO: https://issues.apache.org/jira/browse/IGNITE-25787
                    .whenCompleteAsync((res, ex) -> {
                        if (ex != null) {
                            LOG.error("One-off raft group action on {} failed", ex, raftClientConfiguration);
                        }

                        raftGroupService.shutdown();
                    }, ioExecutor);
        } catch (NodeStoppingException e) {
            return failedFuture(e);
        }
    }

    @TestOnly
    public CompletableFuture<MetaStorageServiceImpl> metaStorageService() {
        return metaStorageSvcFut;
    }

    private static class CompletableFuturePublisher<T> implements Publisher<T> {
        private final CompletableFuture<Publisher<T>> future;

        CompletableFuturePublisher(CompletableFuture<Publisher<T>> future) {
            this.future = future;
        }

        @Override
        public void subscribe(Subscriber<? super T> subscriber) {
            future.whenComplete((publisher, e) -> {
                if (e != null) {
                    subscriber.onError(e);
                } else {
                    publisher.subscribe(subscriber);
                }
            });
        }
    }

    private static class NodeStoppingPublisher<T> implements Publisher<T> {
        @Override
        public void subscribe(Subscriber<? super T> subscriber) {
            subscriber.onError(new NodeStoppingException());
        }
    }

    @Override
    public void registerRevisionUpdateListener(RevisionUpdateListener listener) {
        inBusyLock(busyLock, () -> storage.registerRevisionUpdateListener(listener));
    }

    @Override
    public void unregisterRevisionUpdateListener(RevisionUpdateListener listener) {
        inBusyLock(busyLock, () -> storage.unregisterRevisionUpdateListener(listener));
    }

    @Override
    public void registerCompactionRevisionUpdateListener(CompactionRevisionUpdateListener listener) {
        inBusyLock(busyLock, () -> storage.registerCompactionRevisionUpdateListener(listener));
    }

    @Override
    public void unregisterCompactionRevisionUpdateListener(CompactionRevisionUpdateListener listener) {
        inBusyLock(busyLock, () -> storage.unregisterCompactionRevisionUpdateListener(listener));
    }

    /** Explicitly notifies revisions update listeners. */
    public CompletableFuture<Void> notifyRevisionUpdateListenerOnStart() {
        return recoveryFinishedFuture.thenApply(Revisions::revision).thenCompose(storage::notifyRevisionUpdateListenerOnStart);
    }

    /**
     * Removes obsolete entries from both volatile and persistent idempotent command cache older than evictionTimestamp.
     *
     * @param evictionTimestamp Cached entries older than given timestamp will be evicted.
     * @return Pending operation future.
     */
    public CompletableFuture<Void> evictIdempotentCommandsCache(HybridTimestamp evictionTimestamp) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.evictIdempotentCommandsCache(evictionTimestamp));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Disables addition of learners one by one (as a reaction to nodes joining the validated nodes set).
     *
     * <p>This does NOT affect other ways of changing the learners.
     */
    @TestOnly
    public void disableLearnersAddition() {
        learnerManager.disableLearnersAddition();
    }

    private static class PeersChangeState {
        private final long termBeforeChange;
        private final Set<String> targetPeers;

        private PeersChangeState(long termBeforeChange, Set<String> targetPeers) {
            this.termBeforeChange = termBeforeChange;
            this.targetPeers = Set.copyOf(targetPeers);
        }

        @Override
        public String toString() {
            return S.toString(this);
        }
    }

    @Override
    public long getCompactionRevisionLocally() {
        return inBusyLock(busyLock, storage::getCompactionRevision);
    }

    @TestOnly
    public KeyValueStorage storage() {
        return storage;
    }

    private <T> CompletableFuture<T> withTrackReadOperationFromLeaderFuture(
            long operationRevision,
            Supplier<CompletableFuture<T>> readFromLeader
    ) {
        TrackingToken token = readOperationFromLeaderForCompactionTracker.track(
                operationRevision,
                storage::revision,
                storage::getCompactionRevision
        );

        try {
            return readFromLeader.get().whenComplete((t, throwable) -> token.close());
        } catch (Throwable t) {
            token.close();

            throw t;
        }
    }

    private Publisher<Entry> withTrackReadOperationFromLeaderPublisher(long operationRevision, Supplier<Publisher<Entry>> readFromLeader) {
        TrackingToken token = readOperationFromLeaderForCompactionTracker.track(
                operationRevision,
                storage::revision,
                storage::getCompactionRevision
        );

        try {
            Publisher<Entry> publisherFromLeader = readFromLeader.get();

            return subscriber -> publisherFromLeader.subscribe(new Subscriber<>() {
                @Override
                public void onSubscribe(Subscription subscription) {
                    subscriber.onSubscribe(new Subscription() {
                        @Override
                        public void request(long n) {
                            subscription.request(n);
                        }

                        @Override
                        public void cancel() {
                            token.close();

                            subscription.cancel();
                        }
                    });
                }

                @Override
                public void onNext(Entry item) {
                    subscriber.onNext(item);
                }

                @Override
                public void onError(Throwable throwable) {
                    token.close();

                    subscriber.onError(throwable);
                }

                @Override
                public void onComplete() {
                    token.close();

                    subscriber.onComplete();
                }
            });
        } catch (Throwable t) {
            token.close();

            throw t;
        }
    }

    private static class MetaStorageInfoAndClusterState {
        private final MetaStorageInfo metaStorageInfo;
        private final ClusterState clusterState;

        private MetaStorageInfoAndClusterState(MetaStorageInfo metaStorageInfo, ClusterState clusterState) {
            this.metaStorageInfo = metaStorageInfo;
            this.clusterState = clusterState;
        }
    }

    /**
     * Sends command {@link CompactionCommand} to the leader.
     *
     * @param compactionRevision New metastorage compaction revision.
     * @return Pending operation future.
     */
    CompletableFuture<Void> sendCompactionCommand(long compactionRevision) {
        return inBusyLockAsync(busyLock, () -> metaStorageSvcFut.thenCompose(svc -> svc.sendCompactionCommand(compactionRevision)));
    }
}
