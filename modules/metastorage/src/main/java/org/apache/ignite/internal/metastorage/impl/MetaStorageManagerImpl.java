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
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.cancelOrConsume;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.disaster.system.message.ResetClusterMessage;
import org.apache.ignite.internal.disaster.system.repair.MetastorageRepair;
import org.apache.ignite.internal.disaster.system.storage.MetastorageRepairStorage;
import org.apache.ignite.internal.future.OrderingFuture;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageCompactionManager;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.RevisionUpdateListener;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.metrics.MetaStorageMetricSource;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.OnRevisionAppliedCallback;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.metastorage.server.time.ClusterTime;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.IndexWithTerm;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.RaftNodeDisruptorConfiguration;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.service.CommittedConfiguration;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
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
public class MetaStorageManagerImpl implements MetaStorageManager, MetastorageGroupMaintenance, MetaStorageCompactionManager {
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

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean isStopped = new AtomicBoolean();

    /**
     * Future which completes when MetaStorage manager finished local recovery. The value of the future is the revision which must be used
     * for state recovery by other components.
     */
    private final CompletableFuture<Long> recoveryFinishedFuture = new CompletableFuture<>();

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

    private volatile long appliedRevision = 0;

    private volatile MetaStorageConfiguration metaStorageConfiguration;

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
            RaftGroupOptionsConfigurer raftGroupOptionsConfigurer
    ) {
        this.clusterService = clusterService;
        this.raftMgr = raftMgr;
        this.cmgMgr = cmgMgr;
        this.logicalTopologyService = logicalTopologyService;
        this.storage = storage;
        this.clusterTime = new ClusterTimeImpl(clusterService.nodeName(), busyLock, clock);
        this.metaStorageMetricSource = new MetaStorageMetricSource(clusterTime);
        this.topologyAwareRaftGroupServiceFactory = topologyAwareRaftGroupServiceFactory;
        this.metricManager = metricManager;
        this.metastorageRepairStorage = metastorageRepairStorage;
        this.metastorageRepair = metastorageRepair;
        this.raftGroupOptionsConfigurer = raftGroupOptionsConfigurer;

        learnerManager = new MetaStorageLearnerManager(busyLock, logicalTopologyService, metaStorageSvcFut);
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
            MetaStorageConfiguration configuration,
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
                () -> null,
                (nodes, mgReplicationFactor) -> nullCompletedFuture(),
                raftGroupOptionsConfigurer
        );

        configure(configuration);
    }

    /** Adds new listener to notify with election events. */
    public void addElectionListener(ElectionListener listener) {
        electionListeners.add(listener);
    }

    private CompletableFuture<Long> recover(MetaStorageService service) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            service.currentRevision().whenComplete((targetRevision, throwable) -> {
                if (throwable != null) {
                    recoveryFinishedFuture.completeExceptionally(throwable);

                    return;
                }

                LOG.info("Performing MetaStorage recovery from revision {} to {}", storage.revision(), targetRevision);

                assert targetRevision != null;

                listenForRecovery(targetRevision);
            });

            return recoveryFinishedFuture;
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void listenForRecovery(long targetRevision) {
        storage.setRecoveryRevisionListener(storageRevision -> {
            if (!busyLock.enterBusy()) {
                recoveryFinishedFuture.completeExceptionally(new NodeStoppingException());

                return;
            }

            try {
                if (storageRevision < targetRevision) {
                    return;
                }

                storage.setRecoveryRevisionListener(null);

                appliedRevision = targetRevision;
                if (recoveryFinishedFuture.complete(targetRevision)) {
                    LOG.info("Finished MetaStorage recovery");
                }
            } finally {
                busyLock.leaveBusy();
            }
        });

        if (!busyLock.enterBusy()) {
            recoveryFinishedFuture.completeExceptionally(new NodeStoppingException());

            return;
        }

        // Storage might be already up-to-date, so check here manually after setting the listener.
        try {
            long storageRevision = storage.revision();

            if (storageRevision >= targetRevision) {
                storage.setRecoveryRevisionListener(null);

                appliedRevision = targetRevision;
                if (recoveryFinishedFuture.complete(targetRevision)) {
                    LOG.info("Finished MetaStorage recovery");
                }
            }
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<MetaStorageServiceImpl> initializeMetaStorage(Set<String> metaStorageNodes) {
        try {
            String thisNodeName = clusterService.nodeName();

            var disruptorConfig = new RaftNodeDisruptorConfiguration("metastorage", 1);

            CompletableFuture<? extends RaftGroupService> localRaftServiceFuture = metaStorageNodes.contains(thisNodeName)
                    ? startFollowerNode(metaStorageNodes, disruptorConfig)
                    : startLearnerNode(metaStorageNodes, disruptorConfig);

            raftNodeStarted.complete(null);

            return localRaftServiceFuture
                    .thenApply(raftService -> {
                        raftServiceFuture.complete(raftService);

                        return new MetaStorageServiceImpl(
                                thisNodeName,
                                raftService,
                                busyLock,
                                clusterTime,
                                () -> clusterService.topologyService().localMember().id());
                    });
        } catch (NodeStoppingException e) {
            return failedFuture(e);
        }
    }

    private CompletableFuture<TopologyAwareRaftGroupService> startFollowerNode(
            Set<String> metaStorageNodes,
            RaftNodeDisruptorConfiguration disruptorConfig
    ) throws NodeStoppingException {
        PeersAndLearners configuration = PeersAndLearners.fromConsistentIds(metaStorageNodes);
        Peer localPeer = configuration.peer(clusterService.nodeName());
        assert localPeer != null;

        return startRaftNode(configuration, localPeer, disruptorConfig);
    }

    private CompletableFuture<TopologyAwareRaftGroupService> startLearnerNode(
            Set<String> metaStorageNodes,
            RaftNodeDisruptorConfiguration disruptorConfig
    ) throws NodeStoppingException {
        String thisNodeName = clusterService.nodeName();
        PeersAndLearners configuration = PeersAndLearners.fromConsistentIds(metaStorageNodes, Set.of(thisNodeName));
        Peer localPeer = configuration.learner(thisNodeName);
        assert localPeer != null;

        return startRaftNode(configuration, localPeer, disruptorConfig);
    }

    private CompletableFuture<TopologyAwareRaftGroupService> startRaftNode(
            PeersAndLearners configuration,
            Peer localPeer,
            RaftNodeDisruptorConfiguration disruptorConfig
    ) throws NodeStoppingException {
        MetaStorageConfiguration localMetaStorageConfiguration = metaStorageConfiguration;

        assert localMetaStorageConfiguration != null : "Meta Storage configuration has not been set";

        MetaStorageListener raftListener = new MetaStorageListener(storage, clusterTime, this::onConfigurationCommitted);

        CompletableFuture<TopologyAwareRaftGroupService> serviceFuture = raftMgr.startRaftGroupNodeAndWaitNodeReadyFuture(
                raftNodeId(localPeer),
                configuration,
                raftListener,
                RaftGroupEventsListener.noopLsnr,
                disruptorConfig,
                topologyAwareRaftGroupServiceFactory,
                raftGroupOptionsConfigurer
        );

        serviceFuture
                .thenAccept(service -> service.subscribeLeader(new MetaStorageLeaderElectionListener(
                        busyLock,
                        clusterService,
                        logicalTopologyService,
                        metaStorageSvcFut,
                        learnerManager,
                        clusterTime,
                        // We use the "deployWatchesFuture" to guarantee that the Configuration Manager will be started
                        // when the underlying code tries to read Meta Storage configuration. This is a consequence of having a circular
                        // dependency between these two components.
                        deployWatchesFuture.thenApply(v -> localMetaStorageConfiguration),
                        electionListeners,
                        this::peersChangeStateExists
                )))
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Unable to register MetaStorageLeaderElectionListener", e);
                    }
                });

        return serviceFuture;
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

    private void onConfigurationCommitted(CommittedConfiguration configuration) {
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
                        LOG.error("Error while handling ConfigurationCommitted event", ex);
                    }
                });
    }

    private void updateRaftClientConfigIfEventIsNotStale(CommittedConfiguration configuration, RaftGroupService raftService) {
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

    private void handlePeersChange(CommittedConfiguration configuration, RaftGroupService raftService) {
        synchronized (peersChangeMutex) {
            if (peersChangeState == null || configuration.term() <= peersChangeState.termBeforeChange) {
                return;
            }

            PeersChangeState currentState = peersChangeState;

            if (thisNodeIsEstablishedAsLonelyLeader(configuration)) {
                LOG.info("Lonely leader has been established, changing voting set to target set: {}", currentState.targetPeers);

                PeersAndLearners newConfig = PeersAndLearners.fromConsistentIds(currentState.targetPeers);
                raftService.changePeersAndLearners(newConfig, configuration.term())
                        .whenComplete((res, ex) -> {
                            if (ex != null) {
                                LOG.error("Error while changing voting set to {}", ex, currentState.targetPeers);
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
                                LOG.error("Error while updating learners as a reaction to commit of {}", ex, configuration);
                            }
                        });
            }
        }
    }

    private boolean thisNodeIsEstablishedAsLonelyLeader(CommittedConfiguration configuration) {
        return configuration.peers().size() == 1 && clusterService.nodeName().equals(configuration.peers().get(0));
    }

    private static boolean targetVotingSetIsEstablished(CommittedConfiguration configuration, PeersChangeState currentState) {
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
    public final void configure(MetaStorageConfiguration metaStorageConfiguration) {
        this.metaStorageConfiguration = metaStorageConfiguration;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        storage.start();

        cmgMgr.metaStorageNodes()
                .thenCompose(metaStorageNodes -> {
                    LOG.info("Metastorage nodes on start are {}", metaStorageNodes);

                    if (!busyLock.enterBusy()) {
                        return failedFuture(new NodeStoppingException());
                    }

                    try {
                        return initializeMetaStorage(metaStorageNodes);
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

        busyLock.block();

        deployWatchesFuture.cancel(true);

        recoveryFinishedFuture.cancel(true);

        try {
            IgniteUtils.closeAllManually(
                    () -> metricManager.unregisterSource(metaStorageMetricSource),
                    clusterTime,
                    () -> cancelOrConsume(metaStorageSvcFut, MetaStorageServiceImpl::close),
                    () -> raftMgr.stopRaftNodes(MetastorageGroupId.INSTANCE),
                    storage
            );
        } catch (Exception e) {
            return failedFuture(e);
        }

        return nullCompletedFuture();
    }

    @Override
    public long appliedRevision() {
        return appliedRevision;
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
                    .thenAccept(revision -> inBusyLock(busyLock, () -> {
                        storage.startWatches(revision + 1, new OnRevisionAppliedCallback() {
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
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.get(key));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Entry> get(ByteArray key, long revUpperBound) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.get(key, revUpperBound));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public List<Entry> getLocally(byte[] key, long revLowerBound, long revUpperBound) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            return storage.get(key, revLowerBound, revUpperBound);
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public Entry getLocally(ByteArray key, long revUpperBound) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            return storage.get(key.bytes(), revUpperBound);
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public Cursor<Entry> getLocally(ByteArray startKey, ByteArray endKey, long revUpperBound) {
        return storage.range(startKey.bytes(), endKey.bytes(), revUpperBound);
    }

    @Override
    public Cursor<Entry> prefixLocally(ByteArray keyPrefix, long revUpperBound) {
        byte[] rangeStart = keyPrefix.bytes();
        byte[] rangeEnd = storage.nextKey(rangeStart);

        return storage.range(rangeStart, rangeEnd, revUpperBound);
    }

    @Override
    public HybridTimestamp timestampByRevision(long revision) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            return storage.timestampByRevision(revision);
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.getAll(keys));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Retrieves entries for given keys and the revision upper bound.
     *
     * @see MetaStorageService#getAll(Set, long)
     */
    public CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys, long revUpperBound) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.getAll(keys, revUpperBound));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Inserts or updates an entry with the given key and the given value.
     *
     * @see MetaStorageService#put(ByteArray, byte[])
     */
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

    /**
     * Inserts or updates entries with given keys and given values.
     *
     * @see MetaStorageService#putAll(Map)
     */
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

    /**
     * Removes an entry for the given key.
     *
     * @see MetaStorageService#remove(ByteArray)
     */
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

    /**
     * Removes entries for given keys.
     *
     * @see MetaStorageService#removeAll(Set)
     */
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
    public CompletableFuture<Boolean> invoke(Condition cond, Collection<Operation> success, Collection<Operation> failure) {
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

    /**
     * Retrieves entries for the given key range in lexicographic order. Entries will be filtered out by upper bound of given revision
     * number.
     *
     * @see MetaStorageService#range(ByteArray, ByteArray, long)
     */
    public Publisher<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo, long revUpperBound) {
        if (!busyLock.enterBusy()) {
            return new NodeStoppingPublisher<>();
        }

        try {
            return new CompletableFuturePublisher<>(metaStorageSvcFut.thenApply(svc -> svc.range(keyFrom, keyTo, revUpperBound)));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public Publisher<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo) {
        return range(keyFrom, keyTo, false);
    }

    /**
     * Retrieves entries for the given key range in lexicographic order.
     *
     * @see MetaStorageService#range(ByteArray, ByteArray, boolean)
     */
    public Publisher<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo, boolean includeTombstones) {
        if (!busyLock.enterBusy()) {
            return new NodeStoppingPublisher<>();
        }

        try {
            return new CompletableFuturePublisher<>(metaStorageSvcFut.thenApply(svc -> svc.range(keyFrom, keyTo, includeTombstones)));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public Publisher<Entry> prefix(ByteArray keyPrefix) {
        return prefix(keyPrefix, MetaStorageManager.LATEST_REVISION);
    }

    @Override
    public Publisher<Entry> prefix(ByteArray keyPrefix, long revUpperBound) {
        if (!busyLock.enterBusy()) {
            return new NodeStoppingPublisher<>();
        }

        try {
            return new CompletableFuturePublisher<>(metaStorageSvcFut.thenApply(svc -> svc.prefix(keyPrefix, revUpperBound)));
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
    public CompletableFuture<Long> recoveryFinishedFuture() {
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

            assert indexWithTerm != null : "Attempt to get index and term when Raft node is not started yet or already stopped)";

            return indexWithTerm;
        }));
    }

    @Override
    public CompletableFuture<Void> becomeLonelyLeader(long termBeforeChange, Set<String> targetVotingSet) {
        return inBusyLockAsync(busyLock, () -> {
            synchronized (peersChangeMutex) {
                if (peersChangeState != null) {
                    return failedFuture(new IgniteInternalException(
                            INTERNAL_ERR,
                            "Peers change is under way [state=" + peersChangeState + "]."
                    ));
                }

                // If the target voting set matches the 'lonely leader' voting set, we don't need second step (that is, switching to
                // the target set), so we don't establish the peers change state.
                peersChangeState = targetVotingSet.size() > 1 ? new PeersChangeState(termBeforeChange, targetVotingSet) : null;

                RaftNodeId raftNodeId = raftNodeId();
                PeersAndLearners newConfiguration = PeersAndLearners.fromPeers(Set.of(raftNodeId.peer()), emptySet());

                ((Loza) raftMgr).resetPeers(raftNodeId, newConfiguration);

                return doWithOneOffRaftGroupService(newConfiguration, RaftGroupService::refreshLeader);
            }
        });
    }

    private <T> CompletableFuture<T> doWithOneOffRaftGroupService(
            PeersAndLearners raftClientConfiguration,
            Function<RaftGroupService, CompletableFuture<T>> action
    ) {
        return startOneOffRaftGroupService(raftClientConfiguration)
                .thenCompose(raftGroupService -> action.apply(raftGroupService)
                        .whenComplete((res, ex) -> raftGroupService.shutdown())
                );
    }

    private CompletableFuture<RaftGroupService> startOneOffRaftGroupService(PeersAndLearners newConfiguration) {
        try {
            return raftMgr.startRaftGroupService(MetastorageGroupId.INSTANCE, newConfiguration);
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
        storage.registerRevisionUpdateListener(listener);
    }

    @Override
    public void unregisterRevisionUpdateListener(RevisionUpdateListener listener) {
        storage.unregisterRevisionUpdateListener(listener);
    }

    /** Explicitly notifies revision update listeners. */
    public CompletableFuture<Void> notifyRevisionUpdateListenerOnStart() {
        return recoveryFinishedFuture.thenCompose(storage::notifyRevisionUpdateListenerOnStart);
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
    public void compactLocally(long revision) {
        inBusyLock(busyLock, () -> storage.compact(revision));
    }

    @Override
    public void saveCompactionRevisionLocally(long revision) {
        inBusyLock(busyLock, () -> storage.saveCompactionRevision(revision));
    }

    @Override
    public void setCompactionRevisionLocally(long revision) {
        inBusyLock(busyLock, () -> storage.setCompactionRevision(revision));
    }

    @Override
    public long getCompactionRevisionLocally() {
        return inBusyLock(busyLock, storage::getCompactionRevision);
    }
}
