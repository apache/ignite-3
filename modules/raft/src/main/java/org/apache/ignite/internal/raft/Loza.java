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

package org.apache.ignite.internal.raft;

import static java.util.Objects.requireNonNullElse;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.sources.RaftMetricSource;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.configuration.RaftView;
import org.apache.ignite.internal.raft.configuration.VolatileRaftConfiguration;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.GroupStoragesContextResolver;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.storage.GroupStoragesDestructionIntents;
import org.apache.ignite.internal.raft.storage.impl.NoopGroupStoragesDestructionIntents;
import org.apache.ignite.internal.raft.storage.impl.StorageDestructionIntent;
import org.apache.ignite.internal.raft.storage.impl.StoragesDestructionContext;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.ActionRequestInterceptor;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.apache.ignite.raft.jraft.rpc.impl.core.AppendEntriesRequestInterceptor;
import org.apache.ignite.raft.jraft.util.Utils;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Best raft manager ever since 1982.
 */
// TODO: Encapsulate RaftGroupOptions and move other methods to the RaftManager interface,
//  see https://issues.apache.org/jira/browse/IGNITE-18273
public class Loza implements RaftManager {
    /** Factory. */
    public static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    /** Raft client pool name. */
    public static final String CLIENT_POOL_NAME = "Raft-Group-Client";

    /** Raft client pool size. Size was taken from jraft's TimeManager. */
    private static final int CLIENT_POOL_SIZE = Math.min(Utils.cpus() * 3, 20);

    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(Loza.class);

    /** Cluster network service. */
    private final ClusterService clusterNetSvc;

    /** Raft server. */
    private final JraftServerImpl raftServer;

    /** Executor for raft group services. */
    private final ScheduledExecutorService executor;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Raft configuration. */
    private final RaftConfiguration raftConfiguration;

    private final NodeOptions opts;

    private final MetricManager metricManager;

    private final ThrottlingContextHolder partitionThrottlingContextHolder;

    private final ThrottlingContextHolder systemGroupsThrottlingContextHolder;

    /** Constructor using no-op group storages destruction intents. */
    @TestOnly
    public Loza(
            ClusterService clusterService,
            MetricManager metricManager,
            RaftConfiguration raftConfiguration,
            SystemLocalConfiguration systemLocalConfiguration,
            HybridClock hybridClock,
            RaftGroupEventsClientListener raftGroupEventsClientListener,
            FailureManager failureManager
    ) {
        this(
                clusterService,
                metricManager,
                raftConfiguration,
                systemLocalConfiguration,
                hybridClock,
                raftGroupEventsClientListener,
                failureManager,
                new NoopGroupStoragesDestructionIntents(),
                new GroupStoragesContextResolver(Objects::toString, Map.of(), Map.of())
        );
    }

    /**
     * The constructor.
     *
     * @param clusterNetSvc Cluster network service.
     * @param metricManager Metric manager.
     * @param raftConfiguration Raft configuration.
     * @param systemLocalConfiguration Local system configuration.
     * @param clock A hybrid logical clock.
     * @param failureManager Failure processor that is used to handle critical errors.
     * @param groupStoragesDestructionIntents Storage to persist {@link StorageDestructionIntent}s.
     * @param groupStoragesContextResolver Resolver to get {@link StoragesDestructionContext}s for storage destruction.
     */
    public Loza(
            ClusterService clusterNetSvc,
            MetricManager metricManager,
            RaftConfiguration raftConfiguration,
            SystemLocalConfiguration systemLocalConfiguration,
            HybridClock clock,
            RaftGroupEventsClientListener raftGroupEventsClientListener,
            FailureManager failureManager,
            GroupStoragesDestructionIntents groupStoragesDestructionIntents,
            GroupStoragesContextResolver groupStoragesContextResolver
    ) {
        this.clusterNetSvc = clusterNetSvc;
        this.raftConfiguration = raftConfiguration;
        this.metricManager = metricManager;

        // TODO: IGNITE-27081 Вот тут надо будет читать системную пропертю
        NodeOptions options = new NodeOptions();

        options.setClock(clock);
        options.setCommandsMarshaller(new ThreadLocalOptimizedMarshaller(clusterNetSvc.serializationRegistry()));

        this.opts = options;

        double maxInflightOverflowRate = raftConfiguration.maxInflightOverflowRate().value();

        partitionThrottlingContextHolder = new ThrottlingContextHolderImpl(raftConfiguration, maxInflightOverflowRate);

        // Throttler for system groups doesn't limit requests, but may adapt the request timeout if needed.
        systemGroupsThrottlingContextHolder = new ThrottlingContextHolderImpl(raftConfiguration, Integer.MAX_VALUE);

        this.raftServer = new JraftServerImpl(
                clusterNetSvc,
                options,
                raftGroupEventsClientListener,
                failureManager,
                groupStoragesDestructionIntents,
                groupStoragesContextResolver
        );

        this.executor = new ScheduledThreadPoolExecutor(
                CLIENT_POOL_SIZE,
                IgniteThreadFactory.create(clusterNetSvc.nodeName(), CLIENT_POOL_NAME, LOG)
        );
    }

    /**
     * Sets {@link AppendEntriesRequestInterceptor} to use. Should only be called from the same thread that is used
     * to {@link #startAsync(ComponentContext)} the component.
     *
     * @param appendEntriesRequestInterceptor Interceptor to use.
     */
    public void appendEntriesRequestInterceptor(AppendEntriesRequestInterceptor appendEntriesRequestInterceptor) {
        raftServer.appendEntriesRequestInterceptor(appendEntriesRequestInterceptor);
    }

    /**
     * Sets {@link ActionRequestInterceptor} to use. Should only be called from the same thread that is used
     * to {@link #startAsync(ComponentContext)} the component.
     *
     * @param actionRequestInterceptor Interceptor to use.
     */
    public void actionRequestInterceptor(ActionRequestInterceptor actionRequestInterceptor) {
        raftServer.actionRequestInterceptor(actionRequestInterceptor);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        RaftView raftConfig = raftConfiguration.value();

        var stripeSource = new RaftMetricSource(raftConfiguration.value().stripes(), raftConfiguration.value().logStripesCount());

        metricManager.registerSource(stripeSource);
        metricManager.enable(stripeSource);

        opts.setRaftMetrics(stripeSource);
        opts.setRpcInstallSnapshotTimeout(raftConfig.installSnapshotTimeoutMillis());
        opts.setStripes(raftConfig.disruptor().stripes());
        opts.setLogStripesCount(raftConfig.disruptor().logManagerStripes());
        opts.setLogYieldStrategy(raftConfig.logYieldStrategy());
        opts.getRaftOptions().setDisruptorBufferSize(raftConfig.disruptor().queueSize());

        opts.getRaftOptions().setSync(raftConfig.fsync());

        return raftServer.startAsync(componentContext);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        busyLock.block();

        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);

        return raftServer.stopAsync(componentContext);
    }

    @Override
    public <T extends RaftGroupService> T startRaftGroupNode(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupListener lsnr,
            RaftGroupEventsListener eventsLsnr,
            @Nullable RaftServiceFactory<T> factory,
            RaftGroupOptionsConfigurer groupOptionsConfigurer
    ) throws NodeStoppingException {
        RaftGroupOptions groupOptions = RaftGroupOptions.defaults();

        groupOptionsConfigurer.configure(groupOptions);

        return startRaftGroupNode(nodeId, configuration, lsnr, eventsLsnr, groupOptions, factory);
    }

    /**
     * Starts a Raft group on the current node.
     *
     * @param nodeId Raft node ID.
     * @param configuration Peers and Learners of the Raft group.
     * @param lsnr Raft group listener.
     * @param eventsLsnr Raft group events listener.
     * @param groupOptions Options to apply to the group.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    public RaftGroupService startRaftGroupNode(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupListener lsnr,
            RaftGroupEventsListener eventsLsnr,
            RaftGroupOptions groupOptions
    ) throws NodeStoppingException {
        return startRaftGroupNode(nodeId, configuration, lsnr, eventsLsnr, groupOptions, null);
    }

    /**
     * Starts a Raft group on the current node.
     *
     * @param nodeId Raft node ID.
     * @param configuration Peers and Learners of the Raft group.
     * @param lsnr Raft group listener.
     * @param eventsLsnr Raft group events listener.
     * @param groupOptions Options to apply to the group.
     * @param raftServiceFactory If not null, used for creation of raft group service.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    public <T extends RaftGroupService> T startRaftGroupNode(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupListener lsnr,
            RaftGroupEventsListener eventsLsnr,
            RaftGroupOptions groupOptions,
            @Nullable RaftServiceFactory<T> raftServiceFactory
    ) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            return startRaftGroupNodeInternal(
                    nodeId,
                    configuration,
                    lsnr,
                    eventsLsnr,
                    groupOptions,
                    raftServiceFactory,
                    StoppingExceptionFactories.indicateComponentStop()
            );
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public <T extends RaftGroupService> T startSystemRaftGroupNodeAndWaitNodeReady(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupListener lsnr,
            RaftGroupEventsListener eventsLsnr,
            @Nullable RaftServiceFactory<T> factory,
            RaftGroupOptionsConfigurer groupOptionsConfigurer
    ) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            RaftGroupOptions raftGroupOptions = RaftGroupOptions.defaults()
                    .setSystemGroup(true);

            groupOptionsConfigurer.configure(raftGroupOptions);

            return startRaftGroupNodeInternal(
                    nodeId,
                    configuration,
                    lsnr,
                    eventsLsnr,
                    // Use default marshaller here, because this particular method is used in very specific circumstances.
                    raftGroupOptions,
                    factory,
                    StoppingExceptionFactories.indicateNodeStop()
            );
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public RaftGroupService startRaftGroupService(ReplicationGroupId groupId, PeersAndLearners configuration, boolean isSystemGroup)
            throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            // Use default command marshaller here.
            return startRaftGroupServiceInternal(
                    groupId,
                    configuration,
                    opts.getCommandsMarshaller(),
                    StoppingExceptionFactories.indicateComponentStop(),
                    isSystemGroup
            );
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public <T extends RaftGroupService> T startRaftGroupService(
            ReplicationGroupId groupId,
            PeersAndLearners configuration,
            RaftServiceFactory<T> factory,
            @Nullable Marshaller commandsMarshaller,
            ExceptionFactory stoppingExceptionFactory,
            boolean isSystemGroup
    ) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            if (commandsMarshaller == null) {
                commandsMarshaller = opts.getCommandsMarshaller();
            }

            ThrottlingContextHolder throttlingContextHolder = isSystemGroup
                    ? systemGroupsThrottlingContextHolder
                    : partitionThrottlingContextHolder;

            return factory.startRaftGroupService(
                    groupId,
                    configuration,
                    raftConfiguration,
                    executor,
                    commandsMarshaller,
                    stoppingExceptionFactory,
                    throttlingContextHolder
            );
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public void destroyRaftNodeStorages(RaftNodeId nodeId, RaftGroupOptionsConfigurer raftGroupOptionsConfigurer)
            throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            RaftGroupOptions groupOptions = RaftGroupOptions.defaults();
            raftGroupOptionsConfigurer.configure(groupOptions);

            raftServer.destroyRaftNodeStoragesDurably(nodeId, groupOptions);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Destroys Raft group node storages (log storage, metadata storage and snapshots storage).
     *
     * <p>No durability guarantees are provided. If a node crashes, the storage may come to life.
     *
     * @param nodeId ID of the Raft node.
     * @param raftGroupOptions Group options.
     * @throws NodeStoppingException If the node is already being stopped.
     */
    public void destroyRaftNodeStorages(RaftNodeId nodeId, RaftGroupOptions raftGroupOptions) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            raftServer.destroyRaftNodeStorages(nodeId, raftGroupOptions);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Destroys Raft group node storages (log storage, metadata storage and snapshots storage).
     *
     * <p>Destruction is durable: that is, if this method returns and after that the node crashes, after it starts up, the storage
     * will not be there.
     *
     * @param nodeId ID of the Raft node.
     * @param raftGroupOptions Group options.
     * @throws NodeStoppingException If the node is already being stopped.
     */
    public void destroyRaftNodeStoragesDurably(RaftNodeId nodeId, RaftGroupOptions raftGroupOptions) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            raftServer.destroyRaftNodeStoragesDurably(nodeId, raftGroupOptions);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Returns Raft node IDs for which any storage (log storage or Raft meta storage) is present on disk.
     */
    public Set<StoredRaftNodeId> raftNodeIdsOnDisk() throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            return raftServer.raftNodeIdsOnDisk();
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public @Nullable IndexWithTerm raftNodeIndex(RaftNodeId nodeId) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            return raftServer.raftNodeIndex(nodeId);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private <T extends RaftGroupService> T startRaftGroupNodeInternal(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupListener lsnr,
            RaftGroupEventsListener raftGrpEvtsLsnr,
            RaftGroupOptions groupOptions,
            @Nullable RaftServiceFactory<T> raftServiceFactory,
            ExceptionFactory stoppingExceptionFactory
    ) {
        startRaftGroupNodeInternalWithoutService(nodeId, configuration, lsnr, raftGrpEvtsLsnr, groupOptions);

        Marshaller cmdMarshaller = requireNonNullElse(groupOptions.commandsMarshaller(), opts.getCommandsMarshaller());

        if (raftServiceFactory == null) {
            return (T) startRaftGroupServiceInternal(
                    nodeId.groupId(),
                    configuration,
                    cmdMarshaller,
                    stoppingExceptionFactory,
                    groupOptions.isSystemGroup()
            );
        } else {
            ThrottlingContextHolder throttlingContextHolder = groupOptions.isSystemGroup()
                    ? systemGroupsThrottlingContextHolder
                    : partitionThrottlingContextHolder;

            return raftServiceFactory.startRaftGroupService(
                    nodeId.groupId(),
                    configuration,
                    raftConfiguration,
                    executor,
                    cmdMarshaller,
                    stoppingExceptionFactory,
                    throttlingContextHolder
            );
        }
    }

    private void startRaftGroupNodeInternalWithoutService(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupListener lsnr,
            RaftGroupEventsListener raftGrpEvtsLsnr,
            RaftGroupOptions groupOptions
    ) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Start new raft node={} with initial configuration={}", nodeId, configuration);
        }

        boolean started = raftServer.startRaftNode(nodeId, configuration, raftGrpEvtsLsnr, lsnr, groupOptions);

        if (!started) {
            throw new IgniteInternalException(IgniteStringFormatter.format(
                    "Raft group on the node is already started [nodeId={}]",
                    nodeId
            ));
        }
    }

    private RaftGroupService startRaftGroupServiceInternal(
            ReplicationGroupId grpId,
            PeersAndLearners membersConfiguration,
            Marshaller commandsMarshaller,
            ExceptionFactory stoppingExceptionFactory,
            boolean isSystemGroup
    ) {
        ThrottlingContextHolder throttlingContextHolder = isSystemGroup
                ? systemGroupsThrottlingContextHolder
                : partitionThrottlingContextHolder;

        return RaftGroupServiceImpl.start(
                grpId,
                clusterNetSvc,
                FACTORY,
                raftConfiguration,
                membersConfiguration,
                executor,
                commandsMarshaller,
                stoppingExceptionFactory,
                throttlingContextHolder
        );
    }

    /**
     * Check if the node is started.
     *
     * @param nodeId Raft node ID.
     * @return True if the node is started.
     */
    public boolean isStarted(RaftNodeId nodeId) {
        return raftServer.isStarted(nodeId);
    }

    @Override
    public boolean stopRaftNode(RaftNodeId nodeId) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            if (LOG.isInfoEnabled()) {
                LOG.info("Stop raft node={}", nodeId);
            }

            return raftServer.stopRaftNode(nodeId);
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public boolean stopRaftNodes(ReplicationGroupId groupId) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            if (LOG.isInfoEnabled()) {
                LOG.info("Stop raft group={}", groupId);
            }

            return raftServer.stopRaftNodes(groupId);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Performs a {@code resetPeers} operation on raft node.
     *
     * @param raftNodeId Raft node ID.
     * @param peersAndLearners New node configuration.
     * @param sequenceToken Sequence token.
     */
    public void resetPeers(RaftNodeId raftNodeId, PeersAndLearners peersAndLearners, long sequenceToken) {
        LOG.warn("Reset peers for raft group {}, new configuration is {}, sequence token {}",
                raftNodeId, peersAndLearners, sequenceToken);

        raftServer.resetPeers(raftNodeId, peersAndLearners, sequenceToken);
    }

    /**
     * Iterates over all currently started raft services. Doesn't block the starting or stopping of other services, so consumer may
     * accidentally receive stopped service.
     *
     * @param consumer Closure to process each service.
     */
    public void forEach(BiConsumer<RaftNodeId, org.apache.ignite.raft.jraft.RaftGroupService> consumer) {
        raftServer.forEach(consumer);
    }

    /**
     * Returns messaging service.
     *
     * @return Messaging service.
     */
    public MessagingService messagingService() {
        return clusterNetSvc.messagingService();
    }

    /**
     * Returns topology service.
     *
     * @return Topology service.
     */
    public TopologyService topologyService() {
        return clusterNetSvc.topologyService();
    }

    public VolatileRaftConfiguration volatileRaft() {
        return raftConfiguration.volatileRaft();
    }

    /**
     * Returns a raft server.
     *
     * @return An underlying raft server.
     */
    @TestOnly
    public RaftServer server() {
        return raftServer;
    }

    /**
     * Returns a cluster service.
     *
     * @return An underlying network service.
     */
    @TestOnly
    public ClusterService service() {
        return clusterNetSvc;
    }

    /**
     * Returns a set of locally running Raft nodes.
     *
     * @return Set of Raft node IDs (can be empty if no local Raft nodes have been started).
     */
    @TestOnly
    public Set<RaftNodeId> localNodes() {
        return raftServer.localNodes();
    }
}
