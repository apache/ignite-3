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

import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.configuration.RaftView;
import org.apache.ignite.internal.raft.configuration.VolatileRaftConfiguration;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.TopologyService;
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

    /**
     * The constructor.
     *
     * @param clusterNetSvc Cluster network service.
     * @param raftConfiguration Raft configuration.
     * @param dataPath Data path.
     * @param clock A hybrid logical clock.
     */
    public Loza(
            ClusterService clusterNetSvc,
            RaftConfiguration raftConfiguration,
            Path dataPath,
            HybridClock clock,
            RaftGroupEventsClientListener raftGroupEventsClientListener
    ) {
        this.clusterNetSvc = clusterNetSvc;
        this.raftConfiguration = raftConfiguration;

        NodeOptions options = new NodeOptions();

        options.setClock(clock);
        options.setCommandsMarshaller(new ThreadLocalOptimizedMarshaller(clusterNetSvc.serializationRegistry()));

        this.opts = options;

        this.raftServer = new JraftServerImpl(clusterNetSvc, dataPath, raftConfiguration, options, raftGroupEventsClientListener);

        this.executor = new ScheduledThreadPoolExecutor(
                CLIENT_POOL_SIZE,
                NamedThreadFactory.create(clusterNetSvc.nodeName(), CLIENT_POOL_NAME, LOG)
        );
    }

    /**
     * The constructor.
     *
     * @param clusterNetSvc Cluster network service.
     * @param raftConfiguration Raft configuration.
     * @param dataPath Data path.
     * @param clock A hybrid logical clock.
     */
    @TestOnly
    public Loza(
            ClusterService clusterNetSvc,
            RaftConfiguration raftConfiguration,
            Path dataPath,
            HybridClock clock
    ) {
        this(
                clusterNetSvc,
                raftConfiguration,
                dataPath,
                clock,
                new RaftGroupEventsClientListener()
        );
    }

    /**
     * Sets {@link AppendEntriesRequestInterceptor} to use. Should only be called from the same thread that is used
     * to {@link #startAsync()} the component.
     *
     * @param appendEntriesRequestInterceptor Interceptor to use.
     */
    public void appendEntriesRequestInterceptor(AppendEntriesRequestInterceptor appendEntriesRequestInterceptor) {
        raftServer.appendEntriesRequestInterceptor(appendEntriesRequestInterceptor);
    }

    /**
     * Sets {@link ActionRequestInterceptor} to use. Should only be called from the same thread that is used
     * to {@link #startAsync()} the component.
     *
     * @param actionRequestInterceptor Interceptor to use.
     */
    public void actionRequestInterceptor(ActionRequestInterceptor actionRequestInterceptor) {
        raftServer.actionRequestInterceptor(actionRequestInterceptor);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> startAsync() {
        RaftView raftConfig = raftConfiguration.value();

        opts.setRpcInstallSnapshotTimeout(raftConfig.rpcInstallSnapshotTimeout());
        opts.setStripes(raftConfig.stripes());
        opts.setLogStripesCount(raftConfig.logStripesCount());
        opts.setLogYieldStrategy(raftConfig.logYieldStrategy());

        opts.getRaftOptions().setSync(raftConfig.fsync());

        return raftServer.startAsync();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> stopAsync() {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        busyLock.block();

        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);

        return raftServer.stopAsync();
    }

    @Override
    public <T extends RaftGroupService> CompletableFuture<T> startRaftGroupNode(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupListener lsnr,
            RaftGroupEventsListener eventsLsnr,
            RaftServiceFactory<T> factory
    ) throws NodeStoppingException {
        return startRaftGroupNode(nodeId, configuration, lsnr, eventsLsnr, RaftGroupOptions.defaults(), factory);
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
    public CompletableFuture<RaftGroupService> startRaftGroupNode(
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
    public <T extends RaftGroupService> CompletableFuture<T> startRaftGroupNode(
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
            return startRaftGroupNodeInternal(nodeId, configuration, lsnr, eventsLsnr, groupOptions, raftServiceFactory);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Starts a Raft group on the current node without starting raft service.
     *
     * @param nodeId Raft node ID.
     * @param configuration Peers and Learners of the Raft group.
     * @param lsnr Raft group listener.
     * @param eventsLsnr Raft group events listener.
     * @param groupOptions Options to apply to the group.
     */
    public void startRaftGroupNodeWithoutService(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupListener lsnr,
            RaftGroupEventsListener eventsLsnr,
            RaftGroupOptions groupOptions
    ) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            startRaftGroupNodeInternalWithoutService(nodeId, configuration, lsnr, eventsLsnr, groupOptions);
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<RaftGroupService> startRaftGroupNodeAndWaitNodeReadyFuture(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupListener lsnr,
            RaftGroupEventsListener eventsLsnr
    ) throws NodeStoppingException {
        CompletableFuture<RaftGroupService> fut = startRaftGroupNode(nodeId, configuration, lsnr, eventsLsnr, RaftGroupOptions.defaults());

        return fut;
    }

    @Override
    public CompletableFuture<RaftGroupService> startRaftGroupNodeAndWaitNodeReadyFuture(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupListener lsnr,
            RaftGroupEventsListener eventsLsnr,
            RaftNodeDisruptorConfiguration disruptorConfiguration
    ) throws NodeStoppingException {
        return startRaftGroupNodeAndWaitNodeReadyFuture(nodeId, configuration, lsnr, eventsLsnr, disruptorConfiguration, null);
    }

    @Override
    public <T extends RaftGroupService> CompletableFuture<T> startRaftGroupNodeAndWaitNodeReadyFuture(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupListener lsnr,
            RaftGroupEventsListener eventsLsnr,
            RaftNodeDisruptorConfiguration disruptorConfiguration,
            @Nullable RaftServiceFactory<T> factory
    ) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            CompletableFuture<T> startRaftServiceFuture = startRaftGroupNodeInternal(
                    nodeId,
                    configuration,
                    lsnr,
                    eventsLsnr,
                    // Use default marshaller here, because this particular method is used in very specific circumstances.
                    RaftGroupOptions.defaults()
                            .ownFsmCallerExecutorDisruptorConfig(disruptorConfiguration),
                    factory
            );

            return startRaftServiceFuture;
        } finally {
            busyLock.leaveBusy();
        }
    }

    @TestOnly
    @Override
    public CompletableFuture<RaftGroupService> startRaftGroupService(
            ReplicationGroupId groupId,
            PeersAndLearners configuration
    ) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            // Use default command marshaller here.
            return startRaftGroupServiceInternal(groupId, configuration, opts.getCommandsMarshaller());
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public <T extends RaftGroupService> CompletableFuture<T> startRaftGroupService(
            ReplicationGroupId groupId,
            PeersAndLearners configuration,
            RaftServiceFactory<T> factory,
            @Nullable Marshaller commandsMarshaller
    ) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            if (commandsMarshaller == null) {
                commandsMarshaller = opts.getCommandsMarshaller();
            }

            return factory.startRaftGroupService(groupId, configuration, raftConfiguration, executor, commandsMarshaller);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private <T extends RaftGroupService> CompletableFuture<T> startRaftGroupNodeInternal(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupListener lsnr,
            RaftGroupEventsListener raftGrpEvtsLsnr,
            RaftGroupOptions groupOptions,
            @Nullable RaftServiceFactory<T> raftServiceFactory
    ) {
        startRaftGroupNodeInternalWithoutService(nodeId, configuration, lsnr, raftGrpEvtsLsnr, groupOptions);

        Marshaller cmdMarshaller = requireNonNullElse(groupOptions.commandsMarshaller(), opts.getCommandsMarshaller());

        return raftServiceFactory == null
                ? (CompletableFuture<T>) startRaftGroupServiceInternal(nodeId.groupId(), configuration, cmdMarshaller)
                : raftServiceFactory.startRaftGroupService(nodeId.groupId(), configuration, raftConfiguration, executor, cmdMarshaller);
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

    private CompletableFuture<RaftGroupService> startRaftGroupServiceInternal(
            ReplicationGroupId grpId,
            PeersAndLearners membersConfiguration,
            Marshaller commandsMarshaller
    ) {
        return RaftGroupServiceImpl.start(
                grpId,
                clusterNetSvc,
                FACTORY,
                raftConfiguration,
                membersConfiguration,
                true,
                executor,
                commandsMarshaller
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
     */
    public void resetPeers(RaftNodeId raftNodeId, PeersAndLearners peersAndLearners) {
        LOG.warn("Reset peers for raft group {}, new configuration is {}", raftNodeId, peersAndLearners);

        raftServer.resetPeers(raftNodeId, peersAndLearners);
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

    @Override
    public LogSyncer getLogSyncer() {
        return raftServer.getLogSyncer();
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
