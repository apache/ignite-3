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

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.configuration.VolatileRaftConfiguration;
import org.apache.ignite.internal.raft.server.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupServiceImpl;
import org.apache.ignite.raft.jraft.util.Utils;
import org.jetbrains.annotations.TestOnly;

/**
 * Best raft manager ever since 1982.
 */
public class Loza implements IgniteComponent {
    /** Factory. */
    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    /** Raft client pool name. */
    public static final String CLIENT_POOL_NAME = "Raft-Group-Client";

    /** Raft client pool size. Size was taken from jraft's TimeManager. */
    private static final int CLIENT_POOL_SIZE = Math.min(Utils.cpus() * 3, 20);

    /** Timeout. */
    private static final int RETRY_TIMEOUT = 10000;

    /** Network timeout. */
    private static final int RPC_TIMEOUT = 3000;

    /** Retry delay. */
    private static final int DELAY = 200;

    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(Loza.class);

    /** Cluster network service. */
    private final ClusterService clusterNetSvc;

    /** Raft server. */
    private final RaftServer raftServer;

    /** Executor for raft group services. */
    private final ScheduledExecutorService executor;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Raft configuration. */
    private final RaftConfiguration raftConfiguration;

    /**
     * The constructor.
     *
     * @param clusterNetSvc Cluster network service.
     * @param dataPath      Data path.
     * @param clock         A hybrid logical clock.
     */
    public Loza(ClusterService clusterNetSvc, RaftConfiguration raftConfiguration, Path dataPath, HybridClock clock) {
        this.clusterNetSvc = clusterNetSvc;
        this.raftConfiguration = raftConfiguration;

        NodeOptions options = new NodeOptions();

        options.setClock(clock);

        this.raftServer = new JraftServerImpl(clusterNetSvc, dataPath, options);

        this.executor = new ScheduledThreadPoolExecutor(CLIENT_POOL_SIZE,
                new NamedThreadFactory(NamedThreadFactory.threadPrefix(clusterNetSvc.localConfiguration().getName(),
                        CLIENT_POOL_NAME), LOG
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        raftServer.start();
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);

        raftServer.stop();
    }

    /**
     * Determines whether a RAFT group should be started locally according to a collection of nodes that should have a RAFT group.
     */
    public boolean shouldHaveRaftGroupLocally(Collection<ClusterNode> raftNodes) {
        String locNodeName = clusterNetSvc.topologyService().localMember().name();

        return raftNodes.stream().anyMatch(n -> locNodeName.equals(n.name()));
    }

    /**
     * Creates a raft group service providing operations on a raft group. If {@code nodes} contains the current node, then raft group starts
     * on the current node.
     *
     * @param groupId Raft group id.
     * @param nodes Raft group nodes.
     * @param lsnrSupplier Raft group listener supplier.
     * @param groupOptions Options to apply to the group.
     * @return Future representing pending completion of the operation.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    public CompletableFuture<RaftGroupService> prepareRaftGroup(
            ReplicationGroupId groupId,
            List<ClusterNode> nodes,
            Supplier<RaftGroupListener> lsnrSupplier,
            RaftGroupOptions groupOptions
    ) throws NodeStoppingException {
        return prepareRaftGroup(groupId, nodes, lsnrSupplier, () -> RaftGroupEventsListener.noopLsnr, groupOptions);
    }

    /**
     * Creates a raft group service providing operations on a raft group. If {@code nodes} contains the current node, then raft group starts
     * on the current node.
     *
     * @param groupId Raft group id.
     * @param nodes Raft group nodes.
     * @param lsnrSupplier Raft group listener supplier.
     * @param raftGrpEvtsLsnrSupplier Raft group events listener supplier.
     * @param groupOptions Options to apply to the group.
     * @return Future representing pending completion of the operation.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    public CompletableFuture<RaftGroupService> prepareRaftGroup(
            ReplicationGroupId groupId,
            List<ClusterNode> nodes,
            Supplier<RaftGroupListener> lsnrSupplier,
            Supplier<RaftGroupEventsListener> raftGrpEvtsLsnrSupplier,
            RaftGroupOptions groupOptions
    ) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            return prepareRaftGroupInternal(groupId, nodes, lsnrSupplier, raftGrpEvtsLsnrSupplier, groupOptions);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Internal method to a raft group creation.
     *
     * @param groupId Raft group id.
     * @param nodes Raft group nodes.
     * @param lsnrSupplier Raft group listener supplier.
     * @param raftGrpEvtsLsnrSupplier Raft group events listener supplier.
     * @param groupOptions Options to apply to the group.
     * @return Future representing pending completion of the operation.
     */
    private CompletableFuture<RaftGroupService> prepareRaftGroupInternal(
            ReplicationGroupId groupId,
            List<ClusterNode> nodes,
            Supplier<RaftGroupListener> lsnrSupplier,
            Supplier<RaftGroupEventsListener> raftGrpEvtsLsnrSupplier,
            RaftGroupOptions groupOptions
    ) {
        assert !nodes.isEmpty();

        List<Peer> peers = nodes.stream().map(n -> new Peer(n.address())).collect(Collectors.toList());

        boolean hasLocalRaft = shouldHaveRaftGroupLocally(nodes);

        if (hasLocalRaft) {
            LOG.info("Start new raft node for group={} with initial peers={}", groupId, peers);

            if (!raftServer.startRaftGroup(groupId, raftGrpEvtsLsnrSupplier.get(), lsnrSupplier.get(), peers, groupOptions)) {
                throw new IgniteInternalException(IgniteStringFormatter.format(
                        "Raft group on the node is already started [raftGrp={}]",
                        groupId
                ));
            }
        }

        return RaftGroupServiceImpl.start(
                groupId,
                clusterNetSvc,
                FACTORY,
                RETRY_TIMEOUT,
                RPC_TIMEOUT,
                peers,
                true,
                DELAY,
                executor
        );
    }

    /**
     * Start RAFT group on the current node.
     *
     * @param grpId Raft group id.
     * @param nodes Full set of raft group nodes.
     * @param lsnr Raft group listener.
     * @param raftGrpEvtsLsnr Raft group events listener.
     * @param groupOptions Options to apply to the group.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    public void startRaftGroupNode(
            ReplicationGroupId grpId,
            Collection<ClusterNode> nodes,
            RaftGroupListener lsnr,
            RaftGroupEventsListener raftGrpEvtsLsnr,
            RaftGroupOptions groupOptions
    ) throws NodeStoppingException {
        assert !nodes.isEmpty();

        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            List<Peer> peers = nodes.stream().map(n -> new Peer(n.address())).collect(Collectors.toList());

            LOG.info("Start new raft node for group={} with initial peers={}", grpId, peers);

            if (!raftServer.startRaftGroup(grpId, raftGrpEvtsLsnr, lsnr, peers, groupOptions)) {
                throw new IgniteInternalException(IgniteStringFormatter.format(
                        "Raft group on the node is already started [raftGrp={}]",
                        grpId
                ));
            }
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Creates and starts a raft group service providing operations on a raft group.
     *
     * @param grpId RAFT group id.
     * @param nodes Full set of raft group nodes.
     * @return Future that will be completed with an instance of RAFT group service.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    public CompletableFuture<RaftGroupService> startRaftGroupService(
            ReplicationGroupId grpId,
            Collection<ClusterNode> nodes
    ) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        List<Peer> peers = nodes.stream().map(n -> new Peer(n.address())).collect(Collectors.toList());

        try {
            return RaftGroupServiceImpl.start(
                    grpId,
                    clusterNetSvc,
                    FACTORY,
                    RETRY_TIMEOUT,
                    RPC_TIMEOUT,
                    peers,
                    true,
                    DELAY,
                    executor
            );
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Stops a raft group on the current node.
     *
     * @param groupId Raft group id.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    public void stopRaftGroup(ReplicationGroupId groupId) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            LOG.info("Stop raft group={}", groupId);

            raftServer.stopRaftGroup(groupId);
        } finally {
            busyLock.leaveBusy();
        }
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
     * Returns a cluster service.
     *
     * @return An underlying network service.
     */
    @TestOnly
    public ClusterService service() {
        return clusterNetSvc;
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
     * Returns started groups.
     *
     * @return Started groups.
     */
    @TestOnly
    public Set<ReplicationGroupId> startedGroups() {
        return raftServer.startedGroups();
    }
}
