/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.raft.server.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
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

    /** Cluster network service. */
    private final ClusterService clusterNetSvc;

    /** Raft server. */
    private final RaftServer raftServer;

    /** Executor for raft group services. */
    private final ScheduledExecutorService executor;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * The constructor.
     *
     * @param clusterNetSvc Cluster network service.
     * @param dataPath      Data path.
     */
    public Loza(ClusterService clusterNetSvc, Path dataPath) {
        this.clusterNetSvc = clusterNetSvc;

        this.raftServer = new JraftServerImpl(clusterNetSvc, dataPath);

        this.executor = new ScheduledThreadPoolExecutor(CLIENT_POOL_SIZE,
                new NamedThreadFactory(NamedThreadFactory.threadPrefix(clusterNetSvc.localConfiguration().getName(),
                        CLIENT_POOL_NAME)
                )
        );
    }

    /**
     * The constructor. Used for testing purposes.
     *
     * @param srv Pre-started raft server.
     */
    @TestOnly
    public Loza(JraftServerImpl srv) {
        this.clusterNetSvc = srv.clusterService();

        this.raftServer = srv;

        this.executor = new ScheduledThreadPoolExecutor(CLIENT_POOL_SIZE,
                new NamedThreadFactory(NamedThreadFactory.threadPrefix(clusterNetSvc.localConfiguration().getName(),
                        CLIENT_POOL_NAME)
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
     * Creates a raft group service providing operations on a raft group. If {@code nodes} contains the current node, then raft group starts
     * on the current node.
     *
     * @param groupId      Raft group id.
     * @param nodes        Raft group nodes.
     * @param lsnrSupplier Raft group listener supplier.
     * @return Future representing pending completion of the operation.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    public CompletableFuture<RaftGroupService> prepareRaftGroup(
            String groupId,
            List<ClusterNode> nodes,
            Supplier<RaftGroupListener> lsnrSupplier
    ) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            return prepareRaftGroupInternal(groupId, nodes, lsnrSupplier, () -> RaftGroupEventsListener.noopLsnr);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Internal method to a raft group creation.
     *
     * @param groupId                 Raft group id.
     * @param nodes                   Raft group nodes.
     * @param lsnrSupplier            Raft group listener supplier.
     * @param raftGrpEvtsLsnrSupplier Raft group events listener supplier.
     * @return Future representing pending completion of the operation.
     */
    private CompletableFuture<RaftGroupService> prepareRaftGroupInternal(String groupId, List<ClusterNode> nodes,
            Supplier<RaftGroupListener> lsnrSupplier, Supplier<RaftGroupEventsListener> raftGrpEvtsLsnrSupplier) {
        assert !nodes.isEmpty();

        List<Peer> peers = nodes.stream().map(n -> new Peer(n.address())).collect(Collectors.toList());

        String locNodeName = clusterNetSvc.topologyService().localMember().name();

        boolean hasLocalRaft = nodes.stream().anyMatch(n -> locNodeName.equals(n.name()));

        if (hasLocalRaft) {
            if (!raftServer.startRaftGroup(groupId, raftGrpEvtsLsnrSupplier.get(), lsnrSupplier.get(), peers)) {
                throw new IgniteInternalException(IgniteStringFormatter.format(
                        "Raft group on the node is already started [node={}, raftGrp={}]",
                        locNodeName,
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
     * If {@code deltaNodes} contains the current node, then raft group starts on the current node.
     *
     * @param grpId                   Raft group id.
     * @param nodes                   Full set of raft group nodes.
     * @param deltaNodes              New raft group nodes.
     * @param lsnrSupplier            Raft group listener supplier.
     * @param raftGrpEvtsLsnrSupplier Raft group events listener supplier.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    public void startRaftGroupNode(
            String grpId,
            Collection<ClusterNode> nodes,
            Collection<ClusterNode> deltaNodes,
            Supplier<RaftGroupListener> lsnrSupplier,
            Supplier<RaftGroupEventsListener> raftGrpEvtsLsnrSupplier) throws NodeStoppingException {
        assert !nodes.isEmpty();

        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            List<Peer> peers = nodes.stream().map(n -> new Peer(n.address())).collect(Collectors.toList());

            String locNodeName = clusterNetSvc.topologyService().localMember().name();

            if (deltaNodes.stream().anyMatch(n -> locNodeName.equals(n.name()))) {
                if (!raftServer.startRaftGroup(grpId, raftGrpEvtsLsnrSupplier.get(), lsnrSupplier.get(), peers)) {
                    throw new IgniteInternalException(IgniteStringFormatter.format(
                            "Raft group on the node is already started [node={}, raftGrp={}]",
                            locNodeName,
                            grpId
                    ));
                }
            }
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Creates a raft group service providing operations on a raft group. If {@code deltaNodes} contains the current node, then raft group
     * starts on the current node.
     *
     * @param grpId                   Raft group id.
     * @param nodes                   Full set of raft group nodes.
     * @param deltaNodes              New raft group nodes.
     * @param lsnrSupplier            Raft group listener supplier.
     * @param raftGrpEvtsLsnrSupplier Raft group events listener supplier.
     * @return Future representing pending completion of the operation.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    public CompletableFuture<RaftGroupService> updateRaftGroup(
            String grpId,
            Collection<ClusterNode> nodes,
            Collection<ClusterNode> deltaNodes,
            Supplier<RaftGroupListener> lsnrSupplier,
            Supplier<RaftGroupEventsListener> raftGrpEvtsLsnrSupplier
    ) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            return updateRaftGroupInternal(grpId, nodes, deltaNodes, lsnrSupplier, raftGrpEvtsLsnrSupplier);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Internal method for updating a raft group.
     *
     * @param grpId                   Raft group id.
     * @param nodes                   Full set of raft group nodes.
     * @param deltaNodes              New raft group nodes.
     * @param lsnrSupplier            Raft group listener supplier.
     * @param raftGrpEvtsLsnrSupplier Raft group events listener supplier.
     * @return Future representing pending completion of the operation.
     */
    private CompletableFuture<RaftGroupService> updateRaftGroupInternal(
            String grpId,
            Collection<ClusterNode> nodes,
            Collection<ClusterNode> deltaNodes,
            Supplier<RaftGroupListener> lsnrSupplier,
            Supplier<RaftGroupEventsListener> raftGrpEvtsLsnrSupplier) {
        assert !nodes.isEmpty();

        List<Peer> peers = nodes.stream().map(n -> new Peer(n.address())).collect(Collectors.toList());

        String locNodeName = clusterNetSvc.topologyService().localMember().name();

        if (deltaNodes.stream().anyMatch(n -> locNodeName.equals(n.name()))) {
            if (!raftServer.startRaftGroup(grpId,  raftGrpEvtsLsnrSupplier.get(), lsnrSupplier.get(), peers)) {
                throw new IgniteInternalException(IgniteStringFormatter.format(
                        "Raft group on the node is already started [node={}, raftGrp={}]",
                        locNodeName,
                        grpId
                ));
            }
        }

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
    }

    /**
     * Stops a raft group on the current node.
     *
     * @param groupId Raft group id.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    public void stopRaftGroup(String groupId) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            raftServer.stopRaftGroup(groupId);
        } finally {
            busyLock.leaveBusy();
        }
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
    public Set<String> startedGroups() {
        return raftServer.startedGroups();
    }
}
