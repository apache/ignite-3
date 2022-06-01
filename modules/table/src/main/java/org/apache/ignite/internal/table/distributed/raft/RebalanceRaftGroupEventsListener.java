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

package org.apache.ignite.internal.table.distributed.raft;

import static org.apache.ignite.internal.metastorage.client.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.client.Conditions.revision;
import static org.apache.ignite.internal.metastorage.client.Operations.ops;
import static org.apache.ignite.internal.metastorage.client.Operations.put;
import static org.apache.ignite.internal.metastorage.client.Operations.remove;
import static org.apache.ignite.internal.utils.RebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.plannedPartAssignmentsKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.stablePartAssignmentsKey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.configuration.schema.ExtendedTableChange;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.metastorage.client.If;
import org.apache.ignite.internal.raft.server.RaftGroupEventsListener;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;

/**
 * Listener for the raft group events, which must provide correct error handling of rebalance process
 * and start new rebalance after the current one finished.
 */
public class RebalanceRaftGroupEventsListener implements RaftGroupEventsListener {
    /** Ignite logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(RebalanceRaftGroupEventsListener.class);

    /** Meta storage manager. */
    private final MetaStorageManager metaStorageMgr;

    /** Table configuration instance. */
    private final TableConfiguration tblConfiguration;

    /** Unique partition id. */
    private final String partId;

    /** Partition number. */
    private final int partNum;

    /** Busy lock of parent component for synchronous stop. */
    private final IgniteSpinBusyLock busyLock;

    /** Executor for scheduling rebalance retries. */
    private final ScheduledExecutorService rebalanceScheduler;

    /** Supplier of client for raft group of rebalance listener. */
    private final Supplier<RaftGroupService> raftGroupServiceSupplier;

    /** Attempts to retry the current rebalance in case of errors. */
    private final AtomicInteger rebalanceAttempts =  new AtomicInteger(0);

    /** Number of retrying of the current rebalance in case of errors. */
    private static final int REBALANCE_RETRY_THRESHOLD = 10;

    /** Delay between unsuccessful trial of a rebalance and a new trial, ms. */
    public static final int REBALANCE_RETRY_DELAY_MS = 200;

    /**
     * Constructs new listener.
     *
     * @param metaStorageMgr Meta storage manager.
     * @param tblConfiguration Table configuration.
     * @param partId Partition id.
     * @param partNum Partition number.
     * @param rebalanceScheduler Executor for scheduling rebalance retries.
     */
    public RebalanceRaftGroupEventsListener(
            MetaStorageManager metaStorageMgr,
            TableConfiguration tblConfiguration,
            String partId,
            int partNum,
            IgniteSpinBusyLock busyLock,
            Supplier<RaftGroupService> raftGroupServiceSupplier,
            ScheduledExecutorService rebalanceScheduler) {
        this.metaStorageMgr = metaStorageMgr;
        this.tblConfiguration = tblConfiguration;
        this.partId = partId;
        this.partNum = partNum;
        this.busyLock = busyLock;
        this.raftGroupServiceSupplier = raftGroupServiceSupplier;
        this.rebalanceScheduler = rebalanceScheduler;
    }

    /** {@inheritDoc} */
    @Override
    public void onLeaderElected(long term) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            rebalanceScheduler.schedule(() -> {
                if (!busyLock.enterBusy()) {
                    return;
                }

                try {
                    rebalanceAttempts.set(0);

                    metaStorageMgr.get(pendingPartAssignmentsKey(partId))
                            .thenCompose(pendingEntry -> {
                                if (!pendingEntry.empty()) {
                                    List<ClusterNode> pendingNodes = (List<ClusterNode>) ByteUtils.fromBytes(pendingEntry.value());

                                    return raftGroupServiceSupplier.get().changePeersAsync(clusterNodesToPeers(pendingNodes), term);
                                } else {
                                    return CompletableFuture.completedFuture(null);
                                }
                            }).get();
                } catch (InterruptedException | ExecutionException e) {
                    // TODO: IGNITE-17013 errors during this call should be handled by retry logic
                    LOG.error("Couldn't start rebalance for partition {} of table {} on new elected leader for term {}",
                            e, partNum, tblConfiguration.name().value(), term);
                } finally {
                    busyLock.leaveBusy();
                }
            }, 0, TimeUnit.MILLISECONDS);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onNewPeersConfigurationApplied(List<PeerId> peers) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            rebalanceScheduler.schedule(() -> {
                if (!busyLock.enterBusy()) {
                    return;
                }

                try {
                    doOnNewPeersConfigurationApplied(peers);
                } finally {
                    busyLock.leaveBusy();
                }
            }, 0, TimeUnit.MILLISECONDS);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onReconfigurationError(Status status, List<PeerId> peers, long term) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            if (status == null) {
                // leader stepped down, so we are expecting RebalanceRaftGroupEventsListener.onLeaderElected to be called on a new leader.
                LOG.info("Leader stepped down during the current rebalance for the partId = {}.", partId);

                return;
            }

            assert status.getRaftError() == RaftError.ECATCHUP : "According to the JRaft protocol, RaftError.ECATCHUP is expected.";

            LOG.warn("Error occurred during the current rebalance for partId = {}.", partId);

            if (rebalanceAttempts.incrementAndGet() < REBALANCE_RETRY_THRESHOLD) {
                scheduleChangePeers(peers, term);
            } else {
                LOG.error("The number of retries of the rebalance for the partId = {} exceeded the threshold = {}.", partId,
                        REBALANCE_RETRY_THRESHOLD);

                // TODO: currently we just retry intent to change peers according to the rebalance infinitely, until new leader is elected,
                // TODO: but rebalance cancel mechanism should be implemented. https://issues.apache.org/jira/browse/IGNITE-17056
                scheduleChangePeers(peers, term);
            }
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Schedules changing peers according to the current rebalance.
     *
     * @param peers Peers to change configuration for a raft group.
     * @param term Current known leader term.
     */
    private void scheduleChangePeers(List<PeerId> peers, long term) {
        rebalanceScheduler.schedule(() -> {
            if (!busyLock.enterBusy()) {
                return;
            }

            LOG.info("Started {} attempt to retry the current rebalance for the partId = {}.", rebalanceAttempts.get(), partId);

            try {
                raftGroupServiceSupplier.get().changePeersAsync(peerIdsToPeers(peers), term).get();
            } catch (InterruptedException | ExecutionException e) {
                // TODO: IGNITE-17013 errors during this call should be handled by retry logic
                LOG.error("Error during the rebalance retry for the partId = {}", e, partId);
            } finally {
                busyLock.leaveBusy();
            }
        }, REBALANCE_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Implementation of {@link RebalanceRaftGroupEventsListener#onNewPeersConfigurationApplied(List)}.
     *
     * @param peers Peers
     */
    private void doOnNewPeersConfigurationApplied(List<PeerId> peers) {
        try {
            Map<ByteArray, Entry> keys = metaStorageMgr.getAll(
                    Set.of(
                            plannedPartAssignmentsKey(partId),
                            pendingPartAssignmentsKey(partId),
                            stablePartAssignmentsKey(partId))).get();

            Entry plannedEntry = keys.get(plannedPartAssignmentsKey(partId));

            List<ClusterNode> appliedPeers = resolveClusterNodes(peers,
                    keys.get(pendingPartAssignmentsKey(partId)).value(), keys.get(stablePartAssignmentsKey(partId)).value());

            tblConfiguration.change(ch -> {
                List<List<ClusterNode>> assignments =
                        (List<List<ClusterNode>>) ByteUtils.fromBytes(((ExtendedTableChange) ch).assignments());
                assignments.set(partNum, appliedPeers);
                ((ExtendedTableChange) ch).changeAssignments(ByteUtils.toBytes(assignments));
            }).get();

            if (plannedEntry.value() != null) {
                if (!metaStorageMgr.invoke(If.iif(
                        revision(plannedPartAssignmentsKey(partId)).eq(plannedEntry.revision()),
                        ops(
                                put(stablePartAssignmentsKey(partId), ByteUtils.toBytes(appliedPeers)),
                                put(pendingPartAssignmentsKey(partId), plannedEntry.value()),
                                remove(plannedPartAssignmentsKey(partId)))
                                .yield(true),
                        ops().yield(false))).get().getAsBoolean()) {
                    doOnNewPeersConfigurationApplied(peers);
                }
            } else {
                if (!metaStorageMgr.invoke(If.iif(
                        notExists(plannedPartAssignmentsKey(partId)),
                        ops(put(stablePartAssignmentsKey(partId), ByteUtils.toBytes(appliedPeers)),
                                remove(pendingPartAssignmentsKey(partId))).yield(true),
                        ops().yield(false))).get().getAsBoolean()) {
                    doOnNewPeersConfigurationApplied(peers);
                }
            }

            rebalanceAttempts.set(0);
        } catch (InterruptedException | ExecutionException e) {
            // TODO: IGNITE-17013 errors during this call should be handled by retry logic
            LOG.error("Could't commit new partition configuration to metastore for table = {}, partition = {}",
                    e, tblConfiguration.name(), partNum);
        }
    }

    private static List<ClusterNode> resolveClusterNodes(
            List<PeerId> peers, byte[] pendingAssignments, byte[] stableAssignments) {
        Map<NetworkAddress, ClusterNode> resolveRegistry = new HashMap<>();

        if (pendingAssignments != null) {
            ((List<ClusterNode>) ByteUtils.fromBytes(pendingAssignments)).forEach(n -> resolveRegistry.put(n.address(), n));
        }

        if (stableAssignments != null) {
            ((List<ClusterNode>) ByteUtils.fromBytes(stableAssignments)).forEach(n -> resolveRegistry.put(n.address(), n));
        }

        List<ClusterNode> resolvedNodes = new ArrayList<>(peers.size());

        for (PeerId p : peers) {
            var addr = NetworkAddress.from(p.getEndpoint().getIp() + ":" + p.getEndpoint().getPort());

            if (resolveRegistry.containsKey(addr)) {
                resolvedNodes.add(resolveRegistry.get(addr));
            } else {
                throw new IgniteInternalException("Can't find appropriate cluster node for raft group peer: " + p);
            }
        }

        return resolvedNodes;
    }

    /**
     * Transforms list of cluster nodes to the list of peers.
     *
     * @param nodes List of cluster nodes to transform.
     * @return List of transformed peers.
     */
    private static List<Peer> clusterNodesToPeers(List<ClusterNode> nodes) {
        List<Peer> peers = new ArrayList<>(nodes.size());

        for (ClusterNode node : nodes) {
            peers.add(new Peer(node.address()));
        }

        return peers;
    }

    /**
     * Transforms list of peerIds to list of peers.
     *
     * @param peerIds List of peerIds to transform.
     * @return List of transformed peers.
     */
    private static List<Peer> peerIdsToPeers(List<PeerId> peerIds) {
        List<Peer> peers = new ArrayList<>(peerIds.size());

        for (PeerId peerId : peerIds) {
            peers.add(new Peer(NetworkAddress.from(peerId.getEndpoint().toString())));
        }

        return peers;
    }
}
