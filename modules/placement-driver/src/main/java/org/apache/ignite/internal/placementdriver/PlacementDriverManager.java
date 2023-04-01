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

package org.apache.ignite.internal.placementdriver;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.jetbrains.annotations.TestOnly;

/**
 * Placement driver manager.
 * The manager is a leaseholder tracker: response of renewal of the lease and discover of appear/disappear of a replication group member.
 * The another role of the manager is providing a node, which is leaseholder at the moment, for a particular replication group.
 */
public class PlacementDriverManager implements IgniteComponent {
    static final String PLACEMENTDRIVER_PREFIX = "placementdriver.lease.";
    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean isStopped = new AtomicBoolean();

    /** Cluster service. */
    private final ClusterService clusterService;

    /** Replication group id to store placement driver data. */
    private final ReplicationGroupId replicationGroupId;

    /** The closure determines nodes where are participants of placement driver. */
    private final Supplier<CompletableFuture<Set<String>>> placementDriverNodesNamesProvider;

    private final RaftManager raftManager;

    private final TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory;

    /**
     * Raft client future. Can contain null, if this node is not in placement driver group.
     */
    private final CompletableFuture<TopologyAwareRaftGroupService> raftClientFuture;

    /** Lease tracker. */
    private final LeaseTracker leaseTracker;

    /** Lease updater. */
    private final LeaseUpdater leaseUpdater;

    private volatile boolean isActiveActor;

    /**
     * The constructor.
     *
     * @param metaStorageMgr Meta Storage manager.
     * @param vaultManager Vault manager.
     * @param replicationGroupId Id of placement driver group.
     * @param clusterService Cluster service.
     * @param placementDriverNodesNamesProvider Provider of the set of placement driver nodes' names.
     * @param logicalTopologyService Logical topology service.
     * @param raftManager Raft manager.
     * @param topologyAwareRaftGroupServiceFactory Raft client factory.
     * @param tablesCfg Table configuration.
     * @param clock Hybrid clock.
     */
    public PlacementDriverManager(
            MetaStorageManager metaStorageMgr,
            VaultManager vaultManager,
            ReplicationGroupId replicationGroupId,
            ClusterService clusterService,
            Supplier<CompletableFuture<Set<String>>> placementDriverNodesNamesProvider,
            LogicalTopologyService logicalTopologyService,
            RaftManager raftManager,
            TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory,
            TablesConfiguration tablesCfg,
            DistributionZonesConfiguration distributionZonesConfiguration,
            HybridClock clock
    ) {
        this.replicationGroupId = replicationGroupId;
        this.clusterService = clusterService;
        this.placementDriverNodesNamesProvider = placementDriverNodesNamesProvider;
        this.raftManager = raftManager;
        this.topologyAwareRaftGroupServiceFactory = topologyAwareRaftGroupServiceFactory;

        this.raftClientFuture = new CompletableFuture<>();
        this.leaseTracker = new LeaseTracker(vaultManager, metaStorageMgr);
        this.leaseUpdater = new LeaseUpdater(
                vaultManager,
                metaStorageMgr,
                logicalTopologyService,
                tablesCfg,
                distributionZonesConfiguration,
                leaseTracker,
                clock
        );
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        placementDriverNodesNamesProvider.get()
                .thenCompose(placementDriverNodes -> {
                    String thisNodeName = clusterService.topologyService().localMember().name();

                    if (placementDriverNodes.contains(thisNodeName)) {
                        try {
                            leaseUpdater.init(thisNodeName);

                            return raftManager.startRaftGroupService(
                                    replicationGroupId,
                                    PeersAndLearners.fromConsistentIds(placementDriverNodes),
                                    topologyAwareRaftGroupServiceFactory
                                ).thenCompose(client -> client.subscribeLeader(this::onLeaderChange).thenApply(v -> client));
                        } catch (NodeStoppingException e) {
                            return failedFuture(e);
                        }
                    } else {
                        return completedFuture(null);
                    }
                })
                .whenComplete((client, ex) -> {
                    if (ex == null) {
                        raftClientFuture.complete(client);
                    } else {
                        raftClientFuture.completeExceptionally(ex);
                    }
                });

        leaseTracker.startTrack();
    }

    /** {@inheritDoc} */
    @Override
    public void beforeNodeStop() {
        withRaftClientIfPresent(c -> {
            c.unsubscribeLeader().join();

            leaseUpdater.deInit();
        });

        leaseTracker.stopTrack();
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (!isStopped.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        withRaftClientIfPresent(TopologyAwareRaftGroupService::shutdown);
    }

    private void withRaftClientIfPresent(Consumer<TopologyAwareRaftGroupService> closure) {
        raftClientFuture.thenAccept(client -> {
            if (client != null) {
                closure.accept(client);
            }
        });
    }

    private void onLeaderChange(ClusterNode leader, Long term) {
        if (leader.equals(clusterService.topologyService().localMember())) {
            takeOverActiveActor();
        } else {
            stepDownActiveActor();
        }
    }

    /**
     * Takes over active actor of placement driver group.
     */
    private void takeOverActiveActor() {
        isActiveActor = true;

        leaseUpdater.activate();
    }


    /**
     * Steps down as active actor.
     */
    private void stepDownActiveActor() {
        isActiveActor = false;

        leaseUpdater.deactivate();
    }

    @TestOnly
    boolean isActiveActor() {
        return isActiveActor;
    }
}
