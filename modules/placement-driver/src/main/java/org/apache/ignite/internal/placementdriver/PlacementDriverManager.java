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

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.jetbrains.annotations.TestOnly;

/**
 * Placement driver manager.
 * The manager is a leaseholder tracker: response of renewal of the lease and discover of appear/disappear of a replication group member.
 * The another role of the manager is providing a node, which is leaseholder at the moment, for a particular replication group.
 */
public class PlacementDriverManager implements IgniteComponent {
    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final RaftMessagesFactory raftMessagesFactory = new RaftMessagesFactory();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean isStopped = new AtomicBoolean();

    private final ReplicationGroupId replicationGroupId;

    private final ClusterService clusterService;

    private final Supplier<CompletableFuture<Set<String>>> placementDriverNodesNamesProvider;

    /**
     * Raft client future. Can contain null, if this node is not in placement driver group.
     */
    private final CompletableFuture<TopologyAwareRaftGroupService> raftClientFuture;

    private final ScheduledExecutorService raftClientExecutor;

    private final LogicalTopologyService logicalTopologyService;

    private final RaftConfiguration raftConfiguration;

    private volatile boolean isActiveActor;

    private volatile long lastTermSeen = -1;

    /**
     * The constructor.
     *
     * @param replicationGroupId Id of placement driver group.
     * @param clusterService Cluster service.
     * @param raftConfiguration Raft configuration.
     * @param placementDriverNodesNamesProvider Provider of the set of placement driver nodes' names.
     * @param logicalTopologyService Logical topology service.
     * @param raftClientExecutor Raft client executor.
     */
    public PlacementDriverManager(
            ReplicationGroupId replicationGroupId,
            ClusterService clusterService,
            RaftConfiguration raftConfiguration,
            Supplier<CompletableFuture<Set<String>>> placementDriverNodesNamesProvider,
            LogicalTopologyService logicalTopologyService,
            ScheduledExecutorService raftClientExecutor
    ) {
        this.replicationGroupId = replicationGroupId;
        this.clusterService = clusterService;
        this.raftConfiguration = raftConfiguration;
        this.placementDriverNodesNamesProvider = placementDriverNodesNamesProvider;
        this.logicalTopologyService = logicalTopologyService;
        this.raftClientExecutor = raftClientExecutor;

        raftClientFuture = new CompletableFuture<>();
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        placementDriverNodesNamesProvider.get()
                .thenCompose(placementDriverNodes -> {
                    String thisNodeName = clusterService.topologyService().localMember().name();

                    if (placementDriverNodes.contains(thisNodeName)) {
                        return TopologyAwareRaftGroupService.start(
                                replicationGroupId,
                                clusterService,
                                raftMessagesFactory,
                                raftConfiguration,
                                PeersAndLearners.fromConsistentIds(placementDriverNodes),
                                true,
                                raftClientExecutor,
                                logicalTopologyService,
                                true
                            ).thenCompose(client -> {
                                TopologyAwareRaftGroupService topologyAwareClient = (TopologyAwareRaftGroupService) client;

                                return topologyAwareClient.subscribeLeader(this::onLeaderChange).thenApply(v -> topologyAwareClient);
                            });
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
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (!isStopped.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        withRaftClientIfPresent(client -> {
            client.unsubscribeLeader();
            client.shutdown();
        });
    }

    private void withRaftClientIfPresent(Consumer<TopologyAwareRaftGroupService> closure) {
        raftClientFuture.thenAccept(client -> {
            if (client != null) {
                closure.accept(client);
            }
        });
    }

    private void onLeaderChange(ClusterNode leader, Long term) {
        if (term > lastTermSeen) {
            if (leader.equals(clusterService.topologyService().localMember())) {
                takeOverActiveActor();
            } else {
                stepDownActiveActor();
            }

            lastTermSeen = term;
        }
    }

    /**
     * Takes over active actor of placement driver group.
     */
    private void takeOverActiveActor() {
        isActiveActor = true;
    }


    /**
     * Steps down as active actor.
     */
    private void stepDownActiveActor() {
        isActiveActor = false;
    }

    @TestOnly
    boolean isActiveActor() {
        return isActiveActor;
    }
}
