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

package org.apache.ignite.internal.cluster.management;

import static org.apache.ignite.internal.util.subscription.Publishers.combineLatest;
import static org.apache.ignite.internal.util.subscription.Publishers.filter;
import static org.apache.ignite.internal.util.subscription.Publishers.firstItem;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscription;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.TopologyEventHandler;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.network.ClusterNode;

public class ClusterNodesJoinAwaiter {
    private final ClusterService clusterService;
    private final LogicalTopology logicalTopology;

    public ClusterNodesJoinAwaiter(ClusterService clusterService, LogicalTopology logicalTopology) {
        this.clusterService = clusterService;
        this.logicalTopology = logicalTopology;
    }

    public CompletableFuture<Void> awaitAllNodesToJoin() {
        Publisher<Collection<ClusterNode>> physicalTopologyPublisher = physicalTopologyPublisher(clusterService.topologyService());
        Publisher<LogicalTopologySnapshot> logicalTopologyPublisher = logicalTopologyPublisher(logicalTopology);

        Publisher<Boolean> allNodesJoinedCluster =
                combineLatest(physicalTopologyPublisher, logicalTopologyPublisher, (physical, logical) -> {
                    Set<UUID> physicalNodesSet = physical.stream().map(ClusterNode::id).collect(Collectors.toSet());
                    Set<UUID> logicalNodesSet = logical.nodes().stream().map(ClusterNode::id).collect(Collectors.toSet());

                    physicalNodesSet.removeAll(logicalNodesSet);

                    return physicalNodesSet.isEmpty();
                });

        return firstItem(filter(allNodesJoinedCluster, it -> it == Boolean.TRUE))
                .thenApply(unused -> null);
    }

    private static Publisher<Collection<ClusterNode>> physicalTopologyPublisher(TopologyService topologyService) {
        return subscriber -> {
            TopologyEventHandler eventHandler = new TopologyEventHandler() {
                @Override
                public void onAppeared(ClusterNode member) {
                    subscriber.onNext(topologyService.allMembers());
                }

                @Override
                public void onDisappeared(ClusterNode member) {
                    subscriber.onNext(topologyService.allMembers());
                }
            };

            Subscription subscription = new Subscription() {
                @Override
                public void request(long n) {
                    // noop backpressure: our subscription is always hot, because it's based on a listener push semantics.
                }

                @Override
                public void cancel() {
                    topologyService.removeEventHandler(eventHandler);
                }
            };


            subscriber.onSubscribe(subscription);

            topologyService.addEventHandler(eventHandler);

            subscriber.onNext(topologyService.allMembers());
        };
    }

    private static Publisher<LogicalTopologySnapshot> logicalTopologyPublisher(LogicalTopology logicalTopology) {
        return subscriber -> {
            LogicalTopologyEventListener eventListener = new LogicalTopologyEventListener() {
                @Override
                public void onNodeJoined(LogicalNode joinedNode, LogicalTopologySnapshot newTopology) {
                    subscriber.onNext(newTopology);
                }

                @Override
                public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
                    subscriber.onNext(newTopology);
                }

                @Override
                public void onTopologyLeap(LogicalTopologySnapshot newTopology) {
                    subscriber.onNext(newTopology);
                }
            };

            Subscription subscription = new Subscription() {
                @Override
                public void request(long n) {
                    // noop: our subscription is always hot, because it's based on a listener push semantics
                }

                @Override
                public void cancel() {
                    logicalTopology.removeEventListener(eventListener);
                }
            };

            subscriber.onSubscribe(subscription);
            logicalTopology.addEventListener(eventListener);
            subscriber.onNext(logicalTopology.getLogicalTopology());
        };
    }
}
