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

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.apache.ignite.internal.cluster.management.LogicalTopologyJoinAwaiter.awaitLogicalTopologyToMatchPhysicalTopology;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyIterable.emptyIterable;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class LogicalTopologyJoinAwaiterTest {
    private static final ClusterNode node1 = newClusterNode();
    private static final ClusterNode node2 = newClusterNode();
    private static final ClusterNode node3 = newClusterNode();
    private static final ClusterNode node4 = newClusterNode();

    @ParameterizedTest(name = ParameterizedTest.INDEX_PLACEHOLDER + " positiveLogicalTopologyJoinTest: {0}")
    @Timeout(5)
    @MethodSource("positiveClusterBehaviour")
    void positiveLogicalTopologyJoinTest(
            BiConsumer<PhysicalTopologySimulator, LogicalTopologySimulator> behaviourSimulation
    ) throws Exception {
        Set<ClusterNode> initialPhysicalTopology = Set.of(node1, node2, node3, node4);
        PhysicalTopologySimulator physicalTopologySimulator = new PhysicalTopologySimulator(initialPhysicalTopology);
        LogicalTopologySimulator logicalTopologySimulator = new LogicalTopologySimulator(emptySet());

        ExecutorService simulationExecutor = Executors.newSingleThreadExecutor();
        try {
            CompletableFuture<Void> allNodesJoinedLogicalTopology =
                    awaitLogicalTopologyToMatchPhysicalTopology(physicalTopologySimulator, logicalTopologySimulator);

            Future<?> simulationJob = simulationExecutor.submit(() -> {
                behaviourSimulation.accept(physicalTopologySimulator, logicalTopologySimulator);
            });

            simulationJob.get(5, TimeUnit.SECONDS);

            assertThat(allNodesJoinedLogicalTopology, CompletableFutureMatcher.willCompleteSuccessfully());
            assertTrue(physicalTopologySimulator.hasNoEventHandlers());
            assertTrue(logicalTopologySimulator.hasNoEventHandlers());
        } finally {
            assertThat(simulationExecutor.shutdownNow(), emptyIterable());
        }
    }

    private static Stream<BiConsumer<PhysicalTopologySimulator, LogicalTopologySimulator>> positiveClusterBehaviour() {
        return Stream.of(
                simulation("all nodes join logical topology normally", (physicalTopology, logicalTopology) -> {
                    logicalTopology.putNode(logical(node1));
                    logicalTopology.putNode(logical(node2));
                    logicalTopology.putNode(logical(node3));
                    logicalTopology.putNode(logical(node4));
                }),
                simulation("3 nodes join logical topology and node4 leaves physical topology", (physicalTopology, logicalTopology) -> {
                    logicalTopology.putNode(logical(node1));
                    logicalTopology.putNode(logical(node2));
                    logicalTopology.putNode(logical(node3));
                    physicalTopology.removeNode(node4);
                }),
                simulation("node2 leaves logical topology and between but then recovers", (physicalTopology, logicalTopology) -> {
                    logicalTopology.putNode(logical(node1));
                    logicalTopology.putNode(logical(node2));
                    logicalTopology.putNode(logical(node3));
                    logicalTopology.removeNodes(Set.of(logical(node2)));
                    logicalTopology.putNode(logical(node4));
                    logicalTopology.putNode(logical(node2));
                }),
                simulation("node2 leaves physical topology and then joins back", (physicalTopology, logicalTopology) -> {
                    logicalTopology.putNode(logical(node1));
                    physicalTopology.removeNode(node2);
                    logicalTopology.putNode(logical(node3));
                    logicalTopology.putNode(logical(node4));
                    physicalTopology.addNode(node2);
                    logicalTopology.putNode(logical(node2));
                }),
                simulation("logical topology leaps", (physicalTopology, logicalTopology) -> {
                    logicalTopology.putNode(logical(node1));
                    logicalTopology.fireTopologyLeap(Set.of(logical(node2), logical(node3), logical(node4)));
                    logicalTopology.putNode(logical(node1));
                }),
                simulation("physical topology gains node5", (physicalTopology, logicalTopology) -> {
                    logicalTopology.putNode(logical(node1));
                    logicalTopology.fireTopologyLeap(Set.of(logical(node2), logical(node3)));
                    logicalTopology.putNode(logical(node1));
                    ClusterNode node5 = newClusterNode();
                    physicalTopology.addNode(node5);
                    logicalTopology.putNode(logical(node5));
                    logicalTopology.putNode(logical(node4));
                })
        );
    }

    private static BiConsumer<PhysicalTopologySimulator, LogicalTopologySimulator> simulation(
            String name,
            BiConsumer<PhysicalTopologySimulator, LogicalTopologySimulator> impl
    ) {
        return new BiConsumer<>() {
            @Override
            public void accept(PhysicalTopologySimulator physicalTopologySimulator, LogicalTopologySimulator logicalTopologySimulator) {
                impl.accept(physicalTopologySimulator, logicalTopologySimulator);
            }

            @Override
            public String toString() {
                return name;
            }
        };
    }

    private static ClusterNode newClusterNode() {
        return clusterNode(UUID.randomUUID());
    }

    private static ClusterNode clusterNode(UUID id) {
        return new ClusterNodeImpl(id, id.toString(), NetworkAddress.from("127.0.0.1:10200"));
    }

    private static LogicalNode logical(ClusterNode clusterNode) {
        return new LogicalNode(clusterNode, emptyMap());
    }
}
