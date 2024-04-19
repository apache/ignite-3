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

import static java.lang.Math.abs;
import static java.util.Collections.emptySet;
import static java.util.Comparator.comparing;
import static java.util.Objects.hash;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.affinity.AffinityUtils.calculateAssignments;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class NextCandidateTest {
    private TopologyTracker topologyTracker;

    private Map<String, ClusterNode> clusterNodes;

    List<Set<Assignment>> assignments;

    Set<String> offlineNodes;

    private @Nullable ClusterNode nextLeaseHolder(
            Set<Assignment> assignments,
            ReplicationGroupId grpId,
            @Nullable String proposedConsistentId
    ) {
        // TODO: IGNITE-18879 Implement more intellectual algorithm to choose a node.
        if (proposedConsistentId != null) {
            ClusterNode proposedCandidate = topologyTracker.nodeByConsistentId(proposedConsistentId);

            if (proposedCandidate != null) {
                return proposedCandidate;
            }
        }

        List<ClusterNode> onlineNodes = assignments.stream()
                .map(a -> topologyTracker.nodeByConsistentId(a.consistentId()))
                .filter(Objects::nonNull)
                .sorted(comparing(ClusterNode::name))
                .collect(toList());

        int hash = abs(hash(assignments, grpId));

        return onlineNodes.get(hash % onlineNodes.size());
    }

    @BeforeEach
    public void setUp() {
        topologyTracker = mock(TopologyTracker.class);
        when(topologyTracker.nodeByConsistentId(anyString())).thenAnswer(inv -> {
            String name = inv.getArgument(0);
            return offlineNodes.contains(name) ? null : clusterNodes.get(name);
        });
    }

    private void prepareCluster(int nodes, int partitions, int replicas, Set<String> offlineNodes) {
        this.offlineNodes = offlineNodes;

        Set<String> dataNodes = IntStream.range(0, nodes).mapToObj(i -> "node" + i).collect(toSet());
        clusterNodes = dataNodes.stream()
                .map(n -> new ClusterNodeImpl(UUID.randomUUID().toString(), n, mock(NetworkAddress.class)))
                .collect(toMap(n -> n.name(), n -> n));

        assignments = calculateAssignments(dataNodes, partitions, replicas);
    }

    @Test
    public void testReplicaFactorEqualsNodeCount() {
        int nodes = 3;
        int partitions = 5;
        int replicas = 3;

        prepareCluster(nodes, partitions, replicas, emptySet());

        Set<ClusterNode> candidates = new HashSet<>();

        for (int p = 0; p < partitions; p++) {
            candidates.add(nextLeaseHolder(assignments.get(p), new TablePartitionId(8, p), null));
        }

        assertEquals(clusterNodes.size(), candidates.size());
    }

    @Test
    public void testReplicaFactorLessThanNodeCount() {
        int nodes = 3;
        int partitions = 20;
        int replicas = 2;

        prepareCluster(nodes, partitions, replicas, emptySet());

        Set<ClusterNode> candidates = new HashSet<>();

        for (int p = 0; p < partitions; p++) {
            candidates.add(nextLeaseHolder(assignments.get(p), new TablePartitionId(8, p), null));
        }

        assertEquals(clusterNodes.size(), candidates.size());
    }

    @Test
    public void testProlongationOnSameLease() {
        int nodes = 3;
        int partitions = 5;
        int replicas = 2;

        prepareCluster(nodes, partitions, replicas, emptySet());

        Map<TablePartitionId, ClusterNode> leasesMap = new HashMap<>();

        for (int p = 0; p < partitions; p++) {
            TablePartitionId partitionId = new TablePartitionId(8, p);
            leasesMap.put(partitionId, nextLeaseHolder(assignments.get(p), partitionId, null));
        }

        // Prolongation.
        Map<TablePartitionId, ClusterNode> prolongedLeasesMap = new HashMap<>();
        for (int p = 0; p < partitions; p++) {
            TablePartitionId partitionId = new TablePartitionId(8, p);

            prolongedLeasesMap.put(partitionId, nextLeaseHolder(assignments.get(p), partitionId, leasesMap.get(partitionId).name()));
        }

        assertEquals(leasesMap.size(), prolongedLeasesMap.size());
        for (TablePartitionId partId : leasesMap.keySet()) {
            assertEquals(leasesMap.get(partId), prolongedLeasesMap.get(partId));
        }
    }

    @Test
    public void testLeasesDoNotMoveAfterNodeJoin() {
        int nodes = 3;
        int partitions = 5;
        int replicas = 2;

        String offlineNode = "node0";

        prepareCluster(nodes, partitions, replicas, Set.of(offlineNode));

        assertTrue(clusterNodes.keySet().contains(offlineNode));

        Map<TablePartitionId, ClusterNode> leasesMap = new HashMap<>();

        for (int p = 0; p < partitions; p++) {
            TablePartitionId partitionId = new TablePartitionId(8, p);
            leasesMap.put(partitionId, nextLeaseHolder(assignments.get(p), partitionId, null));
        }

        // Node joins.
        offlineNodes = emptySet();

        // Prolongation.
        Map<TablePartitionId, ClusterNode> prolongedLeasesMap = new HashMap<>();
        for (int p = 0; p < partitions; p++) {
            TablePartitionId partitionId = new TablePartitionId(8, p);

            prolongedLeasesMap.put(partitionId, nextLeaseHolder(assignments.get(p), partitionId, leasesMap.get(partitionId).name()));
        }

        assertEquals(leasesMap.size(), prolongedLeasesMap.size());
        for (TablePartitionId partId : leasesMap.keySet()) {
            assertEquals(leasesMap.get(partId), prolongedLeasesMap.get(partId));
        }
    }

    @Test
    public void testUnaffectedLeasesDoNotMoveAfterNodeLeft() {
        int nodes = 3;
        int partitions = 5;
        int replicas = 2;

        prepareCluster(nodes, partitions, replicas, emptySet());

        Map<TablePartitionId, ClusterNode> leasesMap = new HashMap<>();

        for (int p = 0; p < partitions; p++) {
            TablePartitionId partitionId = new TablePartitionId(8, p);
            leasesMap.put(partitionId, nextLeaseHolder(assignments.get(p), partitionId, null));
        }

        // Node leaves.
        String offlineNode = clusterNodes.keySet().stream().findFirst().orElseThrow();
        offlineNodes = Set.of(offlineNode);

        Set<TablePartitionId> leasesThatShouldBeMoved = leasesMap.keySet()
                .stream()
                .filter(t -> leasesMap.get(t).name().equals(offlineNode))
                .collect(toSet());

        // Prolongation.
        Map<TablePartitionId, ClusterNode> prolongedLeasesMap = new HashMap<>();
        for (int p = 0; p < partitions; p++) {
            TablePartitionId partitionId = new TablePartitionId(8, p);

            prolongedLeasesMap.put(partitionId, nextLeaseHolder(assignments.get(p), partitionId, leasesMap.get(partitionId).name()));
        }

        assertEquals(leasesMap.size(), prolongedLeasesMap.size());
        for (TablePartitionId partId : leasesMap.keySet()) {
            if (leasesThatShouldBeMoved.contains(partId)) {
                assertFalse(prolongedLeasesMap.get(partId).name().equals(offlineNode));
            } else {
                assertEquals(leasesMap.get(partId), prolongedLeasesMap.get(partId));
            }
        }
    }
}
