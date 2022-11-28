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

package org.apache.ignite.internal.affinity;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.network.ClusterNode;

/**
 * Stateless affinity utils that produces helper methods for an affinity assignments calculation.
 */
public class AffinityUtils {
    /**
     * Calculates affinity assignments.
     *
     * @param partitions Partitions count.
     * @param replicas Replicas count.
     * @return List assignments by partition.
     */
    public static List<Set<Assignment>> calculateAssignments(Collection<ClusterNode> baselineNodes, int partitions, int replicas) {
        List<Set<ClusterNode>> affinityNodes = RendezvousAffinityFunction.assignPartitions(
                baselineNodes,
                partitions,
                replicas,
                false,
                null,
                HashSet::new
        );

        return affinityNodes.stream().map(AffinityUtils::clusterNodesToAssignments).collect(toList());
    }

    /**
     * Calculates affinity assignments for a single partition.
     *
     * @param baselineNodes Nodes.
     * @param partition Partition id.
     * @param replicas Replicas count.
     * @return List of assignments.
     */
    public static Set<Assignment> calculateAssignmentForPartition(Collection<ClusterNode> baselineNodes, int partition, int replicas) {
        Set<ClusterNode> affinityNodes = RendezvousAffinityFunction.assignPartition(
                partition,
                new ArrayList<>(baselineNodes),
                replicas,
                null,
                false,
                null,
                HashSet::new
        );

        return clusterNodesToAssignments(affinityNodes);
    }

    private static Set<Assignment> clusterNodesToAssignments(Collection<ClusterNode> nodes) {
        return nodes.stream().map(node -> Assignment.forPeer(node.name())).collect(toSet());
    }
}
