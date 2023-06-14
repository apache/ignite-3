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

/**
 * Stateless affinity utils that produces helper methods for an affinity assignments calculation.
 */
public class AffinityUtils {
    /**
     * Calculates affinity assignments.
     *
     * @param dataNodes Data nodes.
     * @param partitions Partitions count.
     * @param replicas Replicas count.
     * @return List assignments by partition.
     */
    public static List<Set<Assignment>> calculateAssignments(Collection<String> dataNodes, int partitions, int replicas) {
        List<Set<String>> affinityNodes = RendezvousAffinityFunction.assignPartitions(
                dataNodes,
                partitions,
                replicas,
                false,
                null,
                HashSet::new
        );

        return affinityNodes.stream().map(AffinityUtils::dataNodesToAssignments).collect(toList());
    }

    /**
     * Calculates affinity assignments for a single partition.
     *
     * @param dataNodes Data nodes.
     * @param partitionId Partition id.
     * @param replicas Replicas count.
     * @return Set of assignments.
     */
    public static Set<Assignment> calculateAssignmentForPartition(Collection<String> dataNodes, int partitionId, int replicas) {
        Set<String> affinityNodes = RendezvousAffinityFunction.assignPartition(
                partitionId,
                new ArrayList<>(dataNodes),
                replicas,
                null,
                false,
                null,
                HashSet::new
        );

        return dataNodesToAssignments(affinityNodes);
    }

    private static Set<Assignment> dataNodesToAssignments(Collection<String> nodes) {
        return nodes.stream().map(Assignment::forPeer).collect(toSet());
    }
}
