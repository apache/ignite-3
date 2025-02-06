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

package org.apache.ignite.internal.partitiondistribution;

import static java.util.Collections.emptyList;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Stateless distribution utils that produces helper methods for an assignments distribution calculation.
 */
public class PartitionDistributionUtils {

    private static final DistributionAlgorithm DISTRIBUTION_ALGORITHM = new RendezvousDistributionFunction();

    /**
     * Calculates assignments distribution.
     *
     * @param dataNodes Data nodes.
     * @param partitions Partitions count.
     * @param replicas Replicas count.
     * @return List assignments by partition.
     */
    // TODO https://issues.apache.org/jira/browse/IGNITE-24391 pass the consensus group size as the parameter here
    public static List<Set<Assignment>> calculateAssignments(
            Collection<String> dataNodes,
            int partitions,
            int replicas
    ) {
        return DISTRIBUTION_ALGORITHM.assignPartitions(
                dataNodes,
                emptyList(),
                partitions,
                replicas,
                replicas
        );
    }

    /**
     * Calculates assignments distribution for a single partition.
     *
     * @param dataNodes Data nodes.
     * @param partitionId Partition id.
     * @param partitions Partitions count.
     * @param replicas Replicas count.
     * @return Set of assignments.
     */
    // TODO https://issues.apache.org/jira/browse/IGNITE-24391 pass the consensus group size as the parameter here
    public static Set<Assignment> calculateAssignmentForPartition(
            Collection<String> dataNodes,
            int partitionId,
            int partitions,
            int replicas
    ) {
        List<Set<Assignment>> assignments = DISTRIBUTION_ALGORITHM.assignPartitions(dataNodes, emptyList(), partitions, replicas, replicas);

        return assignments.get(partitionId);
    }

}
