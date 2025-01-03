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

import java.util.Collection;
import java.util.List;

/**
 * Partition distribution algorithm.
 */
public interface DistributionAlgorithm {

    /**
     * Generates an assignment by the given parameters.
     *
     * @param nodes List of topology nodes.
     * @param currentDistribution Previous assignments or empty list.
     * @param partitions Number of table partitions.
     * @param replicaFactor Number partition replicas.
     * @return List of nodes by partition.
     */
    List<List<String>> assignPartitions(
            Collection<String> nodes,
            List<List<String>> currentDistribution,
            int partitions,
            int replicaFactor
    );

    /**
     * Generates an assignment by the given parameters for the given partition.
     *
     * @param nodes List of topology nodes.
     * @param currentDistribution Previous assignments or empty list.
     * @param partitionId Id of the partition.
     * @param replicaFactor Number partition replicas.
     * @return List of nodes for partition.
     */
    List<String> assignPartition(
            Collection<String> nodes,
            List<List<String>> currentDistribution,
            int partitionId,
            int replicaFactor
    );
}
