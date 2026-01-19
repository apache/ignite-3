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

package org.apache.ignite.internal.catalog;

import static java.lang.Math.max;

/**
 * Primary to use partition count provider. It calculates the number of partitions using the formula:
 * dataNodesCount * max(cores, 8) * scaleFactor / replicas. Data nodes count is the estimated number of data nodes for the given
 * distribution zone. It is assumed that each node has the same number of CPU cores. Also, there is doubling multiplier to allow the cluster
 * scale up.
 */
public class DataNodesDependentPartitionCountProvider implements PartitionCountProvider {
    private static final int SCALE_FACTOR = 3;

    private final EstimatedDataNodesNumberProvider estimatedDataNodesCountProvider;

    public DataNodesDependentPartitionCountProvider(EstimatedDataNodesNumberProvider estimatedDataNodesCountProvider) {
        this.estimatedDataNodesCountProvider = estimatedDataNodesCountProvider;
    }

    @Override
    public int calculate(PartitionCountCalculationParameters params) {
        int dataNodesCount = estimatedDataNodesCountProvider.estimatedDataNodesNumber(
                params.dataNodesFilter(),
                params.storageProfiles()
        );
        int cores = max(Runtime.getRuntime().availableProcessors(), 8);
        int replicas = params.replicaFactor();

        return dataNodesCount * cores * SCALE_FACTOR / replicas;
    }
}
