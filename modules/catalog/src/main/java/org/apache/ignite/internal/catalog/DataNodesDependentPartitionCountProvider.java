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

import org.apache.ignite.internal.system.CpuInformationProvider;

/**
 * Primary to use partition count provider. It calculates the number of partitions using the formula:
 * dataNodesCount * max(cores, 8) * scaleFactor / replicas. Data nodes count is the estimated number of data nodes for the given
 * distribution zone. It is assumed that each node has the same number of CPU cores. Also, there is doubling multiplier to allow the cluster
 * scale up.
 */
public class DataNodesDependentPartitionCountProvider implements PartitionCountProvider {
    static final int SCALE_FACTOR = 3;

    static final int MINIMUM_CPU_COUNT = 8;

    private final EstimatedDataNodeCountProvider estimatedDataNodesCountProvider;

    private final CpuInformationProvider cpuInfoProvider;

    /**
     * Constructor.
     *
     * @param estimatedDataNodesCountProvider Provides estimated data nodes count based on given zone filter and storage profile list.
     * @param cpuInfoProvider Provides CPU information for local node hardware cores calculation.
     */
    public DataNodesDependentPartitionCountProvider(
            EstimatedDataNodeCountProvider estimatedDataNodesCountProvider,
            CpuInformationProvider cpuInfoProvider
    ) {
        this.estimatedDataNodesCountProvider = estimatedDataNodesCountProvider;
        this.cpuInfoProvider = cpuInfoProvider;
    }

    @Override
    public int calculate(PartitionCountCalculationParameters params) {
        int dataNodesCount = estimatedDataNodesCountProvider.estimatedDataNodeCount(
                params.dataNodesFilter(),
                params.storageProfiles()
        );
        int cores = max(cpuInfoProvider.availableProcessors(), MINIMUM_CPU_COUNT);
        int replicas = params.replicaFactor();

        return dataNodesCount * cores * SCALE_FACTOR / replicas;
    }
}
