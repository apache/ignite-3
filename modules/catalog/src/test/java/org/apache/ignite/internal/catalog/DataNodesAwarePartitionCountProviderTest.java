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

import static org.apache.ignite.internal.catalog.DataNodesAwarePartitionCountProvider.MINIMUM_CPU_COUNT;
import static org.apache.ignite.internal.catalog.DataNodesAwarePartitionCountProvider.SCALE_FACTOR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

import org.apache.ignite.internal.system.CpuInformationProvider;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DataNodesAwarePartitionCountProvider}.
 */
public class DataNodesAwarePartitionCountProviderTest extends BaseIgniteAbstractTest {
    @Test
    void cpuCountLessThatMinimumTest() {
        EstimatedDataNodeCountProvider minPossibleDataNodeCountProvider = (f, sp) -> 1;

        CpuInformationProvider minPossibleCpuInfoProvider = new CpuInformationProvider() {
            @Override
            public int availableProcessors() {
                return 1;
            }
        };

        int minReplicaCount = 1;

        DataNodesAwarePartitionCountProvider provider = new DataNodesAwarePartitionCountProvider(
                minPossibleDataNodeCountProvider,
                minPossibleCpuInfoProvider
        );

        PartitionCountCalculationParameters params = PartitionCountCalculationParameters.builder()
                .replicaFactor(minReplicaCount)
                .build();

        assertThat(provider.calculate(params), is(SCALE_FACTOR * MINIMUM_CPU_COUNT));
    }

    @Test
    void maxValuesDoesNotLeadToOverflowTest() {
        // We may assume that at the time maximum data nodes count doesnt' exceed 200_000: RIKEN Fugaku cluster consists of 158_976 nodes.
        int maxDataNodesCount = 200_000;
        EstimatedDataNodeCountProvider maxPossibleDataNodeCountProvider = (f, sp) -> maxDataNodesCount;

        // We may assume that at the time maximum CPU count doesn't exceed 1000: SYS-681E-TR server with 8490H may hosts up to 480 CPUs.
        int maxCpuCount = 1000;
        CpuInformationProvider maxPossibleCpuInfoProvider = new CpuInformationProvider() {
            @Override
            public int availableProcessors() {
                return maxCpuCount;
            }
        };

        // Replica factor is the divider in the formula.
        int minReplicaCount = 1;

        DataNodesAwarePartitionCountProvider provider = new DataNodesAwarePartitionCountProvider(
                maxPossibleDataNodeCountProvider,
                maxPossibleCpuInfoProvider
        );

        PartitionCountCalculationParameters params = PartitionCountCalculationParameters.builder()
                .replicaFactor(minReplicaCount)
                .build();

        assertThat(provider.calculate(params), is(greaterThan(0)));
        // We check only overflow case, but the value may be more than CatalogUtils.MAX_PARTITION_COUNT, but it's ok and proper validators
        // will catch this case during catalog command processing. But we must not get overflow exception during the calculation.
        assertThat(provider.calculate(params), is(lessThan(Integer.MAX_VALUE)));
    }
}
