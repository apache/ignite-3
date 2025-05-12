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

package org.apache.ignite.migrationtools.tests.utils;

import org.apache.ignite.migrationtools.tests.clusters.FullSampleCluster;
import org.apache.ignite.migrationtools.tests.containers.Ignite2ClusterContainer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Testcontainers;

/** RunIgnite2Cluster. */
@ExtendWith(FullSampleCluster.class)
@Testcontainers
public class RunIgnite2Cluster {
    private final Ignite2ClusterContainer cluster = new Ignite2ClusterContainer(
            FullSampleCluster.CLUSTER_CFG_PATH,
            FullSampleCluster.TEST_CLUSTER_PATH,
            FullSampleCluster.clusterNodeIds
    );

    @Test
    void runIgnite2ContainerWithSamples() {
        try (cluster) {
            cluster.start();
            System.out.println("Cluster started. Feel free to pause and debug.");
        }
    }
}
