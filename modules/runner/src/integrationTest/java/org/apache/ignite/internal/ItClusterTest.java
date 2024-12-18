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

package org.apache.ignite.internal;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests for the {@link Cluster} class.
 */
public class ItClusterTest extends IgniteAbstractTest {
    private Cluster cluster;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        ClusterConfiguration clusterConfiguration = ClusterConfiguration.builder(testInfo, workDir).build();

        cluster = new Cluster(clusterConfiguration);
    }

    @AfterEach
    void tearDown() {
        cluster.shutdown();
    }

    @Test
    void noRunningNodesAfterClusterIsStopped() {
        int nodeCount = 2;

        cluster.startAndInit(nodeCount);

        List<Ignite> runningNodes = cluster.runningNodes().collect(toList());

        assertThat(runningNodes, hasSize(nodeCount));

        cluster.shutdown();

        runningNodes = cluster.runningNodes().collect(toList());

        assertThat(runningNodes, is(empty()));
    }

    @Test
    void isAbleToStartMultipleClusters(TestInfo testInfo) {
        cluster.startAndInit(1);

        ClusterConfiguration clusterConfiguration = ClusterConfiguration.builder(testInfo, workDir)
                .clusterName("secondCluster")
                .basePort(20000)
                .baseHttpPort(20001)
                .baseHttpsPort(20002)
                .baseClientPort(20003)
                .build();

        var secondCluster = new Cluster(clusterConfiguration);

        try {
            secondCluster.startAndInit(1);

            assertThat(cluster.runningNodes().collect(toList()), hasSize(1));
            assertThat(secondCluster.runningNodes().collect(toList()), hasSize(1));
        } finally {
            secondCluster.shutdown();
        }
    }
}
