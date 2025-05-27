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

package org.apache.ignite.internal.cluster.management.metrics;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.junit.jupiter.api.Test;

/**
 * Tests for the topology metrics.
 */
public class ItTopologyMetricsTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    public void testLocalNodeMetrics() {
        IgniteImpl node = unwrapIgniteImpl(node(0));

        TopologyMetricsSource topologyMetricsSource = topologyMetricsSource(node.metricManager());

        assertThat(topologyMetricsSource, is(notNullValue()));

        assertThat(topologyMetricsSource.localNodeName(), is(node.clusterService().nodeName()));
        assertThat(topologyMetricsSource.localNodeId(), is(node.clusterService().topologyService().localMember().id()));
        assertThat(topologyMetricsSource.localNodeVersion(), is(IgniteProductVersion.CURRENT_VERSION.toString()));
    }

    @Test
    public void testClusterMetrics() {
        IgniteImpl node = unwrapIgniteImpl(node(0));

        TopologyMetricsSource topologyMetricsSource = topologyMetricsSource(node.metricManager());

        assertThat(topologyMetricsSource, is(notNullValue()));

        assertThat(topologyMetricsSource.clusterName(), is("cluster"));
        assertThat(topologyMetricsSource.totalNodes(), is(1));
    }

    private static TopologyMetricsSource topologyMetricsSource(MetricManager metricManager) {
        return (TopologyMetricsSource) metricManager
                .metricSources()
                .stream()
                .filter(source -> source.name().equals(TopologyMetricsSource.SOURCE_NAME))
                .findFirst()
                .orElseThrow();
    }
}
