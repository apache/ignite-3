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

package org.apache.ignite.raft;

import static java.util.List.of;
import static org.apache.ignite.internal.ClusterPerTestIntegrationTest.aggressiveLowWatermarkIncreaseClusterConfig;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.metrics.IntMetric;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.sources.RaftMetricSource;
import org.junit.jupiter.api.Test;

public class ItRaftMetricTest extends ClusterPerClassIntegrationTest {
    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration(aggressiveLowWatermarkIncreaseClusterConfig());
    }

    @Test
    void testLeaderCount() {
        testMetricChangeAfterOperation(of(RaftMetricSource.RAFT_GROUP_LEADERS), of((long) DEFAULT_PARTITION_COUNT), () -> {
            sql("CREATE ZONE TEST_ZONE WITH STORAGE_PROFILES='default'");
            sql("CREATE TABLE TEST (id INT PRIMARY KEY, val VARCHAR) ZONE TEST_ZONE");
        });

        testMetricChangeAfterOperation(of(RaftMetricSource.RAFT_GROUP_LEADERS), of((long) -DEFAULT_PARTITION_COUNT), () -> {
            int initialNodes = getRaftNodesCount();

            sql("DROP TABLE TEST");
            sql("DROP ZONE TEST_ZONE");

            // Waiting for zone partitions to be destroyed.
            await().until(() -> initialNodes - getRaftNodesCount() >= DEFAULT_PARTITION_COUNT);
        });
    }

    private static int getRaftNodesCount() {
        return CLUSTER.runningNodes()
                .mapToInt(node -> unwrapIgniteImpl(node).raftManager().localNodes().size())
                .sum();
    }

    private void testMetricChangeAfterOperation(
            List<String> metricNames,
            List<Long> expectedValues,
            Runnable op
    ) {
        Map<String, Long> before = metricValues(metricNames);

        op.run();

        Map<String, Long> after = metricValues(metricNames);

        for (int i = 0; i < metricNames.size(); i++) {
            String metricName = metricNames.get(i);
            long expected = expectedValues.get(i);
            long actual = after.get(metricName) - before.get(metricName);

            assertThat("Metric " + metricName + " value mismatch", actual, is(expected));
        }
    }

    /**
     * Returns the sum of the specified metrics on all nodes.
     *
     * @param metricNames Metric names.
     * @return Map of metric names to their values.
     */
    private Map<String, Long> metricValues(List<String> metricNames) {
        Map<String, Long> values = new HashMap<>(metricNames.size());

        for (int i = 0; i < initialNodes(); ++i) {
            MetricSet metricSet = unwrapIgniteImpl(CLUSTER.node(i)).metricManager().metricSnapshot().metrics()
                    .get(RaftMetricSource.SOURCE_NAME);

            assertThat(metricSet, is(notNullValue()));

            for (String metricName : metricNames) {
                Metric metric = metricSet.get(metricName);

                assertThat(metric, is(notNullValue()));

                if (metric instanceof IntMetric) {
                    values.merge(metricName, (long) ((IntMetric) metric).value(), Long::sum);
                } else {
                    assertThat(metric, is(instanceOf(LongMetric.class)));
                    values.merge(metricName, ((LongMetric) metric).value(), Long::sum);

                }
            }
        }

        return values;
    }
}
