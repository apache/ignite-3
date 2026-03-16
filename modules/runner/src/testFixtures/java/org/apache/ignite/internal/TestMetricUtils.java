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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.metrics.IntMetric;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;

/** Utility class for testing metrics. */
public class TestMetricUtils {
    /**
     * Tests that the given operation increases the specified metrics by the expected values.
     *
     * @param cluster Cluster instance.
     * @param sourceName Name of the source to get metric set from.
     * @param metricNames Metric names to be checked.
     * @param expectedValues Expected values to increase the metrics.
     * @param op Operation to be executed.
     */
    public static void testMetricChangeAfterOperation(
            Cluster cluster,
            String sourceName,
            List<String> metricNames,
            List<Long> expectedValues,
            Runnable op
    ) {
        Map<String, Long> before = metricValues(cluster, sourceName, metricNames);

        op.run();

        Map<String, Long> after = metricValues(cluster, sourceName, metricNames);

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
    public static Map<String, Long> metricValues(Cluster cluster, String sourceName, List<String> metricNames) {
        Map<String, Long> values = new HashMap<>(metricNames.size());

        for (String metricName : metricNames) {
            values.put(metricName, metricValue(cluster, sourceName, metricName));
        }

        return values;
    }

    /**
     * Returns the sum of the specified metric on all nodes.
     *
     * @param metricName Metric names.
     */
    public static long metricValue(Cluster cluster, String sourceName, String metricName) {
        long result = 0;

        for (int i = 0; i < cluster.runningNodes().count(); i++) {
            MetricSet metricSet = unwrapIgniteImpl(cluster.node(i)).metricManager().metricSnapshot().metrics()
                    .get(sourceName);

            assertThat(metricSet, is(notNullValue()));

            Metric metric = metricSet.get(metricName);

            assertThat(metric, is(notNullValue()));

            if (metric instanceof IntMetric) {
                result += ((IntMetric) metric).value();
            } else if (metric instanceof LongMetric) {
                result += ((LongMetric) metric).value();
            } else {
                fail("Not a LongMetric / IntMetric [name=" + metricName + ", class=" + metric.getClass().getSimpleName() + ']');
            }
        }

        return result;
    }
}
