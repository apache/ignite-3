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

package org.apache.ignite.internal.metrics.exporters;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.metrics.sources.JvmMetricSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Suite to test, that enabled by default jvm metrics aren't broken.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItJvmMetricSourceTest {
    @InjectConfiguration(
            value = "mock.exporters = {"
                    + "simple = {exporterName = simple}"
                    + "}",
            polymorphicExtensions = {
                    TestSimpleExporterConfigurationSchema.class
            }
    )
    private MetricConfiguration simpleConfiguration;

    @Test
    public void testMemoryUsageMetric() throws Exception {
        MetricManager metricManager = new MetricManager();

        metricManager.configure(simpleConfiguration);

        Map<String, MetricExporter> exporters = new HashMap<>();

        TestSimpleExporter simpleExporter = new TestSimpleExporter();

        exporters.put(simpleExporter.name(), simpleExporter);

        metricManager.registerSource(new JvmMetricSource());

        metricManager.start(exporters);

        metricManager.enable("jvm");

        var jvmMetrics = simpleExporter.pull().get("jvm");

        assertPositiveLongValue(jvmMetrics.get("memory.heap.init"));
        assertPositiveLongValue(jvmMetrics.get("memory.heap.used"));
        assertPositiveLongValue(jvmMetrics.get("memory.heap.committed"));
        assertPositiveLongValue(jvmMetrics.get("memory.heap.max"));


        assertNotNull(jvmMetrics.get("memory.non-heap.init"));
        assertNotNull(jvmMetrics.get("memory.non-heap.used"));
        assertNotNull(jvmMetrics.get("memory.non-heap.committed"));
        assertNotNull(jvmMetrics.get("memory.non-heap.max"));

        metricManager.stop();
    }

    private void assertPositiveLongValue(String metric) {
        assertThat(Long.parseLong(metric), greaterThan(0L));
    }
}
