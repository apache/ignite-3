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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricManagerImpl;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.metrics.sources.JvmMetricSource;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Suite to test, that enabled by default jvm metrics aren't broken.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItJvmMetricSourceTest extends BaseIgniteAbstractTest {
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
    public void testMemoryUsageMetric() {
        MetricManager metricManager = new MetricManagerImpl();

        metricManager.configure(simpleConfiguration);

        Map<String, MetricExporter> exporters = new HashMap<>();

        TestSimpleExporter simpleExporter = new TestSimpleExporter();

        exporters.put(simpleExporter.name(), simpleExporter);

        metricManager.registerSource(new JvmMetricSource());

        metricManager.start(exporters);

        metricManager.enable("jvm");

        var jvmMetrics = simpleExporter.pull().get("jvm");

        assertPositiveLongValue(jvmMetrics.get("memory.heap.Init"));
        assertPositiveLongValue(jvmMetrics.get("memory.heap.Used"));
        assertPositiveLongValue(jvmMetrics.get("memory.heap.Committed"));
        assertPositiveLongValue(jvmMetrics.get("memory.heap.Max"));


        assertNotNull(jvmMetrics.get("memory.non-heap.Init"));
        assertNotNull(jvmMetrics.get("memory.non-heap.Used"));
        assertNotNull(jvmMetrics.get("memory.non-heap.Committed"));
        assertNotNull(jvmMetrics.get("memory.non-heap.Max"));

        assertThat(metricManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    private void assertPositiveLongValue(String metric) {
        assertThat(Long.parseLong(metric), greaterThan(0L));
    }
}
