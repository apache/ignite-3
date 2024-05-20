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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricManagerImpl;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.metrics.sources.OsMetricSource;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
class ItOsMetricSourceTest extends BaseIgniteAbstractTest {
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
    void testOsMetrics() {
        MetricManager metricManager = new MetricManagerImpl();
        metricManager.configure(simpleConfiguration);

        Map<String, MetricExporter> exporters = new HashMap<>();
        TestSimpleExporter simpleExporter = new TestSimpleExporter();
        exporters.put(simpleExporter.name(), simpleExporter);

        metricManager.registerSource(new OsMetricSource());

        metricManager.start(exporters);

        metricManager.enable("os");

        Map<String, String> osMetrics = simpleExporter.pull().get("os");

        assertPositiveDoubleValue(osMetrics.get("LoadAverage"));

        assertThat(metricManager.stopAsync(ForkJoinPool.commonPool()), willCompleteSuccessfully());
    }

    private static void assertPositiveDoubleValue(String metric) {
        assertThat(Double.parseDouble(metric), greaterThan(0.0));
    }
}
