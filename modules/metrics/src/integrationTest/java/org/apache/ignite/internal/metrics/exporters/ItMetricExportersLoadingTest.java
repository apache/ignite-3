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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricManagerImpl;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration test for metrics' exporters loading.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItMetricExportersLoadingTest extends BaseIgniteAbstractTest {
    @InjectConfiguration(
            value = "mock.exporters = {"
                    + "testPull = {exporterName = testPull},"
                    + "testPush = {exporterName = testPush, period = 100},"
                    + "}",
            polymorphicExtensions = {
                    TestPushMetricsExporterConfigurationSchema.class,
                    TestPullMetricsExporterConfigurationSchema.class
            }
    )
    private MetricConfiguration metricConfiguration;

    @Test
    public void test() throws Exception {
        MetricManager metricManager = new MetricManagerImpl();

        metricManager.configure(metricConfiguration, UUID::randomUUID, "test-node");

        TestMetricsSource src = new TestMetricsSource("TestMetricsSource");

        metricManager.registerSource(src);

        metricManager.enable(src.name());

        try (OutputStream pullOutputStream = new ByteArrayOutputStream();
                OutputStream pushOutputStream = new ByteArrayOutputStream()) {
            TestPullMetricExporter.setOutputStream(pullOutputStream);

            TestPushMetricExporter.setOutputStream(pushOutputStream);

            assertEquals(0, pullOutputStream.toString().length());

            assertEquals(0, pushOutputStream.toString().length());

            assertThat(metricManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

            src.inc();

            waitForOutput(pushOutputStream, "TestMetricsSource:\nMetric:1");
            assertTrue(pushOutputStream.toString().contains("TestMetricsSource:\nMetric:1"));

            TestPullMetricExporter.requestMetrics();

            waitForOutput(pullOutputStream, "TestMetricsSource:\nMetric:1");
            assertTrue(pullOutputStream.toString().contains("TestMetricsSource:\nMetric:1"));

            assertThat(metricManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());
        }
    }

    @Test
    public void shouldChangePeriod() throws Exception {
        MetricManager metricManager = new MetricManagerImpl();

        metricManager.configure(metricConfiguration, UUID::randomUUID, "test-node");

        TestMetricsSource src = new TestMetricsSource("TestMetricsSource");

        metricManager.registerSource(src);

        metricManager.enable(src.name());

        try (OutputStream pushOutputStream = new ByteArrayOutputStream()) {
            TestPushMetricExporter.setOutputStream(pushOutputStream);

            assertEquals(0, pushOutputStream.toString().length());

            assertThat(metricManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

            src.inc();

            waitForOutput(pushOutputStream, "TestMetricsSource:\nMetric:1");
        }
    }

    private void waitForOutput(OutputStream outputStream, String content) {
        while (!outputStream.toString().contains(content)) {
            LockSupport.parkNanos(100_000_000);
        }
    }
}
