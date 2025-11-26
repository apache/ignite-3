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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricManagerImpl;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.metrics.exporters.configuration.ExporterView;
import org.apache.ignite.internal.metrics.exporters.configuration.TestPushExporterConfigurationSchema;
import org.apache.ignite.internal.metrics.exporters.configuration.TestPushExporterView;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test for {@link PushMetricExporter}.
 */
@ExtendWith(ConfigurationExtension.class)
public class PushExporterTest extends BaseIgniteAbstractTest {
    private static final String TEST_EXPORTER_NAME = "test-push-exporter";

    @InjectConfiguration(polymorphicExtensions = TestPushExporterConfigurationSchema.class)
    private MetricConfiguration metricConfiguration;

    private MetricManager metricManager;

    private TestPushExporter exporter;

    @BeforeEach
    public void setUp() {
        metricManager = new MetricManagerImpl();

        Map<String, MetricExporter> availableExporters = new HashMap<>();

        exporter  = new TestPushExporter();

        availableExporters.put(TEST_EXPORTER_NAME, exporter);

        metricManager.configure(metricConfiguration, UUID::randomUUID, "test-node");

        metricManager.start(availableExporters);
    }

    @AfterEach
    void tearDown() {
        CompletableFuture<Void> stopFut = metricManager.stopAsync();

        assertThat(stopFut, willCompleteSuccessfully());
    }

    @Test
    public void testReconfigurePushExporterUsingTheSamePeriod() throws Exception {
        // Start exporter.
        CompletableFuture<Void> fut = metricConfiguration.exporters().change(ch -> {
            ch.create(TEST_EXPORTER_NAME, exporterChange -> exporterChange.convert(TEST_EXPORTER_NAME));
        });

        assertThat(fut, willCompleteSuccessfully());

        // Check that the exporter is running.
        assertTrue(waitForCondition(exporter.metricReported::get, 2_000));

        // Disable the exporter.
        fut = metricConfiguration.exporters().change(ch -> ch.delete(TEST_EXPORTER_NAME));

        assertThat(fut, willCompleteSuccessfully());

        assertFalse(exporter.metricReported.get());

        // Re-configure the exporter using the same period.
        fut = metricConfiguration.exporters().change(ch -> {
            ch.create(TEST_EXPORTER_NAME, exporterChange -> exporterChange.convert(TEST_EXPORTER_NAME));
        });

        assertThat(fut, willCompleteSuccessfully());

        // Check that the exporter is still running.
        assertTrue(waitForCondition(exporter.metricReported::get, 2_000));
    }

    /**
     * Test push exporter.
     */
    private static class TestPushExporter extends PushMetricExporter {
        final AtomicBoolean metricReported = new AtomicBoolean(false);

        @Override
        protected long period(ExporterView exporterView) {
            return ((TestPushExporterView) exporterView).periodMillis();
        }

        @Override
        public void report() {
            metricReported.set(true);
        }

        @Override
        public String name() {
            return TEST_EXPORTER_NAME;
        }

        @Override
        public void stop() {
            super.stop();

            metricReported.set(false);
        }
    }
}
