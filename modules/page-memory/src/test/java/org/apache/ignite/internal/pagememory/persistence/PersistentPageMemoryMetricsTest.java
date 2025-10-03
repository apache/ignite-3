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

package org.apache.ignite.internal.pagememory.persistence;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.TestMetricManager;
import org.apache.ignite.internal.pagememory.configuration.PersistentDataRegionConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** For {@link PersistentPageMemoryMetrics} testing. */
public class PersistentPageMemoryMetricsTest extends BaseIgniteAbstractTest {
    private final PersistentPageMemoryMetricSource metricSource = new PersistentPageMemoryMetricSource("test");

    private final PersistentPageMemory pageMemory = mock(PersistentPageMemory.class);

    private final PersistentDataRegionConfiguration dataRegionConfig = PersistentDataRegionConfiguration.builder().size(100).build();

    private final PersistentPageMemoryMetrics metrics = new PersistentPageMemoryMetrics(metricSource, pageMemory, dataRegionConfig);

    private final MetricManager metricManager = new TestMetricManager();

    @BeforeEach
    void setUp() {
        metricManager.registerSource(metricSource);
        metricManager.enable(metricSource);

        assertThat(metricManager.startAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() {
        metricManager.beforeNodeStop();

        assertThat(metricManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @Test
    void testMaxSize() {
        checkMetricValue("MaxSize", Long.toString(dataRegionConfig.sizeBytes()));
    }

    @Test
    void testUsedCheckpointBufferPages() {
        when(pageMemory.usedCheckpointBufferPages()).thenReturn(42);

        checkMetricValue("UsedCheckpointBufferPages", "42");
    }

    @Test
    void testMaxCheckpointBufferPages() {
        when(pageMemory.maxCheckpointBufferPages()).thenReturn(84);

        checkMetricValue("MaxCheckpointBufferPages", "84");
    }

    @Test
    void testPagesRead() {
        checkMetricValue("PagesRead", "0");

        metrics.incrementReadFromDiskMetric();
        metrics.incrementReadFromDiskMetric();

        checkMetricValue("PagesRead", "2");
    }

    @Test
    void testPagesWritten() {
        checkMetricValue("PagesWritten", "0");

        metrics.incrementWriteToDiskMetric();
        metrics.incrementWriteToDiskMetric();
        metrics.incrementWriteToDiskMetric();

        checkMetricValue("PagesWritten", "3");
    }

    private void checkMetricValue(String metricName, String exp) {
        MetricSet metricsSet = metricManager.metricSnapshot().metrics().get(metricSource.name());

        Metric metric = metricsSet.get(metricName);

        assertNotNull(metric, metricName);

        assertEquals(exp, metric.getValueAsString(), metricName);
    }
}
