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

package org.apache.ignite.internal.metrics;

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.junit.jupiter.api.Test;

/**
 * Tests for metric entities, such as {@link MetricRegistry} and {@link MetricSet}.
 */
public class MetricEntitiesTest {
    private static final String SOURCE_NAME = "testSource";
    private static final String SCALAR_METRIC_NAME = "TestScalarMetric";
    private static final String COMPOSITE_METRIC_NAME = "TestCompositeMetric";
    private static final long[] DISTRIBUTION_BOUNDS = new long[] { 10, 100, 1000 };

    @Test
    public void testMetricLifecycle() {
        MetricRegistry registry = new MetricRegistry();

        MetricSource metricSource = new TestMetricSource();

        registry.registerSource(metricSource);
        assertEquals(0L, registry.metricSnapshot().get2());

        assertThrows(IllegalStateException.class, () -> registry.registerSource(metricSource));

        assertEquals(0L, registry.metricSnapshot().get2());
        assertTrue(registry.metricSnapshot().get1().isEmpty());

        MetricSource alreadyEnabled = new TestMetricSource("alreadyEnabled");
        alreadyEnabled.enable();
        assertThrows(AssertionError.class, () -> registry.registerSource(alreadyEnabled));
        assertEquals(0L, registry.metricSnapshot().get2());

        // Enabling metric source, metric snapshot and its version should be changed.
        MetricSet metricSet = registry.enable(SOURCE_NAME);
        assertNotNull(metricSet);
        assertEquals(1L, registry.metricSnapshot().get2());
        assertFalse(registry.metricSnapshot().get1().isEmpty());
        assertNull(registry.enable(metricSource));

        assertThrows(IllegalStateException.class, () -> registry.enable("unexisting"));
        assertEquals(1L, registry.metricSnapshot().get2());

        // Enabling the metric source that was already enabled before, metric snapshot should not be changed.
        assertNull(registry.enable(SOURCE_NAME));
        IgniteBiTuple<Map<String, MetricSet>, Long> metricSnapshot = registry.metricSnapshot();
        assertEquals(1L, metricSnapshot.get2());
        assertFalse(metricSnapshot.get1().isEmpty());
        MetricSet ms = metricSnapshot.get1().get(SOURCE_NAME);
        assertEquals(metricSet, ms);

        // Disable the metric source.
        registry.disable(SOURCE_NAME);
        assertEquals(2L, registry.metricSnapshot().get2());

        // Disable unexisting metric source, exception is thrown, metric snapshot should not be changed.
        assertThrows(IllegalStateException.class, () -> registry.disable("unexisting"));
        metricSnapshot = registry.metricSnapshot();
        assertEquals(2L, metricSnapshot.get2());
        assertTrue(metricSnapshot.get1().isEmpty());

        // Trying to disable the metric source that was already disabled before, metric snapshot should not be changed.
        registry.disable(SOURCE_NAME);
        assertEquals(2L, registry.metricSnapshot().get2());
        registry.disable(metricSource);
        assertEquals(2L, registry.metricSnapshot().get2());

        // Enabling metric source again, metric snapshot changes.
        registry.enable(metricSource);
        assertEquals(3L, registry.metricSnapshot().get2());
        assertFalse(registry.metricSnapshot().get1().isEmpty());

        // Unregister enabled metric source, it should be disabled, metric snapshot should be changed.
        registry.unregisterSource(metricSource);
        assertEquals(4L, registry.metricSnapshot().get2());
        assertTrue(registry.metricSnapshot().get1().isEmpty());

        // Trying to unregister the metric source that was already unregistered before, metric snapshot should not be changed.
        assertThrows(IllegalStateException.class, () -> registry.unregisterSource(metricSource));
        metricSnapshot = registry.metricSnapshot();
        assertEquals(4L, metricSnapshot.get2());
        assertTrue(metricSnapshot.get1().isEmpty());
    }

    @Test
    public void testMetricSet() {
        MetricRegistry registry = new MetricRegistry();

        TestMetricSource metricSource = new TestMetricSource();

        registry.registerSource(metricSource);

        assertNull(metricSource.holder());

        MetricSet metricSet = registry.enable(metricSource.name());

        TestMetricSource.Holder holder = metricSource.holder();

        assertNotNull(holder);

        assertTrue(metricSet.get(SCALAR_METRIC_NAME) instanceof IntMetric);
        assertTrue(metricSet.get(COMPOSITE_METRIC_NAME) instanceof DistributionMetric);

        List<Metric> metrics = stream(spliteratorUnknownSize(metricSet.iterator(), 0), false).collect(toList());
        assertEquals(2, metrics.size());

        assertEquals(SCALAR_METRIC_NAME, holder.atomicIntMetric.name());
        assertEquals(COMPOSITE_METRIC_NAME, holder.distributionMetric.name());

        List<Metric> scalarMetrics = stream(spliteratorUnknownSize(new CompositeAwareIterator(metrics.iterator()), 0), false)
                .collect(toList());

        assertEquals(2 + DISTRIBUTION_BOUNDS.length, scalarMetrics.size());

        assertEquals(SCALAR_METRIC_NAME, scalarMetrics.get(0).name());
        assertEquals(COMPOSITE_METRIC_NAME + "_0_" + DISTRIBUTION_BOUNDS[0], scalarMetrics.get(1).name());
        assertEquals(COMPOSITE_METRIC_NAME + '_' + DISTRIBUTION_BOUNDS[0] + "_" + DISTRIBUTION_BOUNDS[1], scalarMetrics.get(2).name());
        assertEquals(COMPOSITE_METRIC_NAME + '_' + DISTRIBUTION_BOUNDS[1] + "_" + DISTRIBUTION_BOUNDS[2], scalarMetrics.get(3).name());
        assertEquals(COMPOSITE_METRIC_NAME + '_' + DISTRIBUTION_BOUNDS[2] + "_inf", scalarMetrics.get(4).name());

        registry.disable(metricSource.name());

        assertNull(metricSource.holder());
    }

    private static class TestMetricSource extends AbstractMetricSource<TestMetricSource.Holder> {
        protected TestMetricSource() {
            super(SOURCE_NAME);
        }


        protected TestMetricSource(String name) {
            super(name);
        }

        @Override protected Holder createHolder() {
            return new Holder();
        }

        @Override protected void init(MetricSetBuilder bldr, Holder holder) {
            bldr.register(holder.atomicIntMetric);
            bldr.register(holder.distributionMetric);
        }

        private static class Holder implements AbstractMetricSource.Holder<Holder> {
            final AtomicIntMetric atomicIntMetric = new AtomicIntMetric(SCALAR_METRIC_NAME, null);
            final DistributionMetric distributionMetric = new DistributionMetric(COMPOSITE_METRIC_NAME, null, DISTRIBUTION_BOUNDS);
        }
    }
}
