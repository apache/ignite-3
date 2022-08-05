/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for metric entities, such as {@link MetricRegistry} and {@link MetricSet}.
 */
public class MetricEntitiesTest {
    private static final String SOURCE_NAME = "testSource";
    private static final String SCALAR_METRIC_NAME = "testScalarMetric";
    private static final String COMPOSITE_METRIC_NAME = "testCompositeMetric";
    private static final long[] DISTRIBUTION_BOUNDS = new long[] { 10, 100, 1000 };

    @Test
    public void testMetricRegistry() {
        MetricRegistry registry = new MetricRegistry();

        MetricSource metricSource = new TestMetricSource();

        registry.registerSource(metricSource);

        assertThrows(IllegalStateException.class, () -> registry.registerSource(metricSource));

        MetricSet metricSet = registry.enable(SOURCE_NAME);
        assertThrows(IllegalStateException.class, () -> registry.enable("unexisting"));

        assertEquals(2L, registry.version());
        assertNotNull(metricSet);

        assertNull(registry.enable(SOURCE_NAME));
        assertEquals(2L, registry.version());

        registry.disable(SOURCE_NAME);
        assertEquals(3L, registry.version());

        assertThrows(IllegalStateException.class, () -> registry.disable("unexisting"));
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
        assertEquals("0_" + DISTRIBUTION_BOUNDS[0], scalarMetrics.get(1).name());
        assertEquals(DISTRIBUTION_BOUNDS[0] + "_" + DISTRIBUTION_BOUNDS[1], scalarMetrics.get(2).name());
        assertEquals(DISTRIBUTION_BOUNDS[1] + "_" + DISTRIBUTION_BOUNDS[2], scalarMetrics.get(3).name());
        assertEquals(DISTRIBUTION_BOUNDS[2] + "_", scalarMetrics.get(4).name());

        registry.disable(metricSource.name());

        assertNull(metricSource.holder());
    }

    private static class TestMetricSource extends AbstractMetricSource<TestMetricSource.Holder> {
        protected TestMetricSource() {
            super(SOURCE_NAME);
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
