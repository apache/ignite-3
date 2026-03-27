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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SimpleMetricSource} and {@link SimpleMetricSourceImpl}.
 */
public class SimpleMetricSourceTest {
    private static final String SOURCE_NAME = "test.source";

    private static SimpleMetricSourceImpl createSource() {
        return new SimpleMetricSourceImpl(SOURCE_NAME);
    }

    private static SimpleMetricSourceImpl createEnabledSource() {
        SimpleMetricSourceImpl source = createSource();
        source.enable();
        return source;
    }

    // -- Constructor and properties --

    @Test
    void properties() {
        SimpleMetricSourceImpl source = new SimpleMetricSourceImpl("name", "desc", "group");

        assertEquals("name", source.name());
        assertEquals("desc", source.description());
        assertEquals("group", source.group());
    }

    @Test
    void optionalPropertiesDefaultToNull() {
        SimpleMetricSourceImpl source = new SimpleMetricSourceImpl("name");

        assertNull(source.description());
        assertNull(source.group());
    }

    @Test
    void nullNameThrows() {
        assertThrows(NullPointerException.class, () -> new SimpleMetricSourceImpl(null));
    }

    // -- Factory methods --

    @Test
    void factoryMethodsCreateAllMetricTypes() {
        SimpleMetricSourceImpl source = createEnabledSource();

        assertNotNull(source.atomicInt("ai", null));
        assertNotNull(source.atomicLong("al", null));
        assertNotNull(source.longAdder("la", null));
        assertNotNull(source.atomicDouble("ad", null));
        assertNotNull(source.doubleAdder("da", null));
        assertNotNull(source.intGauge("ig", null, () -> 0));
        assertNotNull(source.longGauge("lg", null, () -> 0L));
        assertNotNull(source.doubleGauge("dg", null, () -> 0.0));
        assertNotNull(source.hitRate("hr", null, 1000));
        assertNotNull(source.hitRate("hr2", null, 1000, 5));
        assertNotNull(source.distribution("dist", null, new long[]{10, 100}));
    }

    @Test
    void registerReturnsSameInstance() {
        SimpleMetricSourceImpl source = createSource();
        StringGauge gauge = new StringGauge("id", null, () -> "val");

        assertSame(gauge, source.register(gauge));
    }

    @Test
    void duplicateMetricNameThrows() {
        SimpleMetricSourceImpl source = createSource();
        source.atomicLong("Counter", null);

        IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                () -> source.atomicLong("Counter", null)
        );

        assertTrue(ex.getMessage().contains(SOURCE_NAME));
        assertTrue(ex.getMessage().contains("Counter"));
    }

    @Test
    void nullMetricThrows() {
        assertThrows(NullPointerException.class, () -> createSource().register(null));
    }

    // -- Enable/disable lifecycle --

    @Test
    void notEnabledByDefault() {
        assertFalse(createSource().enabled());
    }

    @Test
    void enableReturnsMetricSet() {
        SimpleMetricSourceImpl source = createSource();
        LongAdderMetric counter = source.longAdder("Counter", null);

        MetricSet metricSet = source.enable();

        assertNotNull(metricSet);
        assertTrue(source.enabled());
        assertSame(counter, metricSet.get("Counter"));
    }

    @Test
    void secondEnableReturnsNull() {
        SimpleMetricSourceImpl source = createSource();
        source.enable();

        assertNull(source.enable());
    }

    @Test
    void disableWorks() {
        SimpleMetricSourceImpl source = createSource();
        source.enable();
        source.disable();

        assertFalse(source.enabled());
    }

    @Test
    void disableWhenAlreadyDisabledIsNoOp() {
        SimpleMetricSourceImpl source = createSource();
        source.disable();

        assertFalse(source.enabled());
    }

    @Test
    void enableEmptySource() {
        MetricSet metricSet = createEnabledSource().enable();

        // Already enabled, returns null.
        assertNull(metricSet);
    }

    // -- Enabled guard behavior --

    @Test
    void cheapMetricsAlwaysRecord() {
        SimpleMetricSourceImpl source = createSource();

        LongAdderMetric counter = source.longAdder("Counter", null);
        AtomicIntMetric intCounter = source.atomicInt("IntCounter", null);

        // Cheap metrics record even when source is disabled — no guard overhead.
        assertFalse(source.enabled());

        counter.increment();
        intCounter.increment();

        assertEquals(1L, counter.value());
        assertEquals(1, intCounter.value());
    }

    @Test
    void expensiveMetricsAreGuarded() {
        SimpleMetricSourceImpl source = createSource();

        DistributionMetric dist = source.distribution("Dist", null, new long[]{10});
        HitRateMetric hitRate = source.hitRate("Rate", null, 60_000);

        // Expensive metrics are no-ops when source is disabled.
        assertFalse(source.enabled());

        dist.add(5);
        hitRate.increment();

        assertEquals(0L, dist.value()[0]);

        // After enable, they record.
        source.enable();

        dist.add(5);
        hitRate.increment();

        assertEquals(1L, dist.value()[0]);
    }

    @Test
    void expensiveMetricsStopAfterDisable() {
        SimpleMetricSourceImpl source = createEnabledSource();

        DistributionMetric dist = source.distribution("Dist", null, new long[]{10});
        dist.add(5);

        assertEquals(1L, dist.value()[0]);

        source.disable();
        dist.add(5);

        assertEquals(1L, dist.value()[0]);
    }

    @Test
    void gaugesWorkRegardlessOfEnabledState() {
        SimpleMetricSourceImpl source = createSource();
        int[] holder = {42};

        IntGauge gauge = source.intGauge("Gauge", null, () -> holder[0]);

        assertEquals(42, gauge.value());

        source.enable();
        holder[0] = 99;

        assertEquals(99, gauge.value());
    }

    // -- Value persistence across cycles --

    @Test
    void valuesPreservedAcrossEnableDisableCycle() {
        SimpleMetricSourceImpl source = createEnabledSource();

        LongAdderMetric counter = source.longAdder("Counter", null);
        counter.add(100);

        source.disable();
        MetricSet secondSet = source.enable();

        assertSame(counter, secondSet.get("Counter"));
        assertEquals(100L, counter.value());
    }

    // -- Post-enable metric addition --

    @Test
    void metricAddedAfterEnableIsFunctional() {
        SimpleMetricSourceImpl source = createEnabledSource();

        AtomicLongMetric late = source.atomicLong("Late", null);
        late.increment();

        assertTrue(source.enabled());
        assertEquals(1L, late.value());
    }

    // -- Registry integration --

    @Test
    void worksWithMetricRegistry() {
        MetricRegistry registry = new MetricRegistry();
        SimpleMetricSourceImpl source = createSource();
        LongAdderMetric counter = source.longAdder("Requests", null);

        registry.registerSource(source);
        MetricSet metricSet = registry.enable(SOURCE_NAME);

        assertNotNull(metricSet);
        assertSame(counter, metricSet.get("Requests"));

        counter.increment();
        assertEquals(1L, ((LongAdderMetric) metricSet.get("Requests")).value());
    }

    @Test
    void registryDisableAndReEnable() {
        MetricRegistry registry = new MetricRegistry();
        SimpleMetricSourceImpl source = createSource();
        LongAdderMetric counter = source.longAdder("Requests", null);

        registry.registerSource(source);
        registry.enable(SOURCE_NAME);
        counter.add(10);

        registry.disable(SOURCE_NAME);
        assertFalse(source.enabled());

        MetricSet reEnabled = registry.enable(source);
        assertNotNull(reEnabled);
        assertEquals(10L, ((LongAdderMetric) reEnabled.get("Requests")).value());
    }
}
