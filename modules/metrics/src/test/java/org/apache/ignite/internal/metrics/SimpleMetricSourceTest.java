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
 * Tests for {@link SimpleMetricSource}.
 */
public class SimpleMetricSourceTest {
    private static final String SOURCE_NAME = "test.source";

    private static SimpleMetricSource createSource() {
        return new SimpleMetricSource(SOURCE_NAME, "Test source.");
    }

    private static SimpleMetricSource createEnabledSource() {
        SimpleMetricSource source = createSource();
        source.enable();
        return source;
    }

    // -- Constructor and properties --

    @Test
    void properties() {
        SimpleMetricSource source = new SimpleMetricSource("name", "desc", "group");

        assertEquals("name", source.name());
        assertEquals("desc", source.description());
        assertEquals("group", source.group());
    }

    @Test
    void groupDefaultsToNull() {
        SimpleMetricSource source = new SimpleMetricSource("name", "desc");

        assertNull(source.group());
    }

    @Test
    void nullNameThrows() {
        assertThrows(NullPointerException.class, () -> new SimpleMetricSource(null, "desc"));
    }

    @Test
    void nullDescriptionThrows() {
        assertThrows(NullPointerException.class, () -> new SimpleMetricSource("name", null));
    }

    // -- Factory methods --

    @Test
    void factoryMethodsCreateAllMetricTypes() {
        SimpleMetricSource source = createEnabledSource();

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
        SimpleMetricSource source = createSource();
        StringGauge gauge = new StringGauge("id", null, () -> "val");

        assertSame(gauge, source.register(gauge));
    }

    @Test
    void duplicateMetricNameThrows() {
        SimpleMetricSource source = createSource();
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
        SimpleMetricSource source = createSource();
        LongAdderMetric counter = source.longAdder("Counter", null);

        MetricSet metricSet = source.enable();

        assertNotNull(metricSet);
        assertTrue(source.enabled());
        assertSame(counter, metricSet.get("Counter"));
    }

    @Test
    void secondEnableReturnsNull() {
        SimpleMetricSource source = createSource();
        source.enable();

        assertNull(source.enable());
    }

    @Test
    void disableWorks() {
        SimpleMetricSource source = createSource();
        source.enable();
        source.disable();

        assertFalse(source.enabled());
    }

    @Test
    void disableWhenAlreadyDisabledIsNoOp() {
        SimpleMetricSource source = createSource();
        source.disable();

        assertFalse(source.enabled());
    }

    @Test
    void enableEmptySource() {
        MetricSet metricSet = createEnabledSource().enable();

        // Already enabled, returns null.
        assertNull(metricSet);
    }

    // -- Metrics record regardless of enabled state --

    @Test
    void metricsRecordWhenDisabled() {
        SimpleMetricSource source = createSource();

        LongAdderMetric counter = source.longAdder("Counter", null);
        AtomicIntMetric intCounter = source.atomicInt("IntCounter", null);
        DistributionMetric dist = source.distribution("Dist", null, new long[]{10});
        HitRateMetric hitRate = source.hitRate("Rate", null, 60_000);

        assertFalse(source.enabled());

        counter.increment();
        intCounter.increment();
        dist.add(5);
        hitRate.increment();

        assertEquals(1L, counter.value());
        assertEquals(1, intCounter.value());
        assertEquals(1L, dist.value()[0]);
    }

    @Test
    void gaugesWorkRegardlessOfEnabledState() {
        SimpleMetricSource source = createSource();
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
        SimpleMetricSource source = createEnabledSource();

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
        SimpleMetricSource source = createEnabledSource();

        AtomicLongMetric late = source.atomicLong("Late", null);
        late.increment();

        assertTrue(source.enabled());
        assertEquals(1L, late.value());
    }

    // -- Registry integration --

    @Test
    void worksWithMetricRegistry() {
        MetricRegistry registry = new MetricRegistry();
        SimpleMetricSource source = createSource();
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
        SimpleMetricSource source = createSource();
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
