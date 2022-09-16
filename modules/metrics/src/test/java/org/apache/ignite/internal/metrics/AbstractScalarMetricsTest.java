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

import org.junit.jupiter.api.Test;

/**
 * Abstract test for scalar metrics.
 *
 * @param <M> Metric type.
 * @param <V> Type of metric value.
 */
public abstract class AbstractScalarMetricsTest<M extends Metric, V> {
    @Test
    public void testCreateMetric() {
        String name = "testName";
        String description = "testDescription";

        M m = createMetric(name, description);

        assertEquals(name, m.name());
        assertEquals(description, m.description());

        assertEquals(expected(), value(m));
        assertEquals(expected().toString(), value(m).toString());
    }

    @Test
    public void testIncrement() {
        M m = createMetric();

        increment(m);
        assertEquals(expected(), value(m));
        assertEquals(expected().toString(), value(m).toString());
    }

    @Test
    public void testDecrement() {
        M m = createMetric();

        decrement(m);
        assertEquals(expected(), value(m));
        assertEquals(expected().toString(), value(m).toString());
    }

    @Test
    public void testAdd() {
        M m = createMetric();

        add(m);
        assertEquals(expected(), value(m));
        assertEquals(expected().toString(), value(m).toString());
    }

    @Test
    public void testSetValue() {
        M m = createMetric();

        setValue(m);
        assertEquals(expected(), value(m));
        assertEquals(expected().toString(), value(m).toString());
    }

    /**
     * Create a metric.
     *
     * @return Metric.
     */
    private M createMetric() {
        String name = "testName";
        String description = "testDescription";

        return createMetric(name, description);
    }

    /**
     * Create a metric with given name and description.
     *
     * @param name Name.
     * @param description Description.
     * @return Metric.
     */
    protected abstract M createMetric(String name, String description);

    /**
     * Return a value of the metric.
     *
     * @param metric Metric.
     * @return Value of the metric.
     */
    protected abstract V value(M metric);

    /**
     * Expected value of the metric in current moment of time.
     *
     * @return Expected value.
     */
    protected abstract V expected();

    /**
     * Increments the metric, if applicable.
     *
     * @param metric Metric.
     */
    protected abstract void increment(M metric);

    /**
     * Decrements the metric, if applicable.
     *
     * @param metric Metric.
     */
    protected abstract void decrement(M metric);

    /**
     * Add some test value to the metric, if applicable.
     *
     * @param metric Metric.
     */
    protected abstract void add(M metric);

    /**
     * Assign some test value to the metric, if applicable.
     *
     * @param metric Metric.
     */
    protected abstract void setValue(M metric);
}

