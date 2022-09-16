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

/**
 * Abstract test for integer metrics.
 */
public abstract class AbstractIntMetricTest extends AbstractScalarMetricsTest<IntMetric, Integer> {
    /** Test value. */
    private static final int TEST_VALUE = 100;

    /** Expected value. */
    private int expected = 0;

    /** {@inheritDoc} */
    @Override protected Integer value(IntMetric metric) {
        return metric.value();
    }

    /** {@inheritDoc} */
    @Override protected Integer expected() {
        return expected;
    }

    /** {@inheritDoc} */
    @Override protected void increment(IntMetric metric) {
        expected++;
        increment0(metric);
    }

    /**
     * Increment the metric.
     *
     * @param metric Metric.
     */
    protected abstract void increment0(IntMetric metric);

    /** {@inheritDoc} */
    @Override protected void decrement(IntMetric metric) {
        expected--;
        decrement0(metric);
    }

    /**
     * Decrement the metric.
     *
     * @param metric Metric.
     */
    protected abstract void decrement0(IntMetric metric);

    /** {@inheritDoc} */
    @Override protected void add(IntMetric metric) {
        expected += TEST_VALUE;
        add0(metric, TEST_VALUE);
    }

    /**
     * Add the value to the metric.
     *
     * @param metric Metric.
     */
    protected abstract void add0(IntMetric metric, int value);

    /** {@inheritDoc} */
    @Override protected void setValue(IntMetric metric) {
        expected = TEST_VALUE;
        setValue0(metric, TEST_VALUE);
    }

    /**
     * Assign the value to the metric.
     *
     * @param metric Metric.
     */
    protected abstract void setValue0(IntMetric metric, int value);
}
