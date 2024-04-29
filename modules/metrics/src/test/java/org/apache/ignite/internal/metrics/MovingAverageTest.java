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

import java.util.Arrays;

/**
 * Metric to calculate moving average value.
 */
public class MovingAverageTest extends AbstractDoubleMetricTest {
    public double[] items = new double[10];

    int pos = 0;

    @Override
    protected void increment0(DoubleMetric metric) {
        double avg = getAvg();

        addValue(avg);

        ((SimpleMovingAverage) metric).add(avg);
    }

    @Override
    protected void decrement0(DoubleMetric metric) {
        addValue(0);

        ((SimpleMovingAverage) metric).add(0);
    }

    @Override
    protected void add0(DoubleMetric metric, double value) {
        addValue(value);

        ((SimpleMovingAverage) metric).add(value);
    }

    @Override
    protected void setValue0(DoubleMetric metric, double value) {
        for (int i = 0; i < items.length; i++) {
            addValue(value);

            ((SimpleMovingAverage) metric).add(value);
        }
    }

    @Override
    protected DoubleMetric createMetric(String name, String description) {
        return new SimpleMovingAverage(name, description, Double::toString, items.length);
    }

    @Override
    protected Double expected() {
        return getAvg();
    }

    /**
     * Adds a value for the proper calculation of the expected one.
     *
     * @param value Some value.
     */
    private void addValue(double value) {
        items[pos % items.length] = value;
        pos++;
    }

    /**
     * Calculated average value.
     *
     * @return Average value.
     */
    private double getAvg() {
        return pos == 0 ? 0 : Arrays.stream(items, 0, Math.min(pos, items.length)).sum() / (Math.min(pos, items.length));
    }
}
