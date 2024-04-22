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

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.DoubleFunction;
import org.jetbrains.annotations.Nullable;

/**
 * The metric calculates the average value for the last several operations.
 */
public class MovingAverageMetric extends AbstractMetric implements DoubleMetric {
    /** Default rate. */
    public static int DFLT_ITEMS = 100;

    /** Size. */
    private final int items;

    /** Function to format a readable value. */
    private final DoubleFunction<String> stringFormatter;

    /** Elements. */
    ConcurrentLinkedDeque<Double> queue = new ConcurrentLinkedDeque<>();

    /**
     * The constructor.
     *
     * @param name Name.
     * @param desc Description.
     * @param stringFormatter String formatter to get a readable value.
     */
    public MovingAverageMetric(String name, @Nullable String desc, @Nullable DoubleFunction<String> stringFormatter) {
        this(name, desc, stringFormatter, DFLT_ITEMS);
    }

    /**
     * The constructor.
     *
     * @param name Name.
     * @param desc Description.
     * @param stringFormatter String formatter to get a readable value.
     * @param items Quantity items to calculate average value.
     */
    public MovingAverageMetric(String name, @Nullable String desc, DoubleFunction<String> stringFormatter, int items) {
        super(name, desc);

        this.stringFormatter = stringFormatter;
        this.items = items;
    }

    /**
     * Adds some value.
     *
     * @param val Value.
     */
    public void add(double val) {
        queue.add(val);

        while (queue.size() > items) {
            queue.pop();
        }
    }

    /** {@inheritDoc} */
    @Override
    public double value() {
        return queue.stream().mapToDouble(a -> a).average().orElse(0.);
    }

    @Override
    public String getValueAsString() {
        if (stringFormatter == null) {
            return Double.toString(value());
        }

        return stringFormatter.apply(value());
    }
}
