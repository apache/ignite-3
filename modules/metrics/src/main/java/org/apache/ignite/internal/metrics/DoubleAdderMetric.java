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

import java.util.concurrent.atomic.DoubleAdder;
import org.jetbrains.annotations.Nullable;

/**
 * Double metric based on {@link DoubleAdder}.
 */
public class DoubleAdderMetric extends AbstractMetric implements DoubleMetric {
    /** Value. */
    private volatile DoubleAdder val;

    /**
     * Constructor.
     *
     * @param name Name.
     * @param desc Description.
     */
    public DoubleAdderMetric(String name, @Nullable String desc) {
        super(name, desc);

        this.val = new DoubleAdder();
    }

    /**
     * Adds x to the metric.
     *
     * @param x Value to be added.
     */
    public void add(double x) {
        val.add(x);
    }

    /**
     * Sets value.
     *
     * @param val Value.
     */
    public void value(double val) {
        DoubleAdder adder = new DoubleAdder();
        adder.add(val);
        this.val = adder;
    }

    /** {@inheritDoc} */
    @Override public double value() {
        return val.sum();
    }
}
