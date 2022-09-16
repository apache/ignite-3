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

import java.util.concurrent.atomic.LongAdder;
import org.jetbrains.annotations.Nullable;

/**
 * Long metric based on {@link LongAdder}.
 */
public class LongAdderMetric extends AbstractMetric implements LongMetric {
    /** Value. */
    private volatile LongAdder val;

    /**
     * Constructor.
     *
     * @param name Name.
     * @param desc Description.
     */
    public LongAdderMetric(String name, @Nullable String desc) {
        super(name, desc);

        this.val = new LongAdder();
    }

    /**
     * Adds x to the metric.
     *
     * @param x Value to be added.
     */
    public void add(long x) {
        val.add(x);
    }

    /**
     * Increment the metric.
     */
    public void increment() {
        val.increment();
    }

    /**
     * Decrement the metric.
     */
    public void decrement() {
        val.decrement();
    }

    /**
     * Sets value.
     *
     * @param val Value.
     */
    public void value(long val) {
        LongAdder adder = new LongAdder();
        adder.add(val);
        this.val = adder;
    }

    /** {@inheritDoc} */
    @Override public long value() {
        return val.sum();
    }

}
