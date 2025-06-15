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

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.jetbrains.annotations.Nullable;

/**
 * Double metric based on atomic updater of double value.
 */
public class AtomicDoubleMetric extends AbstractMetric implements DoubleMetric {
    /** Field updater. */
    private static final AtomicLongFieldUpdater<AtomicDoubleMetric> updater =
            AtomicLongFieldUpdater.newUpdater(AtomicDoubleMetric.class, "val");

    /** Value. */
    private volatile long val;

    /**
     * Constructor.
     *
     * @param name Name.
     * @param desc Description.
     */
    public AtomicDoubleMetric(String name, @Nullable String desc) {
        super(name, desc);
    }

    /**
     * Adds given value to the metric value.
     *
     * @param v Value to be added.
     */
    public void add(double v) {
        for (;;) {
            long exp = val;
            long upd = Double.doubleToLongBits(Double.longBitsToDouble(exp) + v);
            if (updater.compareAndSet(this, exp, upd)) {
                break;
            }
        }
    }

    /**
     * Sets value.
     *
     * @param val Value.
     */
    public void value(double val) {
        this.val = Double.doubleToLongBits(val);
    }

    /** {@inheritDoc} */
    @Override
    public double value() {
        return Double.longBitsToDouble(val);
    }
}
