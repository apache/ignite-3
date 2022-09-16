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
 * Long metric implementation.
 */
public class AtomicLongMetric extends AbstractMetric implements LongMetric {
    /** Field updater. */
    static final AtomicLongFieldUpdater<AtomicLongMetric> updater =
            AtomicLongFieldUpdater.newUpdater(AtomicLongMetric.class, "val");

    /** Field value. */
    private volatile long val;

    /**
     * Constructor.
     *
     * @param name Name.
     * @param desc Description.
     */
    public AtomicLongMetric(String name, @Nullable String desc) {
        super(name, desc);
    }

    /**
     * Adds x to the metric.
     *
     * @param x Value to be added.
     */
    public void add(long x) {
        updater.getAndAdd(this, x);
    }

    /** Adds 1 to the metric. */
    public void increment() {
        updater.incrementAndGet(this);
    }

    /** Adds -1 to the metric. */
    public void decrement() {
        updater.decrementAndGet(this);
    }

    /** {@inheritDoc} */
    @Override public long value() {
        return val;
    }

    /**
     * Sets value.
     *
     * @param val Value.
     */
    public void value(long val) {
        this.val = val;
    }
}
