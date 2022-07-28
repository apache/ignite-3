/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class MetricsTest<T extends Metric, S> {
    @Test
    public void metricTest() {
        String name = "testName";
        String description = "testDescription";

        T m = createMetric(name, description);

        assertEquals(name, m.name());
        assertEquals(description, m.description());

        assertEquals(expected(), value(m));

        increment(m);
        assertEquals(expected(), value(m));

        add(m, temp());
        assertEquals(expected(), value(m));

        setValue(m, temp());
        assertEquals(expected(), value(m));
    }

    protected abstract T createMetric(String name, String description);

    protected abstract S value(T metric);

    protected abstract S expected();

    protected abstract void increment(T metric);

    protected abstract void add(T metric, S value);

    protected abstract void setValue(T metric, S value);

    protected abstract S temp();
}

