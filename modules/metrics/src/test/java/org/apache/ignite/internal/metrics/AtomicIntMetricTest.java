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

public class AtomicIntMetricTest extends MetricsTest<AtomicIntMetric, Integer> {
    @Override protected AtomicIntMetric createMetric(String name, String description) {
        return new AtomicIntMetric(name, description);
    }

    @Override protected Integer value(AtomicIntMetric metric) {
        return metric.value();
    }

    @Override protected Integer expected() {
        return null;
    }

    @Override protected void increment(AtomicIntMetric metric) {
        metric.increment();
    }

    @Override protected void add(AtomicIntMetric metric, Integer value) {
        metric.add(value);
    }

    @Override protected void setValue(AtomicIntMetric metric, Integer value) {
        metric.value(value);
    }

    @Override protected Integer temp() {
        return null;
    }
}
