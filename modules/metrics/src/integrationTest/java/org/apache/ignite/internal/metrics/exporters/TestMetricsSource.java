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

package org.apache.ignite.internal.metrics.exporters;

import java.util.List;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.AtomicIntMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.exporters.TestMetricsSource.Holder;

/** Simple test metric source. */
public class TestMetricsSource extends AbstractMetricSource<Holder> {
    /** Constructor. */
    TestMetricsSource(String name) {
        super(name);
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    void inc() {
        Holder holder = holder();

        assert holder != null;

        holder.atomicIntMetric.increment();
    }

    /** Holder class. */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final AtomicIntMetric atomicIntMetric = new AtomicIntMetric("Metric", "Metric");

        @Override
        public Iterable<Metric> metrics() {
            return List.of(atomicIntMetric);
        }
    }
}
