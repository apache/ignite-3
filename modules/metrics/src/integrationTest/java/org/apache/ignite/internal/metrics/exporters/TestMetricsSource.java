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

package org.apache.ignite.internal.metrics.exporters;

import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.AtomicIntMetric;
import org.apache.ignite.internal.metrics.MetricSetBuilder;
import org.apache.ignite.internal.metrics.ThreadPoolMetricTest;
import org.apache.ignite.internal.metrics.exporters.TestMetricsSource.Holder;

/**
 * Metric source for {@link ThreadPoolMetricTest}.
 */
public class TestMetricsSource extends AbstractMetricSource<Holder> {
    private AtomicIntMetric atomicIntMetric;

    /**
     * Constructor.
     *
     * @param name Name.
     */
    public TestMetricsSource(String name) {
        super(name);
    }

    /** {@inheritDoc} */
    @Override protected Holder createHolder() {
        return new Holder();
    }

    /** {@inheritDoc} */
    @Override protected void init(MetricSetBuilder bldr, Holder holder) {
        atomicIntMetric = bldr.atomicInt("metric", "Metric");
    }

    public void inc() {
        atomicIntMetric.increment();
    }

    /**
     * Holder class.
     */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        // No-op.
    }
}
