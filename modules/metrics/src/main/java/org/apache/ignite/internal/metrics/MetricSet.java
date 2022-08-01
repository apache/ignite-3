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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import org.apache.ignite.internal.metrics.composite.CompositeMetric;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * The Metric set that consists of set of metrics identified by a metric name.
 * The metrics set is immutable.
 */
public class MetricSet implements Iterable<Metric> {
    /** Metrics set name. */
    private final String name;

    /** Registered metrics. */
    private final Map<String, Metric> metrics;

    /**
     * Creates an instance of a metrics set with given name and metrics.
     *
     * @param name Metrics set name.
     * @param metrics Metrics.
     */
    public MetricSet(String name, Map<String, Metric> metrics) {
        this.name = name;
        this.metrics = Collections.unmodifiableMap(metrics);
    }

    /**
     * Get metric by name.
     *
     * @param name Metric name.
     * @return Metric.
     */
    @Nullable
    @TestOnly
    public <M extends Metric> M get(String name) {
        return (M) metrics.get(name);
    }

    /** {@inheritDoc} */
    public Iterator<Metric> iterator() {
        return metrics.values().iterator();
    }

    /**
     * The iterator that considers composite metrics as a group of scalar ones, see {@link CompositeMetric#asScalarMetrics()},
     * and enumerates composite metrics in order to enumerate every scalar metric.
     *
     * @return Iterator.
     */
    public Iterator<Metric> scalarMetricsIterator() {
        return new CompositeAwareIterator(metrics.values());
    }

    /** @return Name of the metrics set. */
    public String name() {
        return name;
    }

    /**
     * The iterator for metric set, aware of composite metrics, see {@link #scalarMetricsIterator()}.
     */
    private static class CompositeAwareIterator implements Iterator<Metric> {
        private final Iterator<Metric> iterator;
        private Iterator<Metric> compositeMetricIterator = null;

        /**
         * The constructor.
         *
         * @param metrics Collection of metrics that can contain composite metrics.
         */
        public CompositeAwareIterator(Collection<Metric> metrics) {
             iterator = metrics.iterator();
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            if (compositeMetricIterator == null) {
                return iterator.hasNext();
            }
            else if (compositeMetricIterator.hasNext()) {
                return true;
            } else {
                compositeMetricIterator = null;

                return iterator.hasNext();
            }
        }

        /** {@inheritDoc} */
        @Override public Metric next() {
            if (compositeMetricIterator == null) {
                return nextCompositeAware();
            }
            else if (compositeMetricIterator.hasNext()) {
                return compositeMetricIterator.next();
            }
            else {
                compositeMetricIterator = null;

                return nextCompositeAware();
            }
        }

        /**
         * Next method that is aware of composite metrics.
         *
         * @return Next value.
         */
        private Metric nextCompositeAware() {
            Metric nextValue = iterator.next();

            if (nextValue instanceof CompositeMetric) {
                compositeMetricIterator = ((CompositeMetric)nextValue).asScalarMetrics().iterator();

                return compositeMetricIterator.next();
            }
            else {
                return nextValue;
            }
        }
    }
}
