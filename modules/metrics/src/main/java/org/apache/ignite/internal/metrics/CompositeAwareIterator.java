/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.metrics;

import java.util.Iterator;

/**
 * The iterator that considers composite metrics as a group of scalar ones, see {@link CompositeMetric#asScalarMetrics()},
 * and enumerates composite metrics in order to enumerate every scalar metric.
 */
public class CompositeAwareIterator implements Iterator<Metric> {
    private final Iterator<Metric> iterator;
    private Iterator<Metric> compositeMetricIterator = null;

    /**
     * The constructor.
     *
     * @param iterator Collection of metrics that can contain composite metrics.
     */
    public CompositeAwareIterator(Iterator<Metric> iterator) {
        this.iterator = iterator;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        if (compositeMetricIterator == null) {
            return iterator.hasNext();
        } else if (compositeMetricIterator.hasNext()) {
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
        } else if (compositeMetricIterator.hasNext()) {
            return compositeMetricIterator.next();
        } else {
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
            compositeMetricIterator = ((CompositeMetric) nextValue).asScalarMetrics().iterator();

            return compositeMetricIterator.next();
        } else {
            return nextValue;
        }
    }
}
