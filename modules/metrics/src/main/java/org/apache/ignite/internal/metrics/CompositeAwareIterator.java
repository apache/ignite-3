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

import java.util.Iterator;

/**
 * The iterator that considers composite metrics as a group of scalar ones, see {@link CompositeMetric#asScalarMetrics()},
 * and enumerates composite metrics in order to enumerate every scalar metric.
 */
public class CompositeAwareIterator implements Iterator<Metric> {
    /** Delegate iterator of metrics, some of them may be composite. */
    private final Iterator<Metric> delegate;

    /** Iterator for single composite metric, iterating over the group of scalar ones. */
    private Iterator<Metric> compositeMetricIterator = null;

    /**
     * The constructor.
     *
     * @param delegate Collection of metrics that can contain composite metrics.
     */
    public CompositeAwareIterator(Iterator<Metric> delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        if (compositeMetricIterator == null) {
            return delegate.hasNext();
        } else if (compositeMetricIterator.hasNext()) {
            return true;
        } else {
            compositeMetricIterator = null;

            return delegate.hasNext();
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
        Metric nextValue = delegate.next();

        if (nextValue instanceof CompositeMetric) {
            compositeMetricIterator = ((CompositeMetric) nextValue).asScalarMetrics().iterator();

            return compositeMetricIterator.next();
        } else {
            return nextValue;
        }
    }
}
