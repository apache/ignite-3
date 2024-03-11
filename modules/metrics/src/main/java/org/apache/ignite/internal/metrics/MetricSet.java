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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
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
    @Override
    public Iterator<Metric> iterator() {
        return metrics.values().iterator();
    }

    /**
     * Name of the metric set.
     *
     * @return Name of the metrics set.
     */
    public String name() {
        return name;
    }
}
