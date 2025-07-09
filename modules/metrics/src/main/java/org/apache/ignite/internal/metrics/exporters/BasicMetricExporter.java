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

import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.internal.metrics.MetricProvider;
import org.apache.ignite.internal.metrics.MetricSnapshot;
import org.apache.ignite.internal.metrics.exporters.configuration.ExporterView;

/**
 * Base class for new metrics exporters implementations.
 */
public abstract class BasicMetricExporter implements MetricExporter {
    /** Metrics provider. */
    private volatile MetricProvider metricsProvider;

    private volatile Supplier<UUID> clusterIdSupplier;

    private volatile String nodeName;

    @Override
    public void start(
            MetricProvider metricsProvider,
            ExporterView configuration,
            Supplier<UUID> clusterIdSupplier,
            String nodeName
    ) {
        this.metricsProvider = metricsProvider;
        this.clusterIdSupplier = clusterIdSupplier;
        this.nodeName = nodeName;
    }

    /**
     * Returns a map of (metricSetName -> metricSet) pairs with available metrics.
     *
     * @return map of metrics
     */
    protected final MetricSnapshot snapshot() {
        return metricsProvider.snapshot();
    }

    /**
     * Returns current cluster ID.
     */
    protected final Supplier<UUID> clusterIdSupplier() {
        return clusterIdSupplier;
    }

    /**
     * Returns the network alias of the node.
     */
    protected final String nodeName() {
        return nodeName;
    }
}
