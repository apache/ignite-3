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

import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.metrics.MetricProvider;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.exporters.configuration.ExporterView;

/**
 * Base class for new metrics exporters implementations.
 */
public abstract class BasicMetricExporter<CfgT extends ExporterView> implements MetricExporter<CfgT> {
    /** Metrics provider. */
    private MetricProvider metricsProvider;

    private Supplier<UUID> clusterIdSupplier;

    private String nodeName;

    /** Exporter's configuration view. */
    private CfgT configuration;

    @Override
    public synchronized void start(MetricProvider metricsProvider, CfgT configuration, Supplier<UUID> clusterIdSupplier, String nodeName) {
        this.metricsProvider = metricsProvider;
        this.configuration = configuration;
        this.clusterIdSupplier = clusterIdSupplier;
        this.nodeName = nodeName;
    }

    @Override
    public synchronized void reconfigure(CfgT newVal) {
        configuration = newVal;
    }

    /**
     * Returns current exporter configuration.
     *
     * @return Current exporter configuration
     */
    protected synchronized CfgT configuration() {
        return configuration;
    }

    /**
     * Returns a map of (metricSetName -> metricSet) pairs with available metrics.
     *
     * @return map of metrics
     */
    protected final synchronized IgniteBiTuple<Map<String, MetricSet>, Long> metrics() {
        return metricsProvider.metrics();
    }

    /**
     * Returns current cluster ID.
     */
    protected final synchronized UUID clusterId() {
        return clusterIdSupplier.get();
    }

    /**
     * Returns the network alias of the node.
     */
    protected final synchronized String nodeName() {
        return nodeName;
    }
}
