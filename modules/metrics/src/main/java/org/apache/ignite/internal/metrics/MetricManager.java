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

import java.util.Map;

import java.util.List;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.internal.metrics.exporters.MetricExporter;
import org.jetbrains.annotations.NotNull;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * Metric manager.
 */
public class MetricManager implements IgniteComponent {
    /**
     * Metric registry.
     */
    private final MetricRegistry registry;

    /** Metrics' exporters. */
    private List<MetricExporter> metricExporters;

    /**
     * Constructor.
     */
    public MetricManager() {
        this.registry = new MetricRegistry();
    }

    /** {@inheritDoc} */
    @Override public void start() {
        // TODO: IGNITE-17358 not all exporters should be started, it must be defined by configuration
        metricExporters = loadExporters();

        MetricProvider metricsProvider = new MetricProvider(registry);

        for (MetricExporter metricExporter : metricExporters) {
            metricExporter.init(metricsProvider);

            metricExporter.start();
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws Exception {
        for (MetricExporter metricExporter : metricExporters) {
            metricExporter.stop();
        }
    }

    /**
     * Register metric source. See {@link MetricRegistry#registerSource(MetricSource)}.
     *
     * @param src Metric source.
     */
    public void registerSource(MetricSource src) {
        registry.registerSource(src);
    }

    /**
     * Unregister metric source. See {@link MetricRegistry#unregisterSource(MetricSource)}.
     *
     * @param src Metric source.
     */
    public void unregisterSource(MetricSource src) {
        registry.unregisterSource(src);
    }

    /**
     * Unregister metric source by name. See {@link MetricRegistry#unregisterSource(String)}.
     *
     * @param srcName Metric source name.
     */
    public void unregisterSource(String srcName) {
        registry.unregisterSource(srcName);
    }

    /**
     * Enable metric source. See {@link MetricRegistry#enable(MetricSource)}.
     *
     * @param src Metric source.
     * @return Metric set, or {@code null} if already enabled.
     */
    public MetricSet enable(MetricSource src) {
        return registry.enable(src);
    }

    /**
     * Enable metric source by name. See {@link MetricRegistry#enable(String)}.
     *
     * @param srcName Source name.
     * @return Metric set, or {@code null} if already enabled.
     */
    public MetricSet enable(final String srcName) {
        return registry.enable(srcName);
    }

    /**
     * Load exporters by {@link ServiceLoader} mechanism.
     *
     * @return list of loaded exporters.
     */
    private List<MetricExporter> loadExporters() {
        var clsLdr = Thread.currentThread().getContextClassLoader();

        return ServiceLoader
                .load(MetricExporter.class, clsLdr)
                .stream()
                .map(Provider::get)
                .collect(toUnmodifiableList());
    }

    /**
     * Disable metric source. See {@link MetricRegistry#disable(MetricSource)}.
     *
     * @param src Metric source.
     */
    public void disable(MetricSource src) {
        registry.disable(src);
    }

    /**
     * Disable metric source by name. See {@link MetricRegistry#disable(String)}.
     *
     * @param srcName Source name.
     */
    public void disable(final String srcName) {
        registry.disable(srcName);
    }

    /**
     * Metrics snapshot. This is a snapshot of metric sets with corresponding version, the values of the metrics in the
     * metric sets that are included into the snapshot, are changed dynamically.
     *
     * @return Metrics snapshot.
     */
    @NotNull
    public IgniteBiTuple<Map<String, MetricSet>, Long> metricSnapshot() {
        return registry.metricSnapshot();
    }
}
