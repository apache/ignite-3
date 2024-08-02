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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.metrics.configuration.MetricView;
import org.apache.ignite.internal.metrics.exporters.MetricExporter;
import org.apache.ignite.internal.metrics.exporters.configuration.ExporterView;
import org.jetbrains.annotations.VisibleForTesting;


/**
 * Metric manager.
 */
public class MetricManagerImpl implements MetricManager {
    /** Logger. */
    private final IgniteLogger log;

    /** Metric registry. */
    private final MetricRegistry registry;

    private final MetricProvider metricsProvider;

    private final Map<String, MetricExporter> enabledMetricExporters = new ConcurrentHashMap<>();

    /** Metrics' exporters. */
    private Map<String, MetricExporter> availableExporters;

    private MetricConfiguration metricConfiguration;

    /**
     * Constructor.
     */
    public MetricManagerImpl() {
        this(Loggers.forClass(MetricManagerImpl.class));
    }

    /**
     * Constructor.
     *
     * @param log Logger.
     */
    public MetricManagerImpl(IgniteLogger log) {
        registry = new MetricRegistry();
        metricsProvider = new MetricProvider(registry);
        this.log = log;
    }

    @Override
    public void configure(MetricConfiguration metricConfiguration) {
        assert this.metricConfiguration == null : "Metric manager must be configured only once, on the start of the node";

        this.metricConfiguration = metricConfiguration;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        start(loadExporters());

        return nullCompletedFuture();
    }

    @Override
    @VisibleForTesting
    public void start(Map<String, MetricExporter> availableExporters) {
        this.availableExporters = availableExporters;

        MetricView conf = metricConfiguration.value();

        for (ExporterView exporter : conf.exporters()) {
            checkAndStartExporter(exporter.exporterName(), exporter);
        }

        metricConfiguration.exporters().listenElements(new ExporterConfigurationListener());
    }

    @Override
    public void start(Iterable<MetricExporter<?>> exporters) {
        this.availableExporters = new HashMap<>();

        for (MetricExporter<?> exporter : exporters) {
            exporter.start(metricsProvider, null);

            availableExporters.put(exporter.name(), exporter);
            enabledMetricExporters.put(exporter.name(), exporter);
        }
    }

    @Override public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        for (MetricExporter metricExporter : enabledMetricExporters.values()) {
            metricExporter.stop();
        }

        enabledMetricExporters.clear();

        return nullCompletedFuture();
    }

    @Override
    public void registerSource(MetricSource src) {
        registry.registerSource(src);
    }

    @Override
    public void unregisterSource(MetricSource src) {
        registry.unregisterSource(src);
    }

    @Override
    public void unregisterSource(String srcName) {
        registry.unregisterSource(srcName);
    }

    @Override
    public MetricSet enable(MetricSource src) {
        MetricSet enabled = registry.enable(src);

        if (enabled != null) {
            enabledMetricExporters.values().forEach(e -> e.addMetricSet(enabled));
        }

        return enabled;
    }

    @Override
    public MetricSet enable(final String srcName) {
        MetricSet enabled = registry.enable(srcName);

        if (enabled != null) {
            enabledMetricExporters.values().forEach(e -> e.addMetricSet(enabled));
        }

        return enabled;
    }

    @Override
    public void disable(MetricSource src) {
        registry.disable(src);

        enabledMetricExporters.values().forEach(e -> e.removeMetricSet(src.name()));
    }

    @Override
    public void disable(final String srcName) {
        registry.disable(srcName);

        enabledMetricExporters.values().forEach(e -> e.removeMetricSet(srcName));
    }

    @Override
    public IgniteBiTuple<Map<String, MetricSet>, Long> metricSnapshot() {
        return registry.metricSnapshot();
    }

    @Override
    public Collection<MetricSource> metricSources() {
        return registry.metricSources();
    }

    private <T extends ExporterView> void checkAndStartExporter(
            String exporterName,
            T exporterConfiguration) {
        MetricExporter<T> exporter = availableExporters.get(exporterName);

        if (exporter != null) {
            enabledMetricExporters.computeIfAbsent(exporter.name(), name -> {
                try {
                    exporter.start(metricsProvider, exporterConfiguration);

                    return exporter;
                } catch (Exception e) {
                    log.warn("Unable to start metrics exporter name=[" + exporterName + "].", e);

                    return null;
                }
            });
        } else {
            log.warn("Received configuration for unknown metric exporter with the name '" + exporterName + "'");
        }
    }

    /**
     * Load exporters by {@link ServiceLoader} mechanism.
     *
     * @return list of loaded exporters.
     */
    public static Map<String, MetricExporter> loadExporters() {
        var clsLdr = Thread.currentThread().getContextClassLoader();

        return ServiceLoader
                .load(MetricExporter.class, clsLdr)
                .stream()
                .map(Provider::get)
                .collect(Collectors.toMap(e -> e.name(), Function.identity()));
    }

    private class ExporterConfigurationListener implements ConfigurationNamedListListener<ExporterView> {
        @Override
        public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ExporterView> ctx) {
            checkAndStartExporter(ctx.newValue().exporterName(), ctx.newValue());

            return nullCompletedFuture();
        }

        @Override
        public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ExporterView> ctx) {
            var removed = enabledMetricExporters.remove(ctx.oldValue().exporterName());

            if (removed != null) {
                removed.stop();
            }

            return nullCompletedFuture();
        }

        @Override
        public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ExporterView> ctx) {
            MetricExporter exporter = enabledMetricExporters.get(ctx.newValue().exporterName());

            if (exporter != null) {
                exporter.reconfigure(ctx.newValue());
            }

            return nullCompletedFuture();
        }
    }
}
