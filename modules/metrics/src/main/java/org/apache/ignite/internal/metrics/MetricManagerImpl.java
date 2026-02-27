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

import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockSafe;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.metrics.exporters.MetricExporter;
import org.apache.ignite.internal.metrics.exporters.configuration.ExporterView;
import org.apache.ignite.internal.metrics.exporters.configuration.LogPushExporterConfigurationSchema;
import org.apache.ignite.internal.metrics.exporters.configuration.LogPushExporterView;
import org.apache.ignite.internal.metrics.exporters.log.LogPushExporter;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Metric manager.
 */
public class MetricManagerImpl extends AbstractMetricManager {
    // This field cannot be static, because its used by the Thin Client.
    // However, it is expected that MetricManagerImpl will be used only on the server side.
    private final IgniteLogger log = Loggers.forClass(MetricManagerImpl.class);

    private final String nodeName;

    private final Supplier<UUID> clusterIdSupplier;

    private volatile MetricConfiguration metricConfiguration;

    /** Constructor. */
    public MetricManagerImpl(String nodeName, Supplier<UUID> clusterIdSupplier) {
        this.nodeName = nodeName;
        this.clusterIdSupplier = clusterIdSupplier;
    }

    /** Constructor for smoother testing. */
    @TestOnly
    public MetricManagerImpl(String nodeName, Supplier<UUID> clusterIdSupplier, MetricConfiguration metricConfiguration) {
        this.nodeName = nodeName;
        this.clusterIdSupplier = clusterIdSupplier;
        this.metricConfiguration = metricConfiguration;
    }

    /** Sets the configuration. Needed to resolve cyclic dependencies. */
    public void configure(MetricConfiguration metricConfiguration) {
        assert this.metricConfiguration == null : "Metric configuration is already set.";

        this.metricConfiguration = metricConfiguration;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return inBusyLockAsync(busyLock, () -> {
            start(loadExporters());

            return nullCompletedFuture();
        });
    }

    /** Starts the manager using the given exporters. */
    @VisibleForTesting
    public void start(Map<String, MetricExporter> availableExporters) {
        MetricConfiguration metricConfiguration = this.metricConfiguration;

        assert metricConfiguration != null : "Metric configuration is not set.";

        for (ExporterView exporterConfiguration : metricConfiguration.value().exporters()) {
            startAndEnableExporter(exporterConfiguration, availableExporters);
        }

        if (!enabledMetricExporters.containsKey(LogPushExporter.EXPORTER_NAME)) {
            startAndEnableExporter(new DefaultLogPushExporterView(), availableExporters);
        }

        metricConfiguration.exporters().listenElements(new ExporterConfigurationListener(availableExporters));
    }

    private void startAndEnableExporter(ExporterView exporterConfiguration, Map<String, MetricExporter> availableExporters) {
        String exporterName = exporterConfiguration.exporterName();

        MetricExporter exporter = availableExporters.get(exporterName);

        if (exporter == null) {
            log.warn("Unknown metric exporter in configuration [name = {}].", exporterName);

            return;
        }

        try {
            exporter.start(registry, exporterConfiguration, clusterIdSupplier, nodeName);
        } catch (Exception e) {
            log.warn("Unable to start metric exporter [name = {}].", e, exporterName);

            return;
        }

        enabledMetricExporters.put(exporterName, exporter);
    }

    /**
     * Load exporters by {@link ServiceLoader} mechanism.
     */
    private static Map<String, MetricExporter> loadExporters() {
        ClassLoader clsLdr = Thread.currentThread().getContextClassLoader();

        return ServiceLoader
                .load(MetricExporter.class, clsLdr)
                .stream()
                .map(Provider::get)
                .collect(toUnmodifiableMap(MetricExporter::name, Function.identity()));
    }

    private static class DefaultLogPushExporterView implements LogPushExporterView {
        private final LogPushExporterConfigurationSchema schema = new LogPushExporterConfigurationSchema();

        @Override
        public long periodMillis() {
            return schema.periodMillis;
        }

        @Override
        public boolean oneLinePerMetricSource() {
            return schema.oneLinePerMetricSource;
        }

        @Override
        public String[] enabledMetrics() {
            return schema.enabledMetrics;
        }

        @Override
        public String exporterName() {
            return LogPushExporter.EXPORTER_NAME;
        }

        @Override
        public String name() {
            return "log";
        }
    }

    private class ExporterConfigurationListener implements ConfigurationNamedListListener<ExporterView> {
        private final Map<String, MetricExporter> availableExporters;

        ExporterConfigurationListener(Map<String, MetricExporter> availableExporters) {
            this.availableExporters = availableExporters;
        }

        @Override
        public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ExporterView> ctx) {
            inBusyLockSafe(busyLock, () -> {
                ExporterView newValue = ctx.newValue();

                assert newValue != null;

                startAndEnableExporter(newValue, availableExporters);
            });

            return nullCompletedFuture();
        }

        @Override
        public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ExporterView> ctx) {
            inBusyLockSafe(busyLock, () -> {
                ExporterView oldValue = ctx.oldValue();

                assert oldValue != null;

                MetricExporter removed = enabledMetricExporters.remove(oldValue.exporterName());

                if (removed != null) {
                    removed.stop();
                }
            });

            return nullCompletedFuture();
        }

        @Override
        public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ExporterView> ctx) {
            inBusyLockSafe(busyLock, () -> {
                ExporterView newValue = ctx.newValue();

                assert newValue != null;

                MetricExporter exporter = enabledMetricExporters.get(newValue.exporterName());

                if (exporter != null) {
                    exporter.reconfigure(newValue);
                }
            });

            return nullCompletedFuture();
        }
    }
}
