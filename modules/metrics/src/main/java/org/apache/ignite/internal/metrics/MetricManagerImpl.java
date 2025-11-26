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

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockSafe;
import static org.apache.ignite.lang.ErrorGroups.Common.RESOURCE_CLOSING_ERR;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.metrics.configuration.MetricView;
import org.apache.ignite.internal.metrics.exporters.MetricExporter;
import org.apache.ignite.internal.metrics.exporters.configuration.ExporterView;
import org.apache.ignite.internal.metrics.exporters.configuration.LogPushExporterConfigurationSchema;
import org.apache.ignite.internal.metrics.exporters.configuration.LogPushExporterView;
import org.apache.ignite.internal.metrics.exporters.log.LogPushExporter;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Metric manager.
 */
public class MetricManagerImpl implements MetricManager {
    /** Logger. */
    private final IgniteLogger log;

    /** Metric registry. */
    private final MetricRegistry registry;

    private final Map<String, MetricExporter> enabledMetricExporters = new ConcurrentHashMap<>();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Metrics' exporters. */
    private volatile Map<String, MetricExporter> availableExporters;

    private volatile MetricConfiguration metricConfiguration;

    private volatile Supplier<UUID> clusterIdSupplier;

    private volatile @Nullable String nodeName;

    /**
     * Constructor.
     */
    public MetricManagerImpl() {
        this(Loggers.forClass(MetricManagerImpl.class), null);
    }

    /**
     * Constructor.
     *
     * @param log Logger.
     */
    public MetricManagerImpl(IgniteLogger log, @Nullable String nodeName) {
        registry = new MetricRegistry();
        this.log = log;
        this.nodeName = nodeName;
    }

    @Override
    public void configure(MetricConfiguration metricConfiguration, Supplier<UUID> clusterIdSupplier, String nodeName) {
        assert this.metricConfiguration == null : "Metric manager must be configured only once, on the start of the node";
        assert this.clusterIdSupplier == null : "Metric manager must be configured only once, on the start of the node";
        assert this.nodeName == null : "Metric manager must be configured only once, on the start of the node";

        this.metricConfiguration = metricConfiguration;
        this.clusterIdSupplier = clusterIdSupplier;
        this.nodeName = nodeName;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return inBusyLockAsync(busyLock, () -> {
            start(loadExporters());

            return nullCompletedFuture();
        });
    }

    @Override
    @VisibleForTesting
    public void start(Map<String, MetricExporter> availableExporters) {
        this.availableExporters = Map.copyOf(availableExporters);

        MetricView conf = metricConfiguration.value();

        List<ExporterView> exporters = conf.exporters().stream().collect(toList());
        exporters.addAll(defaultExporters(exporters));

        for (ExporterView exporter : exporters) {
            checkAndStartExporter(exporter.exporterName(), exporter);
        }

        metricConfiguration.exporters().listenElements(new ExporterConfigurationListener());
    }

    @Override
    public void start(Iterable<MetricExporter> exporters) {
        inBusyLock(busyLock, () -> {
            var availableExporters = new HashMap<String, MetricExporter>();

            for (MetricExporter exporter : exporters) {
                exporter.start(registry, null, clusterIdSupplier, nodeName);

                availableExporters.put(exporter.name(), exporter);
                enabledMetricExporters.put(exporter.name(), exporter);
            }

            this.availableExporters = Map.copyOf(availableExporters);
        });
    }

    @Override
    public void beforeNodeStop() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        try {
            closeAllManually(Stream.concat(
                    enabledMetricExporters.values().stream().map(metricExporter -> (ManuallyCloseable) metricExporter::stop),
                    Stream.of(registry)
            ));
        } catch (Exception e) {
            throw new IgniteInternalException(RESOURCE_CLOSING_ERR, e);
        }
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        enabledMetricExporters.clear();

        return nullCompletedFuture();
    }

    @Override
    public void registerSource(MetricSource src) {
        inBusyLock(busyLock, () -> registry.registerSource(src));
    }

    @Override
    public void unregisterSource(MetricSource src) {
        inBusyLockSafe(busyLock, () -> registry.unregisterSource(src));
    }

    @Override
    public void unregisterSource(String srcName) {
        inBusyLockSafe(busyLock, () -> registry.unregisterSource(srcName));
    }

    @Override
    public MetricSet enable(MetricSource src) {
        return inBusyLock(busyLock, () -> {
            MetricSet enabled = registry.enable(src);

            if (enabled != null) {
                enabledMetricExporters.values().forEach(e -> e.addMetricSet(enabled));
            }

            return enabled;
        });
    }

    @Override
    public MetricSet enable(String srcName) {
        return inBusyLock(busyLock, () -> {
            MetricSet enabled = registry.enable(srcName);

            if (enabled != null) {
                enabledMetricExporters.values().forEach(e -> e.addMetricSet(enabled));
            }

            return enabled;
        });
    }

    @Override
    public void disable(MetricSource src) {
        inBusyLockSafe(busyLock, () -> {
            MetricSet metricSet = registry.snapshot().metrics().get(src.name());

            registry.disable(src);

            enabledMetricExporters.values().forEach(e -> e.removeMetricSet(metricSet));
        });
    }

    @Override
    public void disable(String srcName) {
        inBusyLockSafe(busyLock, () -> {
            MetricSet metricSet = registry.snapshot().metrics().get(srcName);

            registry.disable(srcName);

            enabledMetricExporters.values().forEach(e -> e.removeMetricSet(metricSet));
        });
    }

    @Override
    public MetricSnapshot metricSnapshot() {
        return inBusyLock(busyLock, registry::snapshot);
    }

    @Override
    public Collection<MetricSource> metricSources() {
        return inBusyLock(busyLock, registry::metricSources);
    }

    @Override
    public Collection<MetricExporter> enabledExporters() {
        return inBusyLock(busyLock, enabledMetricExporters::values);
    }

    private static List<ExporterView> defaultExporters(List<? extends ExporterView> configuredExporters) {
        if (configuredExporters.stream().map(ExporterView::exporterName).anyMatch(n -> n.equals(LogPushExporter.EXPORTER_NAME))) {
            return emptyList();
        } else {
            ExporterView logExporterView = new LogPushExporterView() {
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
            };

            return List.of(logExporterView);
        }
    }

    private void checkAndStartExporter(String exporterName, ExporterView exporterConfiguration) {
        MetricExporter exporter = availableExporters.get(exporterName);

        if (exporter != null) {
            enabledMetricExporters.computeIfAbsent(exporter.name(), name -> {
                try {
                    exporter.start(registry, exporterConfiguration, clusterIdSupplier, nodeName);

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
     */
    private static Map<String, MetricExporter> loadExporters() {
        ClassLoader clsLdr = Thread.currentThread().getContextClassLoader();

        return ServiceLoader
                .load(MetricExporter.class, clsLdr)
                .stream()
                .map(Provider::get)
                .collect(toUnmodifiableMap(MetricExporter::name, Function.identity()));
    }

    private class ExporterConfigurationListener implements ConfigurationNamedListListener<ExporterView> {
        @Override
        public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ExporterView> ctx) {
            inBusyLockSafe(busyLock, () -> {
                ExporterView newValue = ctx.newValue();

                assert newValue != null;

                checkAndStartExporter(newValue.exporterName(), newValue);
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
