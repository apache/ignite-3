package org.apache.ignite.internal.metrics;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.metrics.exporters.MetricExporter;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * The component services of the metrics. It has functions to switch on / off and register them.
 */
public interface MetricManager extends IgniteComponent {
    /**
     * Method to configure {@link MetricManager} with distributed configuration.
     *
     * @param metricConfiguration Distributed metric configuration.
     */
    // TODO: IGNITE-17718 when we design the system to configure metrics itself
    // TODO: this method should be revisited, but now it is supposed to use only to set distributed configuration for exporters.
    void configure(MetricConfiguration metricConfiguration);

    @Override
    CompletableFuture<Void> startAsync();

    /**
     * Start component.
     *
     * @param availableExporters Map of (name, exporter) with available exporters.
     */
    @VisibleForTesting
    void start(Map<String, MetricExporter> availableExporters);

    /**
     * Starts component with default configuration.
     *
     * @param exporters Exporters.
     */
    void start(Iterable<MetricExporter<?>> exporters);

    @Override
    CompletableFuture<Void> stopAsync();

    /**
     * Register metric source. See {@link MetricRegistry#registerSource(MetricSource)}.
     *
     * @param src Metric source.
     */
    void registerSource(MetricSource src);

    /**
     * Unregister metric source. See {@link MetricRegistry#unregisterSource(MetricSource)}.
     *
     * @param src Metric source.
     */
    void unregisterSource(MetricSource src);

    /**
     * Unregister metric source by name. See {@link MetricRegistry#unregisterSource(String)}.
     *
     * @param srcName Metric source name.
     */
    void unregisterSource(String srcName);

    /**
     * Enable metric source. See {@link MetricRegistry#enable(MetricSource)}.
     *
     * @param src Metric source.
     * @return Metric set, or {@code null} if already enabled.
     */
    MetricSet enable(MetricSource src);

    /**
     * Enable metric source by name. See {@link MetricRegistry#enable(String)}.
     *
     * @param srcName Source name.
     * @return Metric set, or {@code null} if already enabled.
     */
    MetricSet enable(String srcName);

    /**
     * Disable metric source. See {@link MetricRegistry#disable(MetricSource)}.
     *
     * @param src Metric source.
     */
    void disable(MetricSource src);

    /**
     * Disable metric source by name. See {@link MetricRegistry#disable(String)}.
     *
     * @param srcName Metric source name.
     */
    void disable(String srcName);

    /**
     * Metrics snapshot. This is a snapshot of metric sets with corresponding version, the values of the metrics in the
     * metric sets that are included into the snapshot, are changed dynamically.
     *
     * @return Metrics snapshot.
     */
    IgniteBiTuple<Map<String, MetricSet>, Long> metricSnapshot();

    /**
     * Gets a collection of metric sources.
     *
     * @return collection of metric sources
     */
    Collection<MetricSource> metricSources();
}
