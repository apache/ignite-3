package org.apache.ignite.internal.metrics;

/**
 * Interface for all metrics source.
 */
public interface MetricsSource {
    /**
     * Returns metric source name.
     *
     * @return Metric source name.
     */
    String name();

    /**
     * Enables metrics for metric source. Creates and returns {@link MetricsSet} built during enabling.
     *
     * @return Newly created {@link MetricsSet} instance or {@code null} if metrics are already enabled.
     */
    MetricsSet enable();

    /**
     * Disables metrics for metric source.
     */
    void disable();

    /**
     * Checks whether metrics is enabled (switched on) or not (switched off) for metric source.
     *
     * @return {@code True} if metrics are enabled, otherwise - {@code false}.
     */
    boolean enabled();
}
