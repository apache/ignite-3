package org.apache.ignite.internal.metrics;

import org.jetbrains.annotations.Nullable;

public interface Metric {
    /**
     * @return Name of the metric.
     */
    String name();

    /**
     * @return Description of the metric.
     */
    String description();

    /**
     * @return String representation of metric value.
     */
    @Nullable
    String getAsString();
}
