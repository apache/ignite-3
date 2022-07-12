package org.apache.ignite.internal.metrics;

/**
 * Interface for the metrics that holds int primitive.
 */
public interface IntMetric extends Metric {
    /** @return Value of the metric. */
    int value();

    /** {@inheritDoc} */
    default String getAsString() {
        return Integer.toString(value());
    }
}
