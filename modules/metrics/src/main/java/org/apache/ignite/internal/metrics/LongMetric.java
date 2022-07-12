package org.apache.ignite.internal.metrics;

/**
 * Interface for the metrics that holds long primitive.
 */
public interface LongMetric extends Metric {
    /** @return Value of the metric. */
    public long value();

    /** {@inheritDoc} */
    @Override public default String getAsString() {
        return Long.toString(value());
    }
}