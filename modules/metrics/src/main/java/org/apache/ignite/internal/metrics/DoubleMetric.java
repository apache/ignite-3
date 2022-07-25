package org.apache.ignite.internal.metrics;

public interface DoubleMetric extends Metric {
    /** @return Value of the metric. */
    public double value();

    /** {@inheritDoc} */
    @Override public default String getAsString() {
        return Double.toString(value());
    }
}
