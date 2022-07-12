package org.apache.ignite.internal.metrics;

import org.jetbrains.annotations.Nullable;

public abstract class AbstractMetric implements Metric {
    /** Metric name. It is local for a particular {@link MetricsSet}. */
    private final String name;

    /** Metric description. */
    private final String desc;

    /**
     * @param name Name.
     * @param desc Description.
     */
    public AbstractMetric(String name, String desc) {
        assert name != null;
        assert !name.isEmpty();

        this.name = name;
        this.desc = desc;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override @Nullable public String description() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        AbstractMetric metric = (AbstractMetric)o;

        if (!name.equals(metric.name))
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return name.hashCode();
    }
}