package org.apache.ignite.internal.metrics;

import java.util.concurrent.atomic.DoubleAdder;
import org.apache.ignite.internal.metrics.AbstractMetric;
import org.apache.ignite.internal.metrics.DoubleMetric;
import org.jetbrains.annotations.Nullable;

/**
 * Double metric.
 */
public class DoubleAdderMetric extends AbstractMetric implements DoubleMetric {
    /** Value. */
    private volatile DoubleAdder val;

    /**
     * @param name Name.
     * @param desc Description.
     */
    public DoubleAdderMetric(String name, @Nullable String desc) {
        super(name, desc);

        this.val = new DoubleAdder();
    }

    /**
     * Adds x to the metric.
     *
     * @param x Value to be added.
     */
    public void add(double x) {
        val.add(x);
    }

    /**
     * Sets value.
     *
     * @param val Value.
     */
    public void value(double val) {
        DoubleAdder adder = new DoubleAdder();
        adder.add(val);
        this.val = adder;
    }

    /** {@inheritDoc} */
    @Override public double value() {
        return val.sum();
    }
}
