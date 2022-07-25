package org.apache.ignite.internal.metrics;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class AtomicDoubleMetric extends AbstractMetric implements DoubleMetric {
    /** Field updater. */
    private static final AtomicLongFieldUpdater<AtomicDoubleMetric> updater =
            AtomicLongFieldUpdater.newUpdater(AtomicDoubleMetric.class, "val");

    /** Value. */
    private volatile long val;

    /**
     * @param name Name.
     * @param desc Description.
     */
    public AtomicDoubleMetric(String name, String desc) {
        super(name, desc);
    }

    /**
     * Adds given value to the metric value.
     *
     * @param v Value to be added.
     */
    public void add(double v) {
        for (;;) {
            long exp = val;
            long upd = Double.doubleToLongBits(Double.longBitsToDouble(exp) + v);
            if (updater.compareAndSet(this, exp, upd)) {
                break;
            }
        }
    }

    /**
     * Sets value.
     *
     * @param val Value.
     */
    public void value(double val) {
        this.val = Double.doubleToLongBits(val);
    }

    /** {@inheritDoc} */
    @Override
    public double value() {
        return Double.longBitsToDouble(val);
    }
}
