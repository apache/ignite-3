package org.apache.ignite.internal.metrics;

import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Long metric implementation.
 */
public class AtomicLongMetric extends AbstractMetric implements LongMetric {
    /** Field updater. */
    static final AtomicLongFieldUpdater<AtomicLongMetric> updater =
            AtomicLongFieldUpdater.newUpdater(AtomicLongMetric.class, "val");

    /** Field value. */
    private volatile long val;

    /**
     * @param name Name.
     * @param desc Description.
     */
    public AtomicLongMetric(String name, @Nullable String desc) {
        super(name, desc);
    }

    /**
     * Adds x to the metric.
     *
     * @param x Value to be added.
     */
    public void add(long x) {
        updater.getAndAdd(this, x);
    }

    /** Adds 1 to the metric. */
    public void increment() {
        add(1);
    }

    /** Adds -1 to the metric. */
    public void decrement() {
        add(-1);
    }

    /** {@inheritDoc} */
    @Override public long value() {
        return val;
    }

    /**
     * Sets value.
     *
     * @param val Value.
     */
    public void value(long val) {
        this.val = val;
    }
}
