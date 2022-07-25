package org.apache.ignite.internal.metrics;

import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Integer metric is implemented as a volatile {@code int} value.
 */
public class AtomicIntMetric extends AbstractMetric implements IntMetric {
    /** Field updater. */
    private static final AtomicIntegerFieldUpdater<AtomicIntMetric> updater =
            AtomicIntegerFieldUpdater.newUpdater(AtomicIntMetric.class, "val");

    /** Value. */
    private volatile int val;

    /**
     * @param name Name.
     * @param desc Description.
     */
    public AtomicIntMetric(String name, @Nullable String desc) {
        super(name, desc);
    }

    /**
     * Adds x to the metric.
     *
     * @param x Value to be added.
     */
    public void add(int x) {
        updater.addAndGet(this, x);
    }

    /** Adds 1 to the metric. */
    public void increment() {
        add(1);
    }

    /**
     * Sets value.
     *
     * @param val Value.
     */
    public void value(int val) {
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public int value() {
        return val;
    }
}
