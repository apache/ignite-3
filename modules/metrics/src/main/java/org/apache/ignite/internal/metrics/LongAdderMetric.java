package org.apache.ignite.internal.metrics;

import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.metrics.AbstractMetric;
import org.apache.ignite.internal.metrics.LongMetric;
import org.jetbrains.annotations.Nullable;

public class LongAdderMetric extends AbstractMetric implements LongMetric {
    /** Value. */
    private volatile LongAdder val;

    /**
     * @param name Name.
     * @param desc Description.
     */
    public LongAdderMetric(String name, @Nullable String desc) {
        super(name, desc);

        this.val = new LongAdder();
    }

    /**
     * Adds x to the metric.
     *
     * @param x Value to be added.
     */
    public void add(long x) {
        val.add(x);
    }

    /**
     * Sets value.
     *
     * @param val Value.
     */
    public void value(long val) {
        LongAdder adder = new LongAdder();
        adder.add(val);
        this.val = adder;
    }

    /** {@inheritDoc} */
    @Override public long value() {
        return val.sum();
    }

}
