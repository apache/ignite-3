package org.apache.ignite.internal.metrics;

import org.apache.ignite.internal.metrics.AbstractMetric;
import org.apache.ignite.internal.metrics.LongMetric;
import org.jetbrains.annotations.Nullable;

import java.util.function.LongSupplier;

/**
 * Implementation based on primitive supplier.
 */
public class LongGauge extends AbstractMetric implements LongMetric {
    /** Value supplier. */
    private final LongSupplier val;

    /**
     * @param name Name.
     * @param desc Description.
     * @param val Supplier.
     */
    public LongGauge(String name, @Nullable String desc, LongSupplier val) {
        super(name, desc);

        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public long value() {
        return val.getAsLong();
    }
}