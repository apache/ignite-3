package org.apache.ignite.internal.metrics;

import org.apache.ignite.internal.metrics.AbstractMetric;
import org.apache.ignite.internal.metrics.IntMetric;
import org.jetbrains.annotations.Nullable;

import java.util.function.IntSupplier;

/**
 * Implementation based on primitive supplier.
 */
public class IntGauge extends AbstractMetric implements IntMetric {
    /** Value supplier. */
    private final IntSupplier val;

    /**
     * @param name Name.
     * @param desc Description.
     * @param val Supplier.
     */
    public IntGauge(String name, @Nullable String desc, IntSupplier val) {
        super(name, desc);

        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public int value() {
        return val.getAsInt();
    }
}
