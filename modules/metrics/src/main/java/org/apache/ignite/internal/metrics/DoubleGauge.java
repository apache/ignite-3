package org.apache.ignite.internal.metrics;

import org.apache.ignite.internal.metrics.AbstractMetric;
import org.apache.ignite.internal.metrics.DoubleMetric;
import org.jetbrains.annotations.Nullable;

import java.util.function.DoubleSupplier;

public class DoubleGauge extends AbstractMetric implements DoubleMetric {
    /** Value supplier. */
    private final DoubleSupplier val;

    /**
     * @param name Name.
     * @param desc Description.
     * @param val Supplier.
     */
    public DoubleGauge(String name, @Nullable String desc, DoubleSupplier val) {
        super(name, desc);

        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public double value() {
        return val.getAsDouble();
    }
}