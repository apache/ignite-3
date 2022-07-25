package org.apache.ignite.internal.metrics;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

/**
 * Base class for all metric sources.
 *
 * @param <T> Holder type.
 */
public abstract class AbstractMetricsSource<T extends AbstractMetricsSource.Holder<T>> implements MetricsSource {
    /** Holder field updater. */
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<AbstractMetricsSource, Holder> HOLDER_FIELD_UPD =
            newUpdater(AbstractMetricsSource.class, AbstractMetricsSource.Holder.class, "holder");

    /** Metric source name. */
    private final String name;

    /** Metric instances holder. */
    private volatile T holder;

    /**
     * Base constructor for all metric source implemnetations.
     *
     * @param name Metric source name.
     */
    protected AbstractMetricsSource(String name) {
        this.name = name;
    }

    /**
     * Returns metric source name.
     *
     * @return Metric source name.
     */
    @Override public final String name() {
        return name;
    }

    /**
     * Checks whether metrics is enabled (switched on) or not (switched off) for metric source.
     *
     * @return {@code True} if metrics are enabled, otherwise - {@code false}.
     */
    @Override public final boolean enabled() {
        return holder != null;
    }

    /**
     * Returns metric instances holder. Use this on oreder to avoid metric lookup from map-like data structures.
     * Returned value is {@code null} if metrics are disabled.
     *
     * @return Metrics holder instance if metrics are enabled, otherwise - {@code null}.
     */
    protected final T holder() {
        return holder;
    }

    /**
     * Method is responsible for creation of appropriate holder instance in underlying implementations.
     *
     * @return New instance of metrics holder that must implements {@link Holder} interface.
     */
    protected abstract T createHolder();

    /** {@inheritDoc} */
    @Override public final MetricsSet enable() {
        MetricsSetBuilder bldr = new MetricsSetBuilder(name);

        T hldr = createHolder();

        init(bldr, hldr);

        MetricsSet reg = bldr.build();

        return HOLDER_FIELD_UPD.compareAndSet(this, null, hldr) ? reg : null;
    }

    /** {@inheritDoc} */
    @Override public final void disable() {
        T holder0 = holder;

        if (HOLDER_FIELD_UPD.compareAndSet(this, holder0, null))
            cleanup(holder0);
    }

    /**
     * Method is responsible for:
     * <ol>
     *     <li>Creation of {@link MetricsSet} instance using provided {@link MetricsSetBuilder}.</li>
     *     <li>Creation of metric instances in given holder.</li>
     *     <li>Other initialization if needed.</li>
     * <ol/>
     *
     * @param bldr Metric registry builder.
     * @param holder Metric instances holder.
     */
    protected abstract void init(MetricsSetBuilder bldr, T holder);

    /**
     * Method is responsible for cleanup and release of all resources initialized or created during {@link #init} method
     * execution. Note that {@link MetricsSet} and {@link Holder} instances will be released automatically.
     *
     * @param holder Metric instances holder.
     */
    protected void cleanup(T holder) {
        // No-op.
    }

    /**
     * Marker interface for metric instances holder.
     *
     * @param <T> Holder type subclass.
     */
    protected interface Holder<T extends Holder<T>> {
    }
}
