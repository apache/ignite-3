package org.apache.ignite.internal.metrics;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * The Metric set that consists of set of metrics identified by a metric name.
 * The metrics set is immutable.
 */
public class MetricsSet implements Iterable<Metric> {
    /** Metrics set name. */
    private final String name;

    /** Registered metrics. */
    private final Map<String, Metric> metrics;

    /**
     * Creates an instance of a metrics set with given name and metrics.
     *
     * @param name Metrics set name.
     * @param metrics Metrics.
     */
    public MetricsSet(String name, Map<String, Metric> metrics) {
        this.name = name;
        this.metrics = Collections.unmodifiableMap(metrics);
    }

    /** {@inheritDoc} */
    @Nullable
    @TestOnly
    public <M extends Metric> M get(String name) {
        return (M)metrics.get(name);
    }

    /** {@inheritDoc} */
    public Iterator<Metric> iterator() {
        return metrics.values().iterator();
    }

    /** @return Name of the metrics set. */
    public String name() {
        return name;
    }

}
