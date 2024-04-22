package org.apache.ignite.internal.metrics.sources;

import java.util.HashMap;
import java.util.stream.LongStream;
import org.apache.ignite.internal.metrics.MovingAverageMetric;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSource;
import org.jetbrains.annotations.Nullable;

/**
 * Metrics of striped disruptor.
 */
public class RaftMetricSource implements MetricSource {
    public static final String SOURCE_NAME = "raft";

    /** True, if source is enabled, false otherwise. */
    private boolean enabled;

    /** Disruptor stripe count. */
    private final int stripeCount;

    /** Log disruptor stripe count. */
    private final int logStripeCount;

    /** Metric set. */
    HashMap<String, Metric> metrics = new HashMap<>();

    /**
     * Constructor.
     *
     * @param stripeCount Count of stripes.
     * @param logStripeCount Log manager disruptor stripe count.
     */
    public RaftMetricSource(int stripeCount, int logStripeCount) {
        this.stripeCount = stripeCount;
        this.logStripeCount = logStripeCount;
    }

    @Override
    public String name() {
        return SOURCE_NAME;
    }

    @Override
    public @Nullable MetricSet enable() {

        // jraft-fsmcaller-disruptor
        metrics.put("jraft-fsmcaller-disruptor.average.bath",
                new MovingAverageMetric(
                        "jraft-fsmcaller-disruptor.average.bath",
                        "Average batch size in disruptor",
                        d -> String.format("%,.2f", d)
                ));
        metrics.put("jraft-fsmcaller-disruptor.stripes.histogram",
                new DistributionMetric("jraft-fsmcaller-disruptor.stripes.histogram",
                        "Distribution tasks by stripes in disruptor",
                        LongStream.range(0, stripeCount).toArray()));

        // jraft-nodeimpl-disruptor
        metrics.put("jraft-nodeimpl-disruptor.average.bath",
                new MovingAverageMetric(
                        "jraft-nodeimpl-disruptor.average.bath",
                        "Average batch size in disruptor",
                        d -> String.format("%,.2f", d)
                ));
        metrics.put("jraft-nodeimpl-disruptor.stripes.histogram",
                new DistributionMetric("jraft-nodeimpl-disruptor.stripes.histogram",
                        "Distribution tasks by stripes in disruptor",
                        LongStream.range(0, stripeCount).toArray()));

        // jraft-readonlyservice-disruptor
        metrics.put("jraft-readonlyservice-disruptor.average.bath",
                new MovingAverageMetric(
                        "jraft-readonlyservice-disruptor.average.bath",
                        "Average batch size in disruptor",
                        d -> String.format("%,.2f", d)
                ));
        metrics.put("jraft-readonlyservice-disruptor.stripes.histogram",
                new DistributionMetric("jraft-readonlyservice-disruptor.stripes.histogram",
                        "Distribution tasks by stripes in disruptor",
                        LongStream.range(0, stripeCount).toArray()));

        // jraft-logmanager-disruptor
        metrics.put("jraft-logmanager-disruptor.average.bath",
                new MovingAverageMetric(
                        "jraft-logmanager-disruptor.average.bath",
                        "Average batch size in disruptor",
                        d -> String.format("%,.2f", d)
                ));
        metrics.put("jraft-logmanager-disruptor.stripes.histogram",
                new DistributionMetric("jraft-logmanager-disruptor.stripes.histogram",
                        "Distribution tasks by stripes in disruptor",
                        LongStream.range(0, logStripeCount).toArray()));

        enabled = true;

        return new MetricSet(SOURCE_NAME, metrics);
    }

    /**
     * Disruptor metrics source.
     *
     * @param name Disruptor name.
     * @return Object to track metrics.
     */
    public DisruptorMetrics disruptorMetrics(String name) {
        return new DisruptorMetrics(
                (MovingAverageMetric) metrics.get(name.toLowerCase() + ".average.bath"),
                (DistributionMetric) metrics.get(name.toLowerCase() + ".stripes.histogram")
        );
    }

    @Override
    public void disable() {
        enabled = false;
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    /**
     * Striped disruptor metrics.
     */
    public static class DisruptorMetrics {
        private MovingAverageMetric averageBatchSizeMetric;
        private DistributionMetric stripeHistogramMetric;

        public DisruptorMetrics(MovingAverageMetric averageBatchSizeMetric, DistributionMetric stripeHistogramMetric) {
            this.averageBatchSizeMetric = averageBatchSizeMetric;
            this.stripeHistogramMetric = stripeHistogramMetric;
        }

        public void addBatchSize(long size) {
            averageBatchSizeMetric.add(size);
        }

        public void hitToStripe(int stripe) {
            stripeHistogramMetric.add(stripe);
        }
    }
}
