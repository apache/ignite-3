/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.metrics.sources;

import java.util.Arrays;
import java.util.HashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSource;
import org.jetbrains.annotations.NotNull;
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

        initMetrics();
    }

    @Override
    public String name() {
        return SOURCE_NAME;
    }

    @Override
    public @Nullable MetricSet enable() {
        enabled = true;

        return new MetricSet(SOURCE_NAME, metrics);
    }

    private void initMetrics() {
        long[] bounds = new long[] {10L, 20L, 30L, 40L, 50L};

        // jraft-fsmcaller-disruptor
        metrics.put("jraft-fsmcaller-disruptor.average.batch",
                new DistributionMetric(
                        "jraft-fsmcaller-disruptor.average.batch",
                        "Average batch size in disruptor",
                        bounds,
                        toStringBatchHistogram(bounds)
                ));
        metrics.put("jraft-fsmcaller-disruptor.stripes.histogram",
                new DistributionMetric(
                        "jraft-fsmcaller-disruptor.stripes.histogram",
                        "Distribution tasks by stripes in disruptor",
                        LongStream.range(0, stripeCount).toArray(),
                        toStringStripesHistogram()
                ));

        // jraft-nodeimpl-disruptor
        metrics.put("jraft-nodeimpl-disruptor.average.batch",
                new DistributionMetric(
                        "jraft-nodeimpl-disruptor.average.batch",
                        "Average batch size in disruptor",
                        bounds,
                        toStringBatchHistogram(bounds)
                ));
        metrics.put("jraft-nodeimpl-disruptor.stripes.histogram",
                new DistributionMetric(
                        "jraft-nodeimpl-disruptor.stripes.histogram",
                        "Distribution tasks by stripes in disruptor",
                        LongStream.range(0, stripeCount).toArray(),
                        toStringStripesHistogram()
                ));

        // jraft-readonlyservice-disruptor
        metrics.put("jraft-readonlyservice-disruptor.average.batch",
                new DistributionMetric(
                        "jraft-readonlyservice-disruptor.average.batch",
                        "Average batch size in disruptor",
                        bounds,
                        toStringBatchHistogram(bounds)
                ));
        metrics.put("jraft-readonlyservice-disruptor.stripes.histogram",
                new DistributionMetric(
                        "jraft-readonlyservice-disruptor.stripes.histogram",
                        "Distribution tasks by stripes in disruptor",
                        LongStream.range(0, stripeCount).toArray(),
                        toStringStripesHistogram()
                ));

        // jraft-logmanager-disruptor
        metrics.put("jraft-logmanager-disruptor.average.batch",
                new DistributionMetric(
                        "jraft-logmanager-disruptor.average.batch",
                        "Average batch size in disruptor",
                        bounds,
                        toStringBatchHistogram(bounds)
                ));
        metrics.put("jraft-logmanager-disruptor.stripes.histogram",
                new DistributionMetric(
                        "jraft-logmanager-disruptor.stripes.histogram",
                        "Distribution tasks by stripes in disruptor",
                        LongStream.range(0, logStripeCount).toArray(),
                        toStringStripesHistogram()
                ));
    }

    /**
     * Represents the stripes histogram as a string.
     *
     * @return String representation of histogram metrics.
     */
    @NotNull
    private Function<long[], String> toStringStripesHistogram() {
        return longs -> Arrays.stream(longs, 0, longs.length - 1).mapToObj(String::valueOf)
                .collect(Collectors.joining(", "));
    }

    /**
     * Represents a string for the histogram metric.
     *
     * @param bounds Bounds.
     * @return String representation of histogram metrics.
     */
    @NotNull
    private Function<long[], String> toStringBatchHistogram(long[] bounds) {
        return values -> {
            assert bounds.length == values.length - 1 :
                    "Bounds do not match with values [bounds=" + bounds.length + ", values=" + values.length + "].";

            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < values.length; i++) {
                sb.append(values[i]);

                if (i != values.length - 1) {
                    sb.append("<|").append(bounds[i]).append("|<");
                }
            }

            return sb.toString();
        };
    }

    /**
     * Disruptor metrics source.
     *
     * @param name Disruptor name.
     * @return Object to track metrics.
     */
    public DisruptorMetrics disruptorMetrics(String name) {
        return new DisruptorMetrics(
                (DistributionMetric) metrics.get(name.toLowerCase() + ".average.batch"),
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
    public class DisruptorMetrics {
        private DistributionMetric batchSizeHistogramMetric;
        private DistributionMetric stripeHistogramMetric;

        public DisruptorMetrics(DistributionMetric averageBatchSizeMetric, DistributionMetric stripeHistogramMetric) {
            this.batchSizeHistogramMetric = averageBatchSizeMetric;
            this.stripeHistogramMetric = stripeHistogramMetric;
        }

        public boolean enabled() {
            return enabled;
        }

        public void addBatchSize(long size) {
            batchSizeHistogramMetric.add(size);
        }

        public void hitToStripe(int stripe) {
            stripeHistogramMetric.add(stripe);
        }
    }
}
