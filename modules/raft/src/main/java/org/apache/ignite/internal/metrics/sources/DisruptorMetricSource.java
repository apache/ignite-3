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

import com.lmax.disruptor.RingBuffer;
import java.util.ArrayList;
import java.util.stream.LongStream;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.metrics.Metric;

/**
 * Striped disruptor metrics.
 */
public class DisruptorMetricSource extends AbstractMetricSource<DisruptorMetricSource.Holder> {
    private final RingBuffer<?>[] ringBuffers;

    /**
     * Constructor.
     *
     * @param sourceName Base source name.
     * @param ringBuffers Ring buffers related to the corresponding disruptor.
     */
    public DisruptorMetricSource(String sourceName, RingBuffer<?>[] ringBuffers) {
        super(sourceName + ".disruptor");

        this.ringBuffers = ringBuffers;
    }

    /**
     * Adds a batch size sample.
     *
     * @param size Batch size.
     */
    public void addBatchSize(long size) {
        Holder holder = holder();

        if (holder != null) {
            holder.batchSizeHistogramMetric.add(size);
        }
    }

    /**
     * Records a hit to the specified stripe.
     *
     * @param stripe Stripe index.
     */
    public void hitToStripe(int stripe) {
        Holder holder = holder();

        if (holder != null) {
            holder.stripeHistogramMetric.add(stripe);
        }
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    /** Metric holder for disruptor metrics. */
    protected class Holder implements AbstractMetricSource.Holder<DisruptorMetricSource.Holder> {
        private final DistributionMetric batchSizeHistogramMetric = new DistributionMetric(
                "Batch",
                "The histogram of the batch size to handle in the disruptor",
                new long[] {10L, 20L, 30L, 40L, 50L}
        );

        private final DistributionMetric stripeHistogramMetric = new DistributionMetric(
                "Stripes",
                "The histogram of distribution data by stripes in disruptor",
                LongStream.range(0, ringBuffers.length).toArray()
        );

        private final List<Metric> metrics = List.of(batchSizeHistogramMetric, stripeHistogramMetric);

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
