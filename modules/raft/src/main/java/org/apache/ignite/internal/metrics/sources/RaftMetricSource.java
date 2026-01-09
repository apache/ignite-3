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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.LongStream;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSource;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.jetbrains.annotations.Nullable;

/**
 * Metrics of striped disruptor.
 */
public class RaftMetricSource implements MetricSource {
    private static final String SOURCE_NAME = "raft";

    private static final VarHandle ENABLED;

    static {
        try {
            ENABLED = MethodHandles.lookup().findVarHandle(RaftMetricSource.class, "enabled", boolean.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private volatile boolean enabled;

    /** Disruptor stripe count. */
    private final int stripeCount;

    /** Log disruptor stripe count. */
    private final int logStripeCount;

    /** Metric set. */
    private final Map<String, Metric> metrics;

    /** Set of raft nodes where this node is leader. */
    private final Set<RaftNodeId> leaderNodeIds = ConcurrentHashMap.newKeySet();

    /**
     * Constructor.
     *
     * @param stripeCount Count of stripes.
     * @param logStripeCount Log manager disruptor stripe count.
     */
    public RaftMetricSource(int stripeCount, int logStripeCount) {
        this.stripeCount = stripeCount;
        this.logStripeCount = logStripeCount;

        this.metrics = createMetrics();
    }

    /** Called when the node becomes leader for a partition.
     *
     * @param raftNodeId Raft node ID.
     */
    public void onLeaderStarted(RaftNodeId raftNodeId) {
        leaderNodeIds.add(raftNodeId);
    }

    /** Called when the node steps down as leader for a partition or is shutdown.
     *
     * @param raftNodeId Raft node ID.
     */
    public void onLeaderStopped(RaftNodeId raftNodeId) {
        leaderNodeIds.remove(raftNodeId);
    }

    @Override
    public String name() {
        return SOURCE_NAME;
    }

    @Override
    public @Nullable MetricSet enable() {
        if (ENABLED.compareAndSet(this, false, true)) {
            return new MetricSet(SOURCE_NAME, metrics);
        }

        return null;
    }

    private Map<String, Metric> createMetrics() {
        long[] bounds = {10L, 20L, 30L, 40L, 50L};

        var metrics = new HashMap<String, Metric>();

        // jraft-fsmcaller-disruptor
        metrics.put("raft.fsmcaller.disruptor.Batch",
                new DistributionMetric(
                        "raft.fsmcaller.disruptor.Batch",
                        "The histogram of the batch size to handle in the state machine for partitions",
                        bounds
                ));
        metrics.put("raft.fsmcaller.disruptor.Stripes",
                new DistributionMetric(
                        "raft.fsmcaller.disruptor.Stripes",
                        "The histogram of distribution data by stripes in the state machine for partitions",
                        LongStream.range(0, stripeCount).toArray()
                ));

        // jraft-nodeimpl-disruptor
        metrics.put("raft.nodeimpl.disruptor.Batch",
                new DistributionMetric(
                        "raft.nodeimpl.disruptor.Batch",
                        "The histogram of the batch size to handle node operations for partitions",
                        bounds
                ));
        metrics.put("raft.nodeimpl.disruptor.Stripes",
                new DistributionMetric(
                        "raft.nodeimpl.disruptor.Stripes",
                        "The histogram of distribution data by stripes for node operations for partitions",
                        LongStream.range(0, stripeCount).toArray()
                ));

        // jraft-readonlyservice-disruptor
        metrics.put("raft.readonlyservice.disruptor.Batch",
                new DistributionMetric(
                        "raft.readonlyservice.disruptor.Batch",
                        "The histogram of the batch size to handle readonly operations for partitions",
                        bounds
                ));
        metrics.put("raft.readonlyservice.disruptor.Stripes",
                new DistributionMetric(
                        "raft.readonlyservice.disruptor.Stripes",
                        "The histogram of distribution data by stripes readonly operations for partitions",
                        LongStream.range(0, stripeCount).toArray()
                ));

        // jraft-logmanager-disruptor
        metrics.put("raft.logmanager.disruptor.Batch",
                new DistributionMetric(
                        "raft.logmanager.disruptor.Batch",
                        "The histogram of the batch size to handle in the log for partitions",
                        bounds
                ));
        metrics.put("raft.logmanager.disruptor.Stripes",
                new DistributionMetric(
                        "raft.logmanager.disruptor.Stripes",
                        "The histogram of distribution data by stripes in the log for partitions",
                        LongStream.range(0, logStripeCount).toArray()
                ));

        metrics.put("raft.group.leaders",
                new IntGauge("Number of raft groups node is a leader of",
                        "Number of raft groups where this node is the leader",
                        leaderNodeIds::size
                ));

        return metrics;
    }

    /**
     * Disruptor metrics source.
     *
     * @param name Disruptor name.
     * @return Object to track metrics.
     */
    public DisruptorMetrics disruptorMetrics(String name) {
        return new DisruptorMetrics(
                (DistributionMetric) metrics.get(name + ".Batch"),
                (DistributionMetric) metrics.get(name + ".Stripes")
        );
    }

    @Override
    public void disable() {
        ENABLED.compareAndSet(this, false, true);
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    public int leaderMetric() {
        return ((IntGauge) metrics.get("raft.group.leaders")).value();
    }

    /**
     * Striped disruptor metrics.
     */
    public class DisruptorMetrics {
        private final DistributionMetric batchSizeHistogramMetric;
        private final DistributionMetric stripeHistogramMetric;

        DisruptorMetrics(DistributionMetric averageBatchSizeMetric, DistributionMetric stripeHistogramMetric) {
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
