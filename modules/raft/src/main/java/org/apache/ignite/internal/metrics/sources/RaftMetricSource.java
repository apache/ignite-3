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
import org.apache.ignite.internal.metrics.AtomicIntMetric;
import org.apache.ignite.internal.metrics.AtomicLongMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSource;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.jetbrains.annotations.Nullable;

/**
 * Metrics of striped disruptor.
 */
public class RaftMetricSource implements MetricSource {
    public static final String SOURCE_NAME = "raft";

    public static final String RAFT_GROUP_LEADERS = "groups.LocalLeadersCount";

    public static final String SAVE_META_DURATION = "SaveMetaDuration";

    private static final VarHandle ENABLED;

    static {
        try {
            ENABLED = MethodHandles.lookup().findVarHandle(RaftMetricSource.class, "enabled", boolean.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private volatile boolean enabled;

    /** Metric set. */
    private final Map<String, Metric> metrics;

    private final AtomicIntMetric leadersCount = new AtomicIntMetric(RAFT_GROUP_LEADERS, "Number of raft leaders on this node");

    private final AtomicLongMetric lastSaveMetaDuration = new AtomicLongMetric(
            SAVE_META_DURATION,
            "The last duration of saving raft meta"
    );

    /**
     * Constructor.
     *
     * @param stripeCount Count of stripes.
     * @param logStripeCount Log manager disruptor stripe count.
     */
    public RaftMetricSource(int stripeCount, int logStripeCount) {
        this.metrics = createMetrics();
    }

    public void onLeaderStart(RaftNodeId raftNodeId) {
        leadersCount.increment();
    }

    public void onLeaderStop(RaftNodeId raftNodeId) {
        leadersCount.decrement();
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
        var metrics = new HashMap<String, Metric>();

        metrics.put(RAFT_GROUP_LEADERS, leadersCount);
        metrics.put(SAVE_META_DURATION, lastSaveMetaDuration);

        return metrics;
    }

    @Override
    public void disable() {
        ENABLED.compareAndSet(this, false, true);
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    public void onSaveMeta(long duration) {
        lastSaveMetaDuration.value(duration);
    }
}
