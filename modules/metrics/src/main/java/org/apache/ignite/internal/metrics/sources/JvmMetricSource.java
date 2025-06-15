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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.util.List;
import java.util.function.Supplier;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSetBuilder;
import org.apache.ignite.internal.metrics.MetricSource;
import org.jetbrains.annotations.Nullable;

/**
 * Metric source, which provides JVM metrics like memory usage, gc stats etc.
 */
public class JvmMetricSource implements MetricSource {
    /** Source name. */
    private static final String SOURCE_NAME = "jvm";

    /** Timeout for memory usage stats cache. */
    private static final long MEMORY_USAGE_CACHE_TIMEOUT = 1000;

    /** JVM standard MXBean to provide information about memory usage. */
    private final MemoryMXBean memoryMxBean;

    private final List<GarbageCollectorMXBean> gcMxBeans;

    /** The managed bean for the runtime system of the Java virtual machine. */
    private final RuntimeMXBean runtimeBean;

    /** Enablement status. Accessed from different threads under synchronization on this object. */
    private boolean enabled;

    /**
     * Constructor.
     *
     * @param runtimeBean MXBean implementation to receive runtime info.
     * @param memoryMxBean MXBean implementation to receive memory info.
     * @param gcMxBeans MXBean implementation to receive GC info.
     */
    JvmMetricSource(RuntimeMXBean runtimeBean, MemoryMXBean memoryMxBean, List<GarbageCollectorMXBean> gcMxBeans) {
        this.runtimeBean = runtimeBean;
        this.memoryMxBean = memoryMxBean;
        this.gcMxBeans = List.copyOf(gcMxBeans);
    }

    /**
     * Constructs new metric source with standard MemoryMXBean as metric provider.
     */
    public JvmMetricSource() {
        memoryMxBean = ManagementFactory.getMemoryMXBean();
        gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        runtimeBean = ManagementFactory.getRuntimeMXBean();
    }

    @Override
    public String name() {
        return SOURCE_NAME;
    }

    @Override
    public synchronized @Nullable MetricSet enable() {
        if (enabled) {
            return null;
        }

        var metricSetBuilder = new MetricSetBuilder(SOURCE_NAME);

        CachedMemoryUsage heapMemoryUsage = new CachedMemoryUsage(memoryMxBean::getHeapMemoryUsage, MEMORY_USAGE_CACHE_TIMEOUT);
        metricSetBuilder.longGauge(
                "memory.heap.Init",
                "Initial amount of heap memory",
                () -> heapMemoryUsage.get().getInit());
        metricSetBuilder.longGauge(
                "memory.heap.Used",
                "Current used amount of heap memory",
                () -> heapMemoryUsage.get().getUsed());
        metricSetBuilder.longGauge(
                "memory.heap.Committed",
                "Committed amount of heap memory",
                () -> heapMemoryUsage.get().getCommitted());
        metricSetBuilder.longGauge(
                "memory.heap.Max",
                "Maximum amount of heap memory",
                () -> heapMemoryUsage.get().getMax());

        CachedMemoryUsage nonHeapMemoryUsage = new CachedMemoryUsage(memoryMxBean::getNonHeapMemoryUsage, MEMORY_USAGE_CACHE_TIMEOUT);
        metricSetBuilder.longGauge(
                "memory.non-heap.Init",
                "Initial amount of non-heap memory",
                () -> nonHeapMemoryUsage.get().getInit());
        metricSetBuilder.longGauge(
                "memory.non-heap.Used",
                "Used amount of non-heap memory",
                () -> nonHeapMemoryUsage.get().getUsed());
        metricSetBuilder.longGauge(
                "memory.non-heap.Committed",
                "Committed amount of non-heap memory",
                () -> nonHeapMemoryUsage.get().getCommitted());
        metricSetBuilder.longGauge(
                "memory.non-heap.Max",
                "Maximum amount of non-heap memory",
                () -> nonHeapMemoryUsage.get().getMax());

        metricSetBuilder.longGauge(
                "gc.CollectionTime",
                "Approximate total time spent on garbage collection in milliseconds, summed across all collectors.",
                this::totalCollectionTime);

        metricSetBuilder.longGauge(
                "UpTime",
                "The uptime of the Java virtual machine in milliseconds.",
                runtimeBean::getUptime);

        enabled = true;

        return metricSetBuilder.build();
    }

    private long totalCollectionTime() {
        long total = 0;

        for (GarbageCollectorMXBean gcMxBean : gcMxBeans) {
            long time = gcMxBean.getCollectionTime();

            if (time > 0) {
                total += time;
            }
        }

        return total;
    }

    @Override
    public synchronized void disable() {
        enabled = false;
    }

    @Override
    public synchronized boolean enabled() {
        return enabled;
    }

    /**
     * Simple wrapper for memoization memory usage stats.
     */
    private static class CachedMemoryUsage {
        /** Source of memory usage stats. */
        private final Supplier<MemoryUsage> source;

        /** Timeout of cache in ms. */
        private final long timeout;

        /** Last update time in ms. */
        private volatile long lastUpdateTime;

        /** Last received from source value. */
        private volatile MemoryUsage currentVal;

        /**
         * Constructor.
         *
         * @param source Source of memory usage data.
         * @param timeout Cache timeout in millis.
         */
        private CachedMemoryUsage(Supplier<MemoryUsage> source, long timeout) {
            this.source = source;
            this.timeout = timeout;

            update();
        }

        /**
         * Returns current cached value.
         *
         * @return Current cached value.
         */
        private MemoryUsage get() {
            if ((System.currentTimeMillis() - lastUpdateTime) > timeout) {
                update();
            }

            return currentVal;
        }

        /**
         * Update cache value and last update time.
         */
        private synchronized void update() {
            currentVal = source.get();

            lastUpdateTime = System.currentTimeMillis();
        }
    }
}
