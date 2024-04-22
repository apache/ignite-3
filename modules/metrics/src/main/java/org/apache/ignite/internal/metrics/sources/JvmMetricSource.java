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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.HashMap;
import java.util.function.Supplier;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSource;
import org.apache.ignite.internal.metrics.SizeFormatUtil;
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

    /** True, if source is enabled, false otherwise. */
    private boolean enabled;

    /**
     * Constructor.
     *
     * @param memoryMxBean MXBean implementation to receive memory info.
     */
    JvmMetricSource(MemoryMXBean memoryMxBean) {
        this.memoryMxBean = memoryMxBean;
    }

    /**
     * Constructs new metric source with standard MemoryMXBean as metric provider.
     */
    public JvmMetricSource() {
        memoryMxBean = ManagementFactory.getMemoryMXBean();
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return SOURCE_NAME;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized @Nullable MetricSet enable() {
        var metrics = new HashMap<String, Metric>();

        CachedMemoryUsage heapMemoryUsage = new CachedMemoryUsage(memoryMxBean::getHeapMemoryUsage, MEMORY_USAGE_CACHE_TIMEOUT);
        metrics.put("memory.heap.Init",
                new LongGauge(
                        "memory.heap.Init",
                        "Initial amount of heap memory",
                        () -> heapMemoryUsage.get().getInit(),
                        SizeFormatUtil::byteCountToDisplaySize
                ));
        metrics.put("memory.heap.Used",
                new LongGauge("memory.heap.Used",
                        "Current used amount of heap memory",
                        () -> heapMemoryUsage.get().getUsed(),
                        SizeFormatUtil::byteCountToDisplaySize));
        metrics.put("memory.heap.Committed",
                new LongGauge("memory.heap.Committed",
                        "Committed amount of heap memory",
                        () -> heapMemoryUsage.get().getCommitted(),
                        SizeFormatUtil::byteCountToDisplaySize));
        metrics.put("memory.heap.Max",
                new LongGauge("memory.heap.Max",
                        "Maximum amount of heap memory",
                        () -> heapMemoryUsage.get().getMax(),
                        SizeFormatUtil::byteCountToDisplaySize));

        CachedMemoryUsage nonHeapMemoryUsage = new CachedMemoryUsage(memoryMxBean::getNonHeapMemoryUsage, MEMORY_USAGE_CACHE_TIMEOUT);
        metrics.put("memory.non-heap.Init",
                new LongGauge("memory.non-heap.Init",
                        "Initial amount of non-heap memory",
                        () -> nonHeapMemoryUsage.get().getInit(),
                        SizeFormatUtil::byteCountToDisplaySize));
        metrics.put("memory.non-heap.Used",
                new LongGauge("memory.non-heap.Used",
                        "Used amount of non-heap memory",
                        () -> nonHeapMemoryUsage.get().getUsed(),
                        SizeFormatUtil::byteCountToDisplaySize));
        metrics.put("memory.non-heap.Committed",
                new LongGauge("memory.non-heap.Committed",
                        "Committed amount of non-heap memory",
                        () -> nonHeapMemoryUsage.get().getCommitted(),
                        SizeFormatUtil::byteCountToDisplaySize));
        metrics.put("memory.non-heap.Max",
                new LongGauge("memory.non-heap.Max",
                        "Maximum amount of non-heap memory",
                        () -> nonHeapMemoryUsage.get().getMax(),
                        SizeFormatUtil::byteCountToDisplaySize));

        enabled = true;

        return new MetricSet(SOURCE_NAME, metrics);
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void disable() {
        enabled = false;
    }

    /** {@inheritDoc} */
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
