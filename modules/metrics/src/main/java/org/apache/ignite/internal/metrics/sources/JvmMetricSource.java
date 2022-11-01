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
import java.util.HashMap;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSource;
import org.jetbrains.annotations.Nullable;

/**
 * Metric source, which provides JVM metrics like memory usage, gc stats etc.
 */
public class JvmMetricSource implements MetricSource {

    /** Source name. */
    private static final String SOURCE_NAME = "jvm";

    /** JVM standard MXBean to provide information about memory usage. */
    private final MemoryMXBean memoryMxBean;

    /** Cache holder for initial amount of heap memory.  */
    private final long heapInit;

    /** Cache holder for max amount of heap memory.  */
    private final long heapMax;

    /** Cache holder for initial amount of non-heap memory.  */
    private final long nonHeapInit;

    /** Cache holder for max amount of non-heap memory.  */
    private final long nonHeapMax;

    /** True, if source is enabled, false otherwise. */
    private boolean enabled;

    /**
     * Constructor.
     *
     * @param memoryMxBean MXBean implementation to receive memory info.
     */
    JvmMetricSource(MemoryMXBean memoryMxBean) {
        this.memoryMxBean = memoryMxBean;

        heapInit = memoryMxBean.getHeapMemoryUsage().getInit();
        heapMax = memoryMxBean.getHeapMemoryUsage().getMax();

        nonHeapInit = memoryMxBean.getNonHeapMemoryUsage().getInit();
        nonHeapMax = memoryMxBean.getNonHeapMemoryUsage().getMax();
    }

    /**
     * Constructs new metric source with standard MemoryMXBean as metric provider.
     */
    public JvmMetricSource() {
        memoryMxBean = ManagementFactory.getMemoryMXBean();

        heapInit = memoryMxBean.getHeapMemoryUsage().getInit();
        heapMax = memoryMxBean.getHeapMemoryUsage().getMax();

        nonHeapInit = memoryMxBean.getNonHeapMemoryUsage().getInit();
        nonHeapMax = memoryMxBean.getNonHeapMemoryUsage().getMax();
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

        metrics.put("memory.heap.init",
                new LongGauge("memory.heap.init", "Initial amount of heap memory", () -> heapInit));
        metrics.put("memory.heap.used",
                new LongGauge("memory.heap.used",
                        "Current used amount of heap memory",
                        () -> memoryMxBean.getHeapMemoryUsage().getUsed()));
        metrics.put("memory.heap.committed",
                new LongGauge("memory.heap.committed",
                        "Committed amount of heap memory",
                        () -> memoryMxBean.getHeapMemoryUsage().getCommitted()));
        metrics.put("memory.heap.max",
                new LongGauge("memory.heap.max",
                        "Maximum amount of heap memory",
                        () -> heapMax));


        metrics.put("memory.non-heap.init",
                new LongGauge("memory.non-heap.init",
                        "Initial amount of non-heap memory",
                        () -> nonHeapInit));
        metrics.put("memory.non-heap.used",
                new LongGauge("memory.non-heap.used",
                        "Used amount of non-heap memory",
                        () -> memoryMxBean.getNonHeapMemoryUsage().getUsed()));
        metrics.put("memory.non-heap.committed",
                new LongGauge("memory.non-heap.committed",
                        "Committed amount of non-heap memory",
                        () -> memoryMxBean.getNonHeapMemoryUsage().getCommitted()));
        metrics.put("memory.non-heap.max",
                new LongGauge("memory.non-heap.max",
                        "Maximum amount of non-heap memory",
                        () -> nonHeapMax));

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
}
