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
 *
 */
public class JvmMetricSource implements MetricSource {

    private static final String SOURCE_NAME = "jvm";

    private final MemoryMXBean memoryMXBean;

    private final long heapInit;

    private final long heapMax;

    private final long nonHeapInit;

    private final long nonHeapMax;

    private boolean enabled;

    public JvmMetricSource(MemoryMXBean memoryMXBean) {
        this.memoryMXBean = memoryMXBean;

        heapInit = memoryMXBean.getHeapMemoryUsage().getInit();
        heapMax = memoryMXBean.getHeapMemoryUsage().getMax();

        nonHeapInit = memoryMXBean.getNonHeapMemoryUsage().getInit();
        nonHeapMax = memoryMXBean.getNonHeapMemoryUsage().getMax();
    }

    public JvmMetricSource() {
        memoryMXBean = ManagementFactory.getMemoryMXBean();

        heapInit = memoryMXBean.getHeapMemoryUsage().getInit();
        heapMax = memoryMXBean.getHeapMemoryUsage().getMax();

        nonHeapInit = memoryMXBean.getNonHeapMemoryUsage().getInit();
        nonHeapMax = memoryMXBean.getNonHeapMemoryUsage().getMax();
    }

    @Override
    public String name() {
        return SOURCE_NAME;
    }

    @Override
    public synchronized @Nullable MetricSet enable() {
        var metrics = new HashMap<String, Metric>();

        // TODO: KKK fill descriptions
        metrics.put("memory.heap.init",
                new LongGauge("memory.heap.init", "", () -> heapInit));
        metrics.put("memory.heap.used",
                new LongGauge("memory.heap.used", "", () -> memoryMXBean.getHeapMemoryUsage().getUsed()));
        metrics.put("memory.heap.committed",
                new LongGauge("memory.heap.committed", "", () -> memoryMXBean.getHeapMemoryUsage().getCommitted()));
        metrics.put("memory.heap.max",
                new LongGauge("memory.heap.max", "", () -> heapMax));


        var nonHeapMemoryUsage = memoryMXBean.getNonHeapMemoryUsage();

        metrics.put("memory.non-heap.init",
                new LongGauge("memory.non-heap.init", "", () -> nonHeapInit));
        metrics.put("memory.non-heap.used",
                new LongGauge("memory.non-heap.used", "", () -> memoryMXBean.getNonHeapMemoryUsage().getUsed()));
        metrics.put("memory.non-heap.committed",
                new LongGauge("memory.non-heap.committed", "", () -> memoryMXBean.getNonHeapMemoryUsage().getCommitted()));
        metrics.put("memory.non-heap.max",
                new LongGauge("memory.non-heap.max", "", () -> nonHeapMax));

        enabled = true;

        return new MetricSet(SOURCE_NAME, metrics);
    }

    @Override
    public synchronized void disable() {
        enabled = false;
    }

    @Override
    public synchronized boolean enabled() {
        return enabled;
    }
}
