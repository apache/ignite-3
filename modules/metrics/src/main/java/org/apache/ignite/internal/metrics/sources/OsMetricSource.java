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
import java.lang.management.OperatingSystemMXBean;
import java.util.HashMap;
import org.apache.ignite.internal.metrics.DoubleGauge;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSource;
import org.jetbrains.annotations.Nullable;

/**
 * Metric source which provides OS metrics like Load Average.
 */
public class OsMetricSource implements MetricSource {
    private static final String SOURCE_NAME = "os";

    private final OperatingSystemMXBean operatingSystemMxBean;

    /** Enablement status. Accessed from different threads under synchronization on this object. */
    private boolean enabled;

    /**
     * Constructor.
     *
     * @param operatingSystemMxBean MXBean implementation to receive OS info.
     */
    OsMetricSource(OperatingSystemMXBean operatingSystemMxBean) {
        this.operatingSystemMxBean = operatingSystemMxBean;
    }

    /**
     * Constructs new metric source with standard MemoryMXBean as metric provider.
     */
    public OsMetricSource() {
        operatingSystemMxBean = ManagementFactory.getOperatingSystemMXBean();
    }

    @Override
    public String name() {
        return SOURCE_NAME;
    }

    @Override
    public synchronized @Nullable MetricSet enable() {
        var metrics = new HashMap<String, Metric>();

        metrics.put(
                "LoadAverage",
                new DoubleGauge(
                        "LoadAverage",
                        "System load average for the last minute. System load average is the sum of the number of runnable entities "
                                + "queued to the available processors and the number of runnable entities running on the available "
                                + "processors averaged over a period of time. The way in which the load average is calculated depends on "
                                + "the operating system. "
                                + "If the load average is not available, a negative value is returned.",
                        operatingSystemMxBean::getSystemLoadAverage
                )
        );

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
