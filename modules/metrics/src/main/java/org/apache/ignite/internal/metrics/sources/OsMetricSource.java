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
import java.util.function.DoubleSupplier;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSetBuilder;
import org.apache.ignite.internal.metrics.MetricSource;
import org.jetbrains.annotations.Nullable;

/**
 * Metric source which provides OS metrics like Load Average.
 */
public class OsMetricSource implements MetricSource {
    private final IgniteLogger log = Loggers.forClass(OsMetricSource.class);

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
        this(ManagementFactory.getOperatingSystemMXBean());
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

        metricSetBuilder.doubleGauge(
                "LoadAverage",
                "System load average for the last minute. System load average is the sum of the number of runnable entities "
                        + "queued to the available processors and the number of runnable entities running on the available "
                        + "processors averaged over a period of time. The way in which the load average is calculated depends on "
                        + "the operating system. "
                        + "If the load average is not available, a negative value is returned.",
                operatingSystemMxBean::getSystemLoadAverage
        );

        metricSetBuilder.doubleGauge(
                "CpuLoad",
                "System CPU load. The value is between 0.0 and 1.0, where 0.0 means no CPU load and 1.0 means "
                        + "100% CPU load."
                        + "If the CPU load is not available, a negative value is returned.",
                cpuLoadSupplier()
        );

        enabled = true;

        return metricSetBuilder.build();
    }

    @Override
    public synchronized void disable() {
        enabled = false;
    }

    @Override
    public synchronized boolean enabled() {
        return enabled;
    }

    private DoubleSupplier cpuLoadSupplier() {
        try {
            if (operatingSystemMxBean instanceof com.sun.management.OperatingSystemMXBean) {
                com.sun.management.OperatingSystemMXBean sunOs = (com.sun.management.OperatingSystemMXBean) operatingSystemMxBean;
                return sunOs::getProcessCpuLoad;
            }
        } catch (NoClassDefFoundError ignored) {
            // This exception is thrown if the com.sun.management.OperatingSystemMXBean class is not available.
            // In this case, we return a supplier that always returns -1.
        }

        log.warn("The 'com.sun.management.OperatingSystemMXBean' class is not available for class loader. "
                + "CPU metrics are not available.");

        return () -> -1.0;
    }
}
