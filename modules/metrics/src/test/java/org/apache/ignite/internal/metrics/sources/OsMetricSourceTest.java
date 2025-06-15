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

import static org.junit.jupiter.api.Assertions.assertEquals;

import javax.management.ObjectName;
import org.apache.ignite.internal.metrics.DoubleMetric;
import org.junit.jupiter.api.Test;

class OsMetricSourceTest {
    @Test
    void testOsMetrics() {
        var osBean = new OperatingSystemBean(1.23, 0.5);
        var metricSource = new OsMetricSource(osBean);

        var metricSet = metricSource.enable();

        assertEquals(1.23, metricSet.<DoubleMetric>get("LoadAverage").value());
        assertEquals(0.5, metricSet.<DoubleMetric>get("CpuLoad").value());

        osBean.loadAverage = 2.34;
        osBean.cpuLoad = 0.75;

        assertEquals(2.34, metricSet.<DoubleMetric>get("LoadAverage").value());
        assertEquals(0.75, metricSet.<DoubleMetric>get("CpuLoad").value());
    }

    private static class OperatingSystemBean implements com.sun.management.OperatingSystemMXBean {
        private double loadAverage;
        private double cpuLoad;

        private OperatingSystemBean(double loadAverage, double cpuLoad) {
            this.loadAverage = loadAverage;
            this.cpuLoad = cpuLoad;
        }

        @Override
        public String getName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getArch() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getVersion() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getAvailableProcessors() {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getSystemLoadAverage() {
            return loadAverage;
        }

        @Override
        public long getCommittedVirtualMemorySize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTotalSwapSpaceSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getFreeSwapSpaceSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getProcessCpuTime() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getFreePhysicalMemorySize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTotalPhysicalMemorySize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getSystemCpuLoad() {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getProcessCpuLoad() {
            return cpuLoad;
        }

        @Override
        public ObjectName getObjectName() {
            throw new UnsupportedOperationException();
        }
    }
}
