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

import java.lang.management.OperatingSystemMXBean;
import javax.management.ObjectName;
import org.apache.ignite.internal.metrics.DoubleMetric;
import org.junit.jupiter.api.Test;

class OsMetricSourceTest {
    @Test
    void testOsMetrics() {
        var osBean = new OperatingSystemBean(1.23);
        var metricSource = new OsMetricSource(osBean);

        var metricSet = metricSource.enable();

        assertEquals(1.23, metricSet.<DoubleMetric>get("LoadAverage").value());

        osBean.loadAverage = 2.34;

        assertEquals(2.34, metricSet.<DoubleMetric>get("LoadAverage").value());
    }

    private static class OperatingSystemBean implements OperatingSystemMXBean {
        private double loadAverage;

        private OperatingSystemBean(double loadAverage) {
            this.loadAverage = loadAverage;
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
        public ObjectName getObjectName() {
            throw new UnsupportedOperationException();
        }
    }
}
