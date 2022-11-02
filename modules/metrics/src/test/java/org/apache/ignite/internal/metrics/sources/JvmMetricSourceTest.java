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

import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.concurrent.locks.LockSupport;
import javax.management.ObjectName;
import org.apache.ignite.internal.metrics.LongMetric;
import org.junit.jupiter.api.Test;

/** Tests for jvm system metrics. */
public class JvmMetricSourceTest {
    @Test
    void testMemoryMetric() {
        var memoryBean = new MemoryBean(5, 15, 20, 90,
                100, 115, 120, 200);
        var metricSource = new JvmMetricSource(memoryBean);

        var metricSet = metricSource.enable();

        assertEquals(memoryBean.heapInit, metricSet.<LongMetric>get("memory.heap.init").value());
        assertEquals(memoryBean.heapUsed, metricSet.<LongMetric>get("memory.heap.used").value());
        assertEquals(memoryBean.heapCommitted, metricSet.<LongMetric>get("memory.heap.committed").value());
        assertEquals(memoryBean.heapMax, metricSet.<LongMetric>get("memory.heap.max").value());

        assertEquals(memoryBean.nonHeapInit, metricSet.<LongMetric>get("memory.non-heap.init").value());
        assertEquals(memoryBean.nonHeapUsed, metricSet.<LongMetric>get("memory.non-heap.used").value());
        assertEquals(memoryBean.nonHeapCommitted, metricSet.<LongMetric>get("memory.non-heap.committed").value());
        assertEquals(memoryBean.nonHeapMax, metricSet.<LongMetric>get("memory.non-heap.max").value());

        memoryBean.heapUsed += 1;
        memoryBean.heapCommitted += 1;

        memoryBean.nonHeapUsed += 1;
        memoryBean.nonHeapCommitted += 1;

        // wait for memory usage cache update
        while (metricSet.<LongMetric>get("memory.heap.used").value() != memoryBean.heapUsed) {
            LockSupport.parkNanos(100_000_000);
        }

        assertEquals(memoryBean.heapInit, metricSet.<LongMetric>get("memory.heap.init").value());
        assertEquals(memoryBean.heapUsed, metricSet.<LongMetric>get("memory.heap.used").value());
        assertEquals(memoryBean.heapCommitted, metricSet.<LongMetric>get("memory.heap.committed").value());
        assertEquals(memoryBean.heapMax, metricSet.<LongMetric>get("memory.heap.max").value());

        assertEquals(memoryBean.nonHeapInit, metricSet.<LongMetric>get("memory.non-heap.init").value());
        assertEquals(memoryBean.nonHeapUsed, metricSet.<LongMetric>get("memory.non-heap.used").value());
        assertEquals(memoryBean.nonHeapCommitted, metricSet.<LongMetric>get("memory.non-heap.committed").value());
        assertEquals(memoryBean.nonHeapMax, metricSet.<LongMetric>get("memory.non-heap.max").value());
    }

    /**
     * Test implementation of {@link java.lang.management.MemoryMXBean},
     * which open for mutations in scope of the current test.
     *
     */
    private class MemoryBean implements MemoryMXBean {
        public long heapInit;
        public long heapUsed;
        public long heapCommitted;
        public long heapMax;

        public long nonHeapInit;
        public long nonHeapUsed;
        public long nonHeapCommitted;
        public long nonHeapMax;

        private MemoryBean(long heapInit, long heapUsed, long heapCommitted, long heapMax,
                long nonHeapInit, long nonHeapUsed, long nonHeapCommitted, long nonHeapMax) {
            this.heapInit = heapInit;
            this.heapUsed = heapUsed;
            this.heapCommitted = heapCommitted;
            this.heapMax = heapMax;

            this.nonHeapInit = nonHeapInit;
            this.nonHeapUsed = nonHeapUsed;
            this.nonHeapCommitted = nonHeapCommitted;
            this.nonHeapMax = nonHeapMax;
        }

        @Override
        public int getObjectPendingFinalizationCount() {
            throw new UnsupportedOperationException("Not supported in test implementation");
        }

        @Override
        public MemoryUsage getHeapMemoryUsage() {
            return new MemoryUsage(heapInit, heapUsed, heapCommitted, heapMax);
        }

        @Override
        public MemoryUsage getNonHeapMemoryUsage() {
            return new MemoryUsage(nonHeapInit, nonHeapUsed, nonHeapCommitted, nonHeapMax);
        }

        @Override
        public boolean isVerbose() {
            throw new UnsupportedOperationException("Not supported in test implementation");
        }

        @Override
        public void setVerbose(boolean value) {
            throw new UnsupportedOperationException("Not supported in test implementation");
        }

        @Override
        public void gc() {
            throw new UnsupportedOperationException("Not supported in test implementation");
        }

        @Override
        public ObjectName getObjectName() {
            throw new UnsupportedOperationException("Not supported in test implementation");
        }
    }
}
