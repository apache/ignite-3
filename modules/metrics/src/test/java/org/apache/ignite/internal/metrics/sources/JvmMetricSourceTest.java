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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.List;
import javax.management.ObjectName;
import org.apache.ignite.internal.metrics.LongMetric;
import org.junit.jupiter.api.Test;

/** Tests for jvm system metrics. */
public class JvmMetricSourceTest {
    @Test
    void testMemoryMetrics() {
        var memoryBean = new MemoryBean(5, 15, 20, 90,
                100, 115, 120, 200);
        var gcBean = new GarbageCollectorBean(10, 100);
        var metricSource = new JvmMetricSource(memoryBean, List.of(gcBean));

        var metricSet = metricSource.enable();

        assertEquals(memoryBean.heapInit, metricSet.<LongMetric>get("memory.heap.Init").value());
        assertEquals(memoryBean.heapUsed, metricSet.<LongMetric>get("memory.heap.Used").value());
        assertEquals(memoryBean.heapCommitted, metricSet.<LongMetric>get("memory.heap.Committed").value());
        assertEquals(memoryBean.heapMax, metricSet.<LongMetric>get("memory.heap.Max").value());

        assertEquals(memoryBean.nonHeapInit, metricSet.<LongMetric>get("memory.non-heap.Init").value());
        assertEquals(memoryBean.nonHeapUsed, metricSet.<LongMetric>get("memory.non-heap.Used").value());
        assertEquals(memoryBean.nonHeapCommitted, metricSet.<LongMetric>get("memory.non-heap.Committed").value());
        assertEquals(memoryBean.nonHeapMax, metricSet.<LongMetric>get("memory.non-heap.Max").value());

        memoryBean.heapUsed += 1;
        memoryBean.heapCommitted += 1;

        memoryBean.nonHeapUsed += 1;
        memoryBean.nonHeapCommitted += 1;

        // wait for memory usage cache update
        await()
                .atMost(10, SECONDS)
                .untilAsserted(() -> assertEquals(
                        memoryBean.heapUsed,
                        metricSet.<LongMetric>get("memory.heap.Used").value(),
                        "The value of the memory.heap.Used metric was not updated in 10 sec."));

        assertEquals(memoryBean.heapInit, metricSet.<LongMetric>get("memory.heap.Init").value());
        assertEquals(memoryBean.heapUsed, metricSet.<LongMetric>get("memory.heap.Used").value());
        assertEquals(memoryBean.heapCommitted, metricSet.<LongMetric>get("memory.heap.Committed").value());
        assertEquals(memoryBean.heapMax, metricSet.<LongMetric>get("memory.heap.Max").value());

        assertEquals(memoryBean.nonHeapInit, metricSet.<LongMetric>get("memory.non-heap.Init").value());
        assertEquals(memoryBean.nonHeapUsed, metricSet.<LongMetric>get("memory.non-heap.Used").value());
        assertEquals(memoryBean.nonHeapCommitted, metricSet.<LongMetric>get("memory.non-heap.Committed").value());
        assertEquals(memoryBean.nonHeapMax, metricSet.<LongMetric>get("memory.non-heap.Max").value());
    }

    @Test
    void testGcMetrics() {
        var memoryBean = new MemoryBean(5, 15, 20, 90,
                100, 115, 120, 200);
        var gcBean1 = new GarbageCollectorBean(10, 100);
        var gcBean2 = new GarbageCollectorBean(20, 200);
        var metricSource = new JvmMetricSource(memoryBean, List.of(gcBean1, gcBean2));

        var metricSet = metricSource.enable();

        assertEquals(300, metricSet.<LongMetric>get("gc.CollectionTime").value());

        gcBean1.changeCollectionMetrics(1, 10);
        gcBean2.changeCollectionMetrics(1, 15);

        assertEquals(325, metricSet.<LongMetric>get("gc.CollectionTime").value());
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

    private static class GarbageCollectorBean implements GarbageCollectorMXBean {
        private long collectionCount;
        private long collectionTime;

        private GarbageCollectorBean(long collectionCount, long collectionTime) {
            this.collectionCount = collectionCount;
            this.collectionTime = collectionTime;
        }

        @Override
        public long getCollectionCount() {
            return collectionCount;
        }

        @Override
        public long getCollectionTime() {
            return collectionTime;
        }

        @Override
        public String getName() {
            return "Fictional Collector";
        }

        @Override
        public boolean isValid() {
            return true;
        }

        @Override
        public String[] getMemoryPoolNames() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ObjectName getObjectName() {
            throw new UnsupportedOperationException();
        }

        private void changeCollectionMetrics(int countDelta, int timeDelta) {
            collectionCount += countDelta;
            collectionTime += timeDelta;
        }
    }
}
