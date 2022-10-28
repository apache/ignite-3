package org.apache.ignite.internal.metrics.sources;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import javax.management.ObjectName;
import org.apache.ignite.internal.metrics.LongMetric;
import org.junit.jupiter.api.Test;

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

        assertEquals(memoryBean.heapInit, metricSet.<LongMetric>get("memory.heap.init").value());
        assertEquals(memoryBean.heapUsed, metricSet.<LongMetric>get("memory.heap.used").value());
        assertEquals(memoryBean.heapCommitted, metricSet.<LongMetric>get("memory.heap.committed").value());
        assertEquals(memoryBean.heapMax, metricSet.<LongMetric>get("memory.heap.max").value());

        assertEquals(memoryBean.nonHeapInit, metricSet.<LongMetric>get("memory.non-heap.init").value());
        assertEquals(memoryBean.nonHeapUsed, metricSet.<LongMetric>get("memory.non-heap.used").value());
        assertEquals(memoryBean.nonHeapCommitted, metricSet.<LongMetric>get("memory.non-heap.committed").value());
        assertEquals(memoryBean.nonHeapMax, metricSet.<LongMetric>get("memory.non-heap.max").value());
    }

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
