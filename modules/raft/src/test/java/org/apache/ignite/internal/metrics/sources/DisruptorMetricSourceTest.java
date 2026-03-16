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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.lmax.disruptor.RingBuffer;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * Tests metric source name and disruptor metric names.
 */
public class DisruptorMetricSourceTest extends BaseIgniteAbstractTest {
    private static final String SOURCE_NAME = "test_disruptor";

    @Test
    void testMetricSourceName() {
        RingBuffer<?>[] ringBuffers = createMockRingBuffers(1);

        var metricSource = new DisruptorMetricSource(SOURCE_NAME, ringBuffers);

        assertThat(metricSource.name(), is(SOURCE_NAME + ".disruptor"));
    }

    @Test
    void testMetricNamesMultipleStripes() {
        RingBuffer<?>[] ringBuffers = createMockRingBuffers(3);

        var metricSource = new DisruptorMetricSource(SOURCE_NAME, ringBuffers);

        MetricSet set = metricSource.enable();

        assertThat(set, is(notNullValue()));

        Set<String> expectedMetrics = Set.of(
                "Batch",
                "Stripes"
        );

        var actualMetrics = new HashSet<String>();
        set.forEach(m -> actualMetrics.add(m.name()));

        assertThat(actualMetrics, is(expectedMetrics));
    }

    @SuppressWarnings("unchecked")
    private static RingBuffer<?>[] createMockRingBuffers(int count) {
        RingBuffer<?>[] ringBuffers = new RingBuffer[count];

        for (int i = 0; i < count; i++) {
            RingBuffer<?> ringBuffer = mock(RingBuffer.class);
            when(ringBuffer.remainingCapacity()).thenReturn(1024L);
            when(ringBuffer.getBufferSize()).thenReturn(2048);
            ringBuffers[i] = ringBuffer;
        }

        return ringBuffers;
    }
}

