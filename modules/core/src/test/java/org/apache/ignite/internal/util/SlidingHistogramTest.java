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

package org.apache.ignite.internal.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Test for {@link SlidingHistogram}.
 */
public class SlidingHistogramTest {
    @Test
    public void testDefaultValueWhenEmpty() {
        SlidingHistogram hist = new SlidingHistogram(100, 999);
        assertEquals(999, hist.estimatePercentile(0.5));

        hist.record(10);
        hist.record(1000);

        assertEquals(999, hist.estimatePercentile(0.5));
    }

    @Test
    public void testSlidingWindow() {
        SlidingHistogram hist = new SlidingHistogram(3, -1);

        hist.record(10);
        hist.record(1000);
        hist.record(1000);

        assertEquals(16, hist.estimatePercentile(0.3));

        // Slide away from the 1st value.
        hist.record(1000);

        // All values should be 100 now.
        assertEquals(1024, hist.estimatePercentile(0.3));
    }

    @Test
    public void testHistogram() {
        SlidingHistogram hist = new SlidingHistogram(100, 0);

        for (int i = 0; i < 98; i++) {
            hist.record(5);
        }

        hist.record(Integer.MAX_VALUE);
        hist.record(Integer.MAX_VALUE);

        // 98% of values are less than 9.
        assertEquals(9, hist.estimatePercentile(0.98));
    }
}
