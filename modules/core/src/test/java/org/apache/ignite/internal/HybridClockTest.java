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

package org.apache.ignite.internal;

import static org.apache.ignite.internal.hlc.HybridClockTestUtils.mockToEpochMilli;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Clock;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Tests of a Hybrid Logical Clock implementation.
 * {@link HybridClock}
 */
class HybridClockTest {
    /**
     * Mock of a system clock.
     */
    private static MockedStatic<Clock> clockMock;

    @AfterEach
    public void afterEach() {
        closeClockMock();
    }

    /**
     * Tests a {@link HybridClock#now()}.
     */
    @Test
    public void testNow() {
        clockMock = mockToEpochMilli(100);

        HybridClock clock = new HybridClockImpl();

        assertTimestampEquals(100, new HybridTimestamp(100, 1), clock::now);

        assertTimestampEquals(100, new HybridTimestamp(100, 2), clock::now);

        assertTimestampEquals(200, new HybridTimestamp(200, 0), clock::now);

        assertTimestampEquals(50, new HybridTimestamp(200, 1), clock::now);
    }

    /**
     * Tests a {@link HybridClock#update(HybridTimestamp)}.
     */
    @Test
    public void testTick() {
        clockMock = mockToEpochMilli(100);

        HybridClock clock = new HybridClockImpl();

        assertTimestampEquals(100, new HybridTimestamp(100, 1),
                () -> clock.update(new HybridTimestamp(50, 1)));

        assertTimestampEquals(100, new HybridTimestamp(100, 2),
                () -> clock.update(new HybridTimestamp(60, 1000)));

        assertTimestampEquals(200, new HybridTimestamp(200, 0),
                () -> clock.update(new HybridTimestamp(70, 1)));

        assertTimestampEquals(50, new HybridTimestamp(200, 1),
                () -> clock.update(new HybridTimestamp(70, 1)));

        assertTimestampEquals(500, new HybridTimestamp(500, 0),
                () -> clock.update(new HybridTimestamp(70, 1)));

        assertTimestampEquals(500, new HybridTimestamp(600, 1),
                () -> clock.update(new HybridTimestamp(600, 0)));

        assertTimestampEquals(500, new HybridTimestamp(600, 2),
                () -> clock.update(new HybridTimestamp(600, 0)));
    }

    private void assertTimestampEquals(long sysTime, HybridTimestamp expTs, Supplier<HybridTimestamp> clo) {
        closeClockMock();

        clockMock = mockToEpochMilli(sysTime);

        assertEquals(expTs, clo.get());
    }

    private void closeClockMock() {
        if (clockMock != null && !clockMock.isClosed()) {
            clockMock.close();
        }
    }
}
