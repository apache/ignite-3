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

package org.apache.ignite.internal.hlc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests of a Hybrid Logical Clock implementation.
 * {@link HybridClock}
 */
@ExtendWith(MockitoExtension.class)
class HybridClockTest extends BaseIgniteAbstractTest {
    /**
     * Mock of a system clock.
     */
    private static MockedStatic<Clock> clockMock;

    @Mock
    private ClockUpdateListener updateListener;

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

    @Test
    void updateListenerIsNotNotifiedOnNowCall() {
        HybridClock clock = new HybridClockImpl();

        clock.addUpdateListener(updateListener);

        HybridTimestamp ts = clock.now();

        verify(updateListener, never()).onUpdate(ts.longValue());
    }

    @Test
    void updateListenerIsNotNotifiedOnNowLongCall() {
        HybridClock clock = new HybridClockImpl();

        clock.addUpdateListener(updateListener);

        long ts = clock.nowLong();

        verify(updateListener, never()).onUpdate(ts);
    }

    @Test
    void updateListenerGetsNotifiedOnExternalUpdate() {
        HybridClock clock = new HybridClockImpl();

        clock.addUpdateListener(updateListener);

        HybridTimestamp ts = clock.now().addPhysicalTime(TimeUnit.DAYS.toMillis(365));

        HybridTimestamp afterUpdate = clock.update(ts);

        verify(updateListener).onUpdate(afterUpdate.longValue());
    }

    @Test
    void updateListenerIsNotUpdatedAfterRemoval() {
        HybridClock clock = new HybridClockImpl();

        clock.addUpdateListener(updateListener);

        clock.removeUpdateListener(updateListener);

        clock.now();

        verify(updateListener, never()).onUpdate(anyLong());
    }

    private static MockedStatic<Clock> mockToEpochMilli(long expected) {
        Clock spyClock = spy(Clock.class);
        MockedStatic<Clock> clockMock = mockStatic(Clock.class);

        clockMock.when(Clock::systemUTC).thenReturn(spyClock);
        when(spyClock.instant()).thenReturn(Instant.ofEpochMilli(expected));

        return clockMock;
    }
}
