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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests of a Hybrid Logical Clock implementation.
 * {@link HybridClock}
 */
@ExtendWith(MockitoExtension.class)
class HybridClockTest extends BaseIgniteAbstractTest {
    @Mock
    private ClockUpdateListener updateListener;

    private long mockedTime;

    /**
     * Tests a {@link HybridClock#now()}.
     */
    @Test
    public void testNow() {
        mockedTime = 100;

        HybridClock clock = new TestHybridClock(() -> mockedTime);

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
        mockedTime = 100;

        HybridClock clock = new TestHybridClock(() -> mockedTime);

        assertTimestampEquals(100, new HybridTimestamp(100, 1),
                () -> clock.update(new HybridTimestamp(50, 1)));

        assertTimestampEquals(100, new HybridTimestamp(100, 2),
                () -> clock.update(new HybridTimestamp(60, 1000)));

        assertTimestampEquals(200, new HybridTimestamp(100, 3),
                () -> clock.update(new HybridTimestamp(70, 1)));

        assertTimestampEquals(50, new HybridTimestamp(100, 4),
                () -> clock.update(new HybridTimestamp(70, 1)));

        assertTimestampEquals(500, new HybridTimestamp(100, 5),
                () -> clock.update(new HybridTimestamp(70, 1)));

        assertTimestampEquals(500, new HybridTimestamp(600, 1),
                () -> clock.update(new HybridTimestamp(600, 0)));

        assertTimestampEquals(500, new HybridTimestamp(600, 2),
                () -> clock.update(new HybridTimestamp(600, 0)));
    }

    private void assertTimestampEquals(long sysTime, HybridTimestamp expTs, Supplier<HybridTimestamp> clo) {
        mockedTime = sysTime;

        assertEquals(expTs, clo.get());
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
}
