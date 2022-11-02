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

import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import org.mockito.MockedStatic;

/**
 * Utils for a Hybrid Logical Clock testing.
 */
public class HybridClockTestUtils {
    /**
     * Creates a mocked system clock.
     *
     * @param expected Expected value which returns by system clock.
     * @return The mocked clock.
     */
    public static MockedStatic<Clock> mockToEpochMilli(long expected) {
        Clock spyClock = spy(Clock.class);
        MockedStatic<Clock> clockMock = mockStatic(Clock.class);

        clockMock.when(Clock::systemUTC).thenReturn(spyClock);
        when(spyClock.instant()).thenReturn(Instant.ofEpochMilli(expected));

        return clockMock;
    }
}
