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

import org.junit.jupiter.api.Test;
import org.mockito.Mock;

/**
 * Tests of a clock service implementation. {@link ClockService}
 */
public class ClockServiceTest {
    @Mock
    private ClockWaiter clockWaiter;

    @Test
    public void testMaximumAllowedClockDriftExceededIsPrinted() {
        HybridClock clock = new HybridClockImpl();
        ClockServiceImpl clockService = new ClockServiceImpl(clock, clockWaiter, () -> 100);

        clockService.updateClock(clock.current().addPhysicalTime(50));

        // Check that there are no messages in log.

        clockService.updateClock(clock.current().addPhysicalTime(150000000));

        // Check that there are no messages in log.
    }
}
