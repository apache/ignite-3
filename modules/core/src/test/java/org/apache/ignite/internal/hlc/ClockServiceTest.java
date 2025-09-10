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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

/**
 * Tests of a clock service implementation. {@link ClockService}
 */
public class ClockServiceTest extends BaseIgniteAbstractTest {
    private static final long MAX_CLOCK_SKEW_MILLIS = 100;
    @Mock
    private ClockWaiter clockWaiter;

    private final ClockServiceLogInspector logInspector = new ClockServiceLogInspector();

    @BeforeEach
    void startLogInspectors() {
        logInspector.start();
    }

    @AfterEach
    public void stopCluster() {
        logInspector.stop();
    }

    @Test
    public void testMaximumAllowedClockDriftExceededIsPrinted() {
        HybridClock clock = new HybridClockImpl();
        ClockServiceImpl clockService = new ClockServiceImpl(
                clock,
                clockWaiter,
                () -> MAX_CLOCK_SKEW_MILLIS,
                skew -> {}
        );

        // Check that request time less than max clock skew won't trigger log warning.
        clockService.updateClock(clock.current().addPhysicalTime(MAX_CLOCK_SKEW_MILLIS / 2));
        logInspector.assertNoMessages();

        // Check that request time gt than max clock skew will trigger log warning.
        var timeFromFuture1 = clock.current().addPhysicalTime(MAX_CLOCK_SKEW_MILLIS * 10);
        clockService.updateClock(timeFromFuture1);
        logInspector.assertMessageMatchedNtimes(1);

        // Check that a request time greater than max clock skew will trigger log warning once only.
        // Or in other words, check that logs won't be flooded with dozens of warnings caused by the same or similar clock skew.
        var timeFromFuture2 = clock.current().addPhysicalTime(MAX_CLOCK_SKEW_MILLIS * 10);
        clockService.updateClock(timeFromFuture2);
        clockService.updateClock(timeFromFuture2);
        // 1 for timeFromFuture1 + timeFromFuture2
        logInspector.assertMessageMatchedNtimes(2);

        // Check that another request time greater than max clock skew will trigger log warning once only.
        var timeFromFuture3 = clock.current().addPhysicalTime(MAX_CLOCK_SKEW_MILLIS * 10);
        clockService.updateClock(timeFromFuture3);
        // 1 for timeFromFuture1 + timeFromFuture2 + timeFromFuture3
        logInspector.assertMessageMatchedNtimes(3);
    }

    private static class ClockServiceLogInspector {
        private static final String EXPECTED_MESSAGE = "Maximum allowed clock drift exceeded";
        private final LogInspector logInspector;

        private final AtomicInteger msgCount = new AtomicInteger();

        ClockServiceLogInspector() {
            this.logInspector = LogInspector.create(ClockServiceImpl.class);

            logInspector.addHandler(
                    evt -> evt.getMessage().getFormattedMessage().startsWith(EXPECTED_MESSAGE),
                    msgCount::incrementAndGet);
        }

        void start() {
            logInspector.start();
        }

        void stop() {
            logInspector.stop();
        }

        void assertNoMessages() {
            assertThat(String.format("Error message '%s' is present in the log.", EXPECTED_MESSAGE), msgCount.get(), is(0));
        }

        void assertMessageMatchedNtimes(int n) {
            assertThat(String.format("Expected error message '%s' count doesn't matched.", EXPECTED_MESSAGE), msgCount.get(),
                    is(n));
        }
    }
}
