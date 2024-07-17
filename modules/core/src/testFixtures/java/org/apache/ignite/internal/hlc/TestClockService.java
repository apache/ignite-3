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

import java.util.concurrent.CompletableFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Test implementation of {@link ClockService}.
 */
public class TestClockService implements ClockService {
    /** Max clock skew (in millis) for tests. */
    public static final long TEST_MAX_CLOCK_SKEW_MILLIS = 7L;

    private final HybridClock clock;
    private final ClockWaiter clockWaiter;

    /**
     * Constructor.
     */
    public TestClockService(HybridClock clock) {
        this(clock, null);
    }

    /**
     * Constructor.
     */
    public TestClockService(HybridClock clock, @Nullable ClockWaiter clockWaiter) {
        this.clock = clock;
        this.clockWaiter = clockWaiter;
    }

    @Override
    public HybridTimestamp now() {
        return clock.now();
    }

    @Override
    public long nowLong() {
        return clock.nowLong();
    }

    @Override
    public HybridTimestamp updateClock(HybridTimestamp requestTime) {
        return clock.update(requestTime);
    }

    @Override
    public CompletableFuture<Void> waitFor(HybridTimestamp targetTimestamp) {
        if (clockWaiter == null) {
            throw new IllegalStateException("No clockWaiter provided; please use a constructor that accepts it");
        }

        return clockWaiter.waitFor(targetTimestamp);
    }

    @Override
    public long maxClockSkewMillis() {
        return TEST_MAX_CLOCK_SKEW_MILLIS;
    }
}
