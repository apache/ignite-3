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
import java.util.function.LongSupplier;

/**
 * Default implementation of {@link ClockService}.
 */
public class ClockServiceImpl implements ClockService {
    private final HybridClock clock;
    private final ClockWaiter clockWaiter;

    private final LongSupplier maxClockSkewMsSupplier;

    /**
     * Constructor.
     */
    public ClockServiceImpl(HybridClock clock, ClockWaiter clockWaiter, LongSupplier maxClockSkewMsSupplier) {
        this.clock = clock;
        this.clockWaiter = clockWaiter;
        this.maxClockSkewMsSupplier = maxClockSkewMsSupplier;
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
        return clockWaiter.waitFor(targetTimestamp);
    }

    @Override
    public long maxClockSkewMillis() {
        return maxClockSkewMsSupplier.getAsLong();
    }
}
