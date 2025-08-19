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
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Default implementation of {@link ClockService}.
 */
public class ClockServiceImpl implements ClockService {
    private final IgniteLogger log = Loggers.forClass(ClockServiceImpl.class);

    private final HybridClock clock;
    private final ClockWaiter clockWaiter;
    private final LongSupplier maxClockSkewMsSupplier;
    private final Consumer<Long> onMaxClockSkewExceededClosure;

    private volatile long maxClockSkewMillis = -1;

    /**
     * Constructor.
     */
    public ClockServiceImpl(
            HybridClock clock,
            ClockWaiter clockWaiter,
            LongSupplier maxClockSkewMsSupplier,
            Consumer<Long> onMaxClockSkewExceededClosure
    ) {
        this.clock = clock;
        this.clockWaiter = clockWaiter;
        this.maxClockSkewMsSupplier = maxClockSkewMsSupplier;
        this.onMaxClockSkewExceededClosure = onMaxClockSkewExceededClosure;
    }

    @Override
    public HybridTimestamp now() {
        return clock.now();
    }

    @Override
    public HybridTimestamp current() {
        return clock.current();
    }

    @Override
    public long nowLong() {
        return clock.nowLong();
    }

    @Override
    public long currentLong() {
        return clock.currentLong();
    }

    @Override
    public HybridTimestamp updateClock(HybridTimestamp requestTime) {
        // Since clock.current() is also called inside clock.update formally it's a method call duplication.
        // However, since benchmarks did not show any noticeable performance penalty due to the aforementioned call duplication,
        // design purity was prioritized over call redundancy.
        HybridTimestamp currentLocalTimestamp = clock.current();
        long requestTimePhysical = requestTime.getPhysical();
        long currentLocalTimePhysical = currentLocalTimestamp.getPhysical();

        if (requestTimePhysical - maxClockSkewMillis() > currentLocalTimePhysical) {
            log.warn("Maximum allowed clock drift exceeded [requestTime={}, localTime={}, maxClockSkew={}]", requestTime,
                    currentLocalTimestamp, maxClockSkewMillis());
            onMaxClockSkewExceededClosure.accept(requestTimePhysical - currentLocalTimePhysical);
        }

        return clock.update(requestTime);
    }

    @Override
    public CompletableFuture<Void> waitFor(HybridTimestamp targetTimestamp) {
        return clockWaiter.waitFor(targetTimestamp);
    }

    @Override
    public long maxClockSkewMillis() {
        // -1 is an invalid maxClockSkewMillis, thus it might be used as a special "non-initialized" value.
        if (maxClockSkewMillis == -1) {
            maxClockSkewMillis = maxClockSkewMsSupplier.getAsLong();
        }

        return maxClockSkewMillis;
    }
}
