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

package org.apache.ignite.hlc;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Clock;
import java.util.function.Supplier;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.TestOnly;

/**
 * A Hybrid Logical Clock.
 */
public class HybridClock {
    /**
     * Var handle for {@link #latestTime}.
     */
    private static final VarHandle LATEST_TIME;

    /**
     * Physical time provider.
     */
    private final Supplier<Long> currentTimeMillisProvider;

    static {
        try {
            LATEST_TIME = MethodHandles.lookup().findVarHandle(HybridClock.class, "latestTime", HybridTimestamp.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** Latest timestamp. */
    protected volatile HybridTimestamp latestTime;

    /**
     * The constructor which initializes the latest time to current time by system clock.
     */
    public HybridClock() {
        this.currentTimeMillisProvider = () -> Clock.systemUTC().instant().toEpochMilli();

        this.latestTime = new HybridTimestamp(currentTimeMillisProvider.get(), 0);
    }

    /**
     * The constructor which initializes the latest time to the given provider.
     */
    @TestOnly
    public HybridClock(Supplier<Long> currentTimeMillisProvider) {
        this.currentTimeMillisProvider = currentTimeMillisProvider;

        this.latestTime = new HybridTimestamp(currentTimeMillisProvider.get(), 0);
    }

    /**
     * Creates a timestamp for new event.
     *
     * @return The hybrid timestamp.
     */
    public HybridTimestamp now() {
        while (true) {
            long currentTimeMillis = currentTimeMillisProvider.get();

            // Read the latest time after accessing UTC time to reduce contention.
            HybridTimestamp latestTime = this.latestTime;

            HybridTimestamp newLatestTime;

            if (latestTime.getPhysical() >= currentTimeMillis) {
                newLatestTime = latestTime.addTicks(1);
            } else {
                newLatestTime = new HybridTimestamp(currentTimeMillis, 0);
            }

            if (LATEST_TIME.compareAndSet(this, latestTime, newLatestTime)) {
                onUpdate(newLatestTime);

                return newLatestTime;
            }
        }
    }

    /**
     * Synchronizes this timestamp with a timestamp from request, if the latter is greater.
     *
     * @param requestTime Timestamp from request.
     * @return The hybrid timestamp.
     */
    public HybridTimestamp sync(HybridTimestamp requestTime) {
        while (true) {
            HybridTimestamp latestTime = this.latestTime;

            if (requestTime.compareTo(latestTime) > 0) {
                if (LATEST_TIME.compareAndSet(this, latestTime, requestTime)) {
                    onUpdate(requestTime);

                    return requestTime;
                }
            } else {
                return latestTime;
            }
        }
    }

    /**
     * Creates a timestamp for a received event.
     *
     * @param requestTime Timestamp from request.
     * @return The hybrid timestamp.
     */
    public HybridTimestamp update(HybridTimestamp requestTime) {
        while (true) {
            HybridTimestamp now = new HybridTimestamp(currentTimeMillisProvider.get(), -1);

            // Read the latest time after accessing UTC time to reduce contention.
            HybridTimestamp latestTime = this.latestTime;

            HybridTimestamp maxLatestTime = HybridTimestamp.max(now, requestTime, latestTime);

            HybridTimestamp newLatestTime = maxLatestTime.addTicks(1);

            if (LATEST_TIME.compareAndSet(this, latestTime, newLatestTime)) {
                onUpdate(newLatestTime);

                return newLatestTime;
            }
        }
    }

    protected void onUpdate(HybridTimestamp timestamp) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(HybridClock.class, this);
    }
}
