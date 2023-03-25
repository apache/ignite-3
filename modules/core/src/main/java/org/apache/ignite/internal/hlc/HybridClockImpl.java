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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Clock;
import org.apache.ignite.internal.tostring.S;

/**
 * A Hybrid Logical Clock implementation.
 */
public class HybridClockImpl implements HybridClock {
    /**
     * Var handle for {@link #latestTime}.
     */
    private static final VarHandle LATEST_TIME;

    static {
        try {
            LATEST_TIME = MethodHandles.lookup().findVarHandle(HybridClockImpl.class, "latestTime", HybridTimestamp.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** Latest timestamp. */
    protected volatile HybridTimestamp latestTime;

    /**
     * The constructor which initializes the latest time to current time by system clock.
     */
    public HybridClockImpl() {
        this.latestTime = new HybridTimestamp(Clock.systemUTC().instant().toEpochMilli(), 0);
    }

    /** {@inheritDoc} */
    @Override
    public HybridTimestamp now() {
        while (true) {
            long currentTimeMillis = Clock.systemUTC().instant().toEpochMilli();

            // Read the latest time after accessing UTC time to reduce contention.
            HybridTimestamp latestTime = this.latestTime;

            HybridTimestamp newLatestTime;

            if (latestTime.getPhysical() >= currentTimeMillis) {
                newLatestTime = latestTime.addTicks(1);
            } else {
                newLatestTime = new HybridTimestamp(currentTimeMillis, 0);
            }

            if (LATEST_TIME.compareAndSet(this, latestTime, newLatestTime)) {
                return newLatestTime;
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public HybridTimestamp update(HybridTimestamp requestTime) {
        while (true) {
            HybridTimestamp now = new HybridTimestamp(Clock.systemUTC().instant().toEpochMilli(), -1);

            // Read the latest time after accessing UTC time to reduce contention.
            HybridTimestamp latestTime = this.latestTime;

            HybridTimestamp maxLatestTime = HybridTimestamp.max(now, requestTime, latestTime);

            HybridTimestamp newLatestTime = maxLatestTime.addTicks(1);

            if (LATEST_TIME.compareAndSet(this, latestTime, newLatestTime)) {
                return newLatestTime;
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(HybridClock.class, this);
    }
}
