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
import java.util.function.LongSupplier;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;

/**
 * Test hybrid clock with custom supplier of current time.
 */
public class TestHybridClock implements HybridClock {
    /** Supplier of current time in milliseconds. */
    private final LongSupplier currentTimeMillisSupplier;

    /** Latest time. */
    private volatile HybridTimestamp latestTime;

    /**
     * Var handle for {@link #latestTime}.
     */
    private static final VarHandle LATEST_TIME;

    static {
        try {
            LATEST_TIME = MethodHandles.lookup().findVarHandle(TestHybridClock.class, "latestTime", HybridTimestamp.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public TestHybridClock(LongSupplier currentTimeMillisSupplier) {
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
        this.latestTime = new HybridTimestamp(currentTimeMillisSupplier.getAsLong(), 0);
    }

    /** {@inheritDoc} */
    @Override
    public HybridTimestamp now() {
        while (true) {
            long currentTimeMillis = currentTimeMillisSupplier.getAsLong();

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
    public HybridTimestamp sync(HybridTimestamp requestTime) {
        while (true) {
            HybridTimestamp latestTime = this.latestTime;

            if (requestTime.compareTo(latestTime) > 0) {
                if (LATEST_TIME.compareAndSet(this, latestTime, requestTime)) {
                    return requestTime;
                }
            } else {
                return latestTime;
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public HybridTimestamp update(HybridTimestamp requestTime) {
        while (true) {
            HybridTimestamp now = new HybridTimestamp(currentTimeMillisSupplier.getAsLong(), -1);

            HybridTimestamp newLatestTime = HybridTimestamp.max(now, requestTime, latestTime).addTicks(1);

            if (LATEST_TIME.compareAndSet(this, latestTime, newLatestTime)) {
                return newLatestTime;
            }
        }
    }
}
