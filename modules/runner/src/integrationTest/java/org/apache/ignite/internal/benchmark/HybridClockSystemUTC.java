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

package org.apache.ignite.internal.benchmark;

import static java.lang.Math.max;
import static java.time.Clock.systemUTC;
import static org.apache.ignite.internal.hlc.HybridTimestamp.LOGICAL_TIME_BITS_SIZE;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.apache.ignite.internal.hlc.ClockUpdateListener;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.FastTimestamps;

/**
 * A Hybrid Logical Clock implementation.
 */
public class HybridClockSystemUTC implements HybridClock {
    private final IgniteLogger log = Loggers.forClass(HybridClockSystemUTC.class);

    private static final AtomicLongFieldUpdater<HybridClockSystemUTC> LATEST_TIME = AtomicLongFieldUpdater.newUpdater(
            HybridClockSystemUTC.class, "latestTime");

    private volatile long latestTime;

    private final List<ClockUpdateListener> updateListeners = new CopyOnWriteArrayList<>();

    /**
     * The constructor which initializes the latest time to current time by system clock.
     */
    public HybridClockSystemUTC() {
        this.latestTime = currentTime();
    }

    private static long currentTime() {
        // TODO https://issues.apache.org/jira/browse/IGNITE-23049 Benchmarks required.
        return systemUTC().instant().toEpochMilli() << LOGICAL_TIME_BITS_SIZE;
    }

    @Override
    public long nowLong() {
        while (true) {
            long now = currentTime();

            // Read the latest time after accessing UTC time to reduce contention.
            long oldLatestTime = latestTime;

            if (oldLatestTime >= now) {
                return LATEST_TIME.incrementAndGet(this);
            }

            long newLatestTime = max(oldLatestTime + 1, now);

            if (LATEST_TIME.compareAndSet(this, oldLatestTime, newLatestTime)) {
                return newLatestTime;
            }
        }
    }

    private void notifyUpdateListeners(long newTs) {
        for (ClockUpdateListener listener : updateListeners) {
            try {
                listener.onUpdate(newTs);
            } catch (Throwable e) {
                log.error("ClockUpdateListener#onUpdate() failed for {} at {}", e, listener, newTs);

                if (e instanceof Error) {
                    throw e;
                }
            }
        }
    }

    @Override
    public HybridTimestamp now() {
        return hybridTimestamp(nowLong());
    }

    /**
     * Updates the clock in accordance with an external event timestamp. If the supplied timestamp is ahead of the current clock timestamp,
     * the clock gets adjusted to make sure it never returns any timestamp before (or equal to) the supplied external timestamp.
     *
     * @param requestTime Timestamp from request.
     * @return The resulting timestamp (guaranteed to exceed both previous clock 'currentTs' and the supplied external ts).
     */
    @Override
    public HybridTimestamp update(HybridTimestamp requestTime) {
        while (true) {
            long now = currentTime();

            // Read the latest time after accessing UTC time to reduce contention.
            long oldLatestTime = this.latestTime;

            long newLatestTime = max(requestTime.longValue() + 1, max(now, oldLatestTime + 1));

            if (LATEST_TIME.compareAndSet(this, oldLatestTime, newLatestTime)) {
                notifyUpdateListeners(newLatestTime);

                return hybridTimestamp(newLatestTime);
            }
        }
    }

    @Override
    public void addUpdateListener(ClockUpdateListener listener) {
        updateListeners.add(listener);
    }

    @Override
    public void removeUpdateListener(ClockUpdateListener listener) {
        updateListeners.remove(listener);
    }

    @Override
    public String toString() {
        return S.toString(HybridClock.class, this);
    }
}
