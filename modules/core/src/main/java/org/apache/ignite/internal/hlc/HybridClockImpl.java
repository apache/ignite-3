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

import static java.lang.Math.max;
import static org.apache.ignite.internal.hlc.HybridTimestamp.LOGICAL_TIME_BITS_SIZE;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * A Hybrid Logical Clock implementation.
 */
public class HybridClockImpl implements HybridClock {
    private final IgniteLogger log = Loggers.forClass(HybridClockImpl.class);

    private final @Nullable FailureProcessor failureProcessor;

    /**
     * Var handle for {@link #latestTime}.
     */
    private static final AtomicLongFieldUpdater<HybridClockImpl> LATEST_TIME = AtomicLongFieldUpdater.newUpdater(HybridClockImpl.class,
            "latestTime");

    private volatile long latestTime;

    private final List<ClockUpdateListener> updateListeners = new CopyOnWriteArrayList<>();

    @TestOnly
    public HybridClockImpl() {
        this.failureProcessor = null;
    }

    public HybridClockImpl(FailureProcessor failureProcessor) {
        this.failureProcessor = failureProcessor;
    }

    @Override
    public final long nowLong() {
        while (true) {
            long now = currentTime();

            // Read the latest time after accessing UTC time to reduce contention.
            long oldLatestTime = latestTime;

            if (oldLatestTime >= now) {
                return LATEST_TIME.incrementAndGet(this);
            }

            if (LATEST_TIME.compareAndSet(this, oldLatestTime, now)) {
                return now;
            }
        }
    }

    @Override
    public final long currentLong() {
        long current = currentTime();

        return max(latestTime, current);
    }

    @Override
    public final HybridTimestamp now() {
        return hybridTimestamp(nowLong());
    }

    @Override
    public final HybridTimestamp current() {
        return hybridTimestamp(currentLong());
    }

    /**
     * Updates the clock in accordance with an external event timestamp. If the supplied timestamp is ahead of the current clock timestamp,
     * the clock gets adjusted to make sure it never returns any timestamp before (or equal to) the supplied external timestamp.
     *
     * @param requestTime Timestamp from request.
     * @return The resulting timestamp (guaranteed to exceed both previous clock 'currentTs' and the supplied external ts).
     */
    @Override
    public final HybridTimestamp update(HybridTimestamp requestTime) {
        long requestTimeLong = requestTime.longValue();

        while (true) {
            long oldLatestTime = this.latestTime;

            if (oldLatestTime >= requestTimeLong) {
                return hybridTimestamp(LATEST_TIME.incrementAndGet(this));
            }

            long now = currentTime();

            if (now > requestTimeLong) {
                if (LATEST_TIME.compareAndSet(this, oldLatestTime, now)) {
                    return hybridTimestamp(now);
                }
            } else {
                long newLatestTime = requestTimeLong + 1;

                if (LATEST_TIME.compareAndSet(this, oldLatestTime, newLatestTime)) {
                    notifyUpdateListeners(newLatestTime);

                    return hybridTimestamp(newLatestTime);
                }
            }
        }
    }

    /**
     * Returns current physical time in milliseconds.
     *
     * @return Current time.
     */
    protected long physicalTime() {
        return System.currentTimeMillis();
    }

    private long currentTime() {
        return physicalTime() << LOGICAL_TIME_BITS_SIZE;
    }

    private void notifyUpdateListeners(long newTs) {
        for (ClockUpdateListener listener : updateListeners) {
            try {
                listener.onUpdate(newTs);
            } catch (Throwable e) {
                if (failureProcessor != null) {
                    String errorMessage = IgniteStringFormatter.format(
                            "ClockUpdateListener#onUpdate() failed for {} at {}",
                            listener,
                            newTs
                    );
                    failureProcessor.process(new FailureContext(e, errorMessage));
                } else {
                    log.error("ClockUpdateListener#onUpdate() failed for {} at {}", e, listener, newTs);
                }

                if (e instanceof Error) {
                    throw e;
                }
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
