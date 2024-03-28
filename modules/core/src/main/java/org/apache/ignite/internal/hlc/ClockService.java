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

/**
 * Service related to clock. Allows to get clock values, update it, wait for it. Also, encapsulates clock-related logic like
 * uncertainty-aware calculations.
 */
public interface ClockService {
    /**
     * Creates a timestamp for new event.
     *
     * @return The hybrid timestamp.
     */
    HybridTimestamp now();

    /**
     * Creates a timestamp for new event.
     *
     * @return The hybrid timestamp as long.
     */
    long nowLong();

    /**
     * Advances the clock in accordance with the request time. If the request time is ahead of the clock,
     * the clock is advanced to the tick that is next to the request time; otherwise, it's advanced to the tick
     * that is next to the local time.
     *
     * @param requestTime Timestamp from request.
     * @return New local hybrid timestamp that is on the clock (it is ahead of both the old clock time and the request time).
     */
    HybridTimestamp updateClock(HybridTimestamp requestTime);

    /**
     * Wait for the clock to reach the given timestamp.
     *
     * <p>If completion of the returned future triggers some I/O operations or causes the code to block, it is highly
     * recommended to execute those completion stages on a specific thread pool to avoid the waiter's pool starvation.
     *
     * @param targetTimestamp Timestamp to wait for.
     * @return A future that completes when the timestamp is reached by the clock's time.
     */
    CompletableFuture<Void> waitFor(HybridTimestamp targetTimestamp);

    /** Returns max tolerable difference between physical clocks in the cluster (in milliseconds). */
    long maxClockSkewMillis();

    /**
     * Defines whether this timestamp is strictly before the given one, taking the clock skew into account.
     *
     * @param firstTimestamp First timestamp.
     * @param anotherTimestamp Another timestamp.
     * @return Whether this timestamp is before the given one or not.
     */
    default boolean before(HybridTimestamp firstTimestamp, HybridTimestamp anotherTimestamp) {
        return compareWithClockSkew(firstTimestamp, anotherTimestamp) < 0;
    }

    /**
     * Defines whether this timestamp is strictly after the given one, taking the clock skew into account.
     *
     * @param firstTimestamp First timestamp.
     * @param anotherTimestamp Another timestamp.
     * @return Whether this timestamp is after the given one or not.
     */
    default boolean after(HybridTimestamp firstTimestamp, HybridTimestamp anotherTimestamp) {
        return compareWithClockSkew(firstTimestamp, anotherTimestamp) > 0;
    }

    /**
     * Compares two timestamps with the clock skew.
     * t1, t2 comparable if t1 is not contained on [t2 - CLOCK_SKEW; t2 + CLOCK_SKEW].
     * TODO: IGNITE-18978 Method to comparison timestamps with clock skew.
     *
     * @param firstTimestamp First timestamp.
     * @param anotherTimestamp Another timestamp.
     * @return Result of comparison can be positive or negative, or {@code 0} if timestamps are not comparable.
     */
    private int compareWithClockSkew(HybridTimestamp firstTimestamp, HybridTimestamp anotherTimestamp) {
        if (firstTimestamp.getPhysical() - maxClockSkewMillis() <= anotherTimestamp.getPhysical()
                && firstTimestamp.getPhysical() + maxClockSkewMillis() >= anotherTimestamp.getPhysical()) {
            return 0;
        }

        return firstTimestamp.compareTo(anotherTimestamp);
    }
}
