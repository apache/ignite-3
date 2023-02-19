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

import static java.lang.Math.addExact;

import java.io.Serializable;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * A hybrid timestamp that combines physical clock and logical clock.
 */
public final class HybridTimestamp implements Comparable<HybridTimestamp>, Serializable {
    /** Serial version UID. */
    private static final long serialVersionUID = 2459861612869605904L;

    /**
     * Time value to store for {@code null} hybrid timestamp values.
     */
    public static final long NULL_HYBRID_TIMESTAMP = 0L;

    /** Number of bits in "logical time" part. */
    public static final int LOGICAL_TIME_BITS_SIZE = 2 * Byte.SIZE;

    /** Number of bits in "physical time" part. */
    public static final int PHYSICAL_TIME_BITS_SIZE = 6 * Byte.SIZE;

    /** Timestamp size in bytes. */
    public static final int HYBRID_TIMESTAMP_SIZE = Long.BYTES;

    /** A constant holding the maximum value a {@code HybridTimestamp} can have. */
    public static final HybridTimestamp MAX_VALUE = new HybridTimestamp(Long.MAX_VALUE);

    /** Long time value, that consists of physical time in higher 6 bytes and logical time in lower 2 bytes. */
    private final long time;

    /**
     * The constructor.
     *
     * @param physical The physical time.
     * @param logical The logical time.
     */
    public HybridTimestamp(long physical, int logical) {
        if (physical < 0 || physical >= (1L << PHYSICAL_TIME_BITS_SIZE)) {
            throw new IllegalArgumentException("physical time is out of bounds: " + physical);
        }

        if (logical < 0 || logical >= (1L << LOGICAL_TIME_BITS_SIZE)) {
            throw new IllegalArgumentException("logical time is out of bounds: " + logical);
        }

        time = (physical << LOGICAL_TIME_BITS_SIZE) | logical;

        if (time <= 0) {
            throw new IllegalArgumentException("time is out of bounds: " + time);
        }
    }

    private HybridTimestamp(long time) {
        if (time <= 0) {
            throw new IllegalArgumentException("time is out of bounds: " + time);
        }

        this.time = time;
    }

    public static @Nullable HybridTimestamp of(long time) {
        return time == NULL_HYBRID_TIMESTAMP ? null : new HybridTimestamp(time);
    }

    public static long nullableLongTime(@Nullable HybridTimestamp timestamp) {
        return timestamp == null ? NULL_HYBRID_TIMESTAMP : timestamp.time;
    }

    /**
     * Compares hybrid timestamps.
     *
     * @param times Times for comparing.
     * @return The highest hybrid timestamp.
     */
    public static @Nullable HybridTimestamp max(HybridTimestamp... times) {
        if (times.length == 0) {
            return null;
        }

        HybridTimestamp maxTime = times[0];

        for (int i = 1; i < times.length; i++) {
            if (maxTime.compareTo(times[i]) < 0) {
                maxTime = times[i];
            }
        }

        return maxTime;
    }

    /**
     * Returns a physical component.
     *
     * @return The physical component.
     */
    public long getPhysical() {
        return time >>> LOGICAL_TIME_BITS_SIZE;
    }

    /**
     * Returns a logical component.
     *
     * @return The logical component.
     */
    public int getLogical() {
        return (int) (time << PHYSICAL_TIME_BITS_SIZE >>> PHYSICAL_TIME_BITS_SIZE);
    }

    public long longValue() {
        return time;
    }

    /**
     * Returns a new hybrid timestamp with incremented logical component.
     *
     * @return The hybrid timestamp.
     *
     * @throws ArithmeticException on the long overflow.
     */
    public HybridTimestamp addTicks(int ticks) {
        assert ticks >= 0;

        return new HybridTimestamp(addExact(time, ticks));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return time == ((HybridTimestamp) o).time;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(time);
    }

    @Override
    public int compareTo(HybridTimestamp other) {
        return Long.compare(this.time, other.time);
    }

    @Override
    public String toString() {
        return S.toString(HybridTimestamp.class, this);
    }
}
