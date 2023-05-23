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

import java.io.Serializable;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * A hybrid timestamp that combines physical clock and logical clock.
 */
public final class HybridTimestamp implements Comparable<HybridTimestamp>, Serializable {
    /** Serial version UID. */
    private static final long serialVersionUID = -4285668148196042529L;

    /** Time value to store for {@code null} hybrid timestamp values. */
    public static final long NULL_HYBRID_TIMESTAMP = 0L;

    /** Number of bits in "logical time" part. */
    public static final int LOGICAL_TIME_BITS_SIZE = 2 * Byte.SIZE;

    /** Mask to extract logical time. */
    public static final long LOGICAL_TIME_MASK = (1L << LOGICAL_TIME_BITS_SIZE) - 1;

    /** Number of bits in "physical time" part. */
    public static final int PHYSICAL_TIME_BITS_SIZE = 6 * Byte.SIZE;

    /** Timestamp size in bytes. */
    public static final int HYBRID_TIMESTAMP_SIZE = Long.BYTES;

    /** A constant holding the maximum value a {@code HybridTimestamp} can have. */
    public static final HybridTimestamp MAX_VALUE = new HybridTimestamp(Long.MAX_VALUE);

    /** The constant holds the minimum value which {@code HybridTimestamp} might formally have. */
    public static final HybridTimestamp MIN_VALUE = new HybridTimestamp(0L, 1);

    /**
     * Cluster cLock skew. The constant determines the undefined inclusive interval to compares timestamp from various nodes.
     * TODO: IGNITE-18978 Method to comparison timestamps with clock skew.
     */
    private static final long CLOCK_SKEW = 7L;

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
            throw new IllegalArgumentException("Physical time is out of bounds: " + physical);
        }

        if (logical < 0 || logical >= (1L << LOGICAL_TIME_BITS_SIZE)) {
            throw new IllegalArgumentException("Logical time is out of bounds: " + logical);
        }

        time = (physical << LOGICAL_TIME_BITS_SIZE) | logical;

        // Negative time breaks comparison, we don't allow overflow of the physical time.
        // "0" is a reserved value for "NULL_HYBRID_TIMESTAMP".
        if (time <= 0) {
            throw new IllegalArgumentException("Time is out of bounds: " + time);
        }
    }

    /**
     * The constructor.
     *
     * @param time Long time value.
     */
    public HybridTimestamp(long time) {
        this.time = time;

        // Negative time breaks comparison, we don't allow overflow of the physical time.
        // "0" is a reserved value for "NULL_HYBRID_TIMESTAMP".
        if (time <= 0) {
            throw new IllegalArgumentException("Time is out of bounds: " + time);
        }
    }

    /**
     * Converts primitive {@code long} representation into a hybrid timestamp instance.
     * {@link #NULL_HYBRID_TIMESTAMP} is interpreted as {@code null}.
     *
     * @throws IllegalArgumentException If timestamp is negative.
     */
    public static @Nullable HybridTimestamp nullableHybridTimestamp(long time) {
        return time == NULL_HYBRID_TIMESTAMP ? null : new HybridTimestamp(time);
    }

    /**
     * Converts primitive {@code long} representation into a hybrid timestamp instance.
     *
     * @throws IllegalArgumentException If timestamp is not positive.
     */
    public static HybridTimestamp hybridTimestamp(long time) {
        return new HybridTimestamp(time);
    }

    /**
     * Converts hybrid timestamp instance to a primitive {@code long} representation.
     * {@code null} is represented as {@link #NULL_HYBRID_TIMESTAMP}.
     */
    public static long hybridTimestampToLong(@Nullable HybridTimestamp timestamp) {
        return timestamp == null ? NULL_HYBRID_TIMESTAMP : timestamp.time;
    }

    /**
     * Finds maximum hybrid timestamp.
     *
     * @param times Times for comparing. Must not be {@code null} or empty.
     * @return The highest hybrid timestamp.
     */
    public static HybridTimestamp max(HybridTimestamp... times) {
        assert times != null;
        assert times.length > 0;

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
        return (int) (time & LOGICAL_TIME_MASK);
    }

    /**
     * Returns a compressed representation as a primitive {@code long} value.
     */
    public long longValue() {
        return time;
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

    /**
     * Compares two timestamps with the clock skew.
     * t1, t2 comparable if t1 is not contained on [t2 - CLOCK_SKEW; t2 + CLOCK_SKEW].
     * TODO: IGNITE-18978 Method to comparison timestamps with clock skew.
     *
     * @param anotherTimestamp Another timestamp.
     * @return Result of comparison can be positive or negative, or {@code 0} if timestamps are not comparable.
     */
    private int compareWithClockSkew(HybridTimestamp anotherTimestamp) {
        if (getPhysical() - CLOCK_SKEW <= anotherTimestamp.getPhysical() && getPhysical() + CLOCK_SKEW >= anotherTimestamp.getPhysical()) {
            return 0;
        }

        return compareTo(anotherTimestamp);
    }

    /**
     * Defines whether this timestamp is strictly before the given one, taking the clock skew into account.
     *
     * @param anotherTimestamp Another timestamp.
     * @return Whether this timestamp is before the given one or not.
     */
    public boolean before(HybridTimestamp anotherTimestamp) {
        return compareWithClockSkew(anotherTimestamp) < 0;
    }

    /**
     * Defines whether this timestamp is strictly after the given one, taking the clock skew into account.
     *
     * @param anotherTimestamp Another timestamp.
     * @return Whether this timestamp is after the given one or not.
     */
    public boolean after(HybridTimestamp anotherTimestamp) {
        return compareWithClockSkew(anotherTimestamp) > 0;
    }

    @Override
    public int compareTo(HybridTimestamp other) {
        return Long.compare(this.time, other.time);
    }

    @Override
    public String toString() {
        return S.toString(HybridTimestamp.class, this, "physical", getPhysical(), "logical", getLogical());
    }

    /**
     * Returns a new hybrid timestamp with incremented physical component.
     */
    public HybridTimestamp addPhysicalTime(long mills) {
        if (mills >= (1L << PHYSICAL_TIME_BITS_SIZE)) {
            throw new IllegalArgumentException("Physical time is out of bounds: " + mills);
        }

        return new HybridTimestamp(time + (mills << LOGICAL_TIME_BITS_SIZE));
    }
}
