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

/**
 * A hybrid timestamp that combines physical clock and logical clock.
 */
public final class HybridTimestamp implements Comparable<HybridTimestamp>, Serializable {
    /** Serial version UID. */
    private static final long serialVersionUID = 2459861612869605904L;

    /** Timestamp size in bytes. */
    public static final int HYBRID_TIMESTAMP_SIZE = Long.BYTES + Integer.BYTES;

    /** A constant holding the maximum value a {@code HybridTimestamp} can have. */
    public static final HybridTimestamp MAX_VALUE = new HybridTimestamp(Long.MAX_VALUE, Integer.MAX_VALUE);

    /** The constant holds the minimum value which {@code HybridTimestamp} might formally have. */
    public static final HybridTimestamp MIN_VALUE = new HybridTimestamp(1L, -1);

    /**
     * Cluster cLock skew. The constant determines the undefined inclusive interval to compares timestamp from various nodes.
     * TODO: IGNITE-18978 Method to comparison timestamps with clock skew.
     */
    private static final long CLOCK_SKEW = 7L;

    /** Physical clock. */
    private final long physical;

    /** Logical clock. */
    private final int logical;

    /**
     * The constructor.
     *
     * @param physical The physical time.
     * @param logical The logical time.
     */
    public HybridTimestamp(long physical, int logical) {
        assert physical > 0 : physical;
        // Value -1 is used in "org.apache.ignite.internal.hlc.HybridClock.update" to produce "0" after the increment.
        // Real usable value cannot be negative.
        assert logical >= -1 : logical;

        this.physical = physical;
        this.logical = logical;
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
        return physical;
    }

    /**
     * Returns a logical component.
     *
     * @return The logical component.
     */
    public int getLogical() {
        assert logical >= 0;

        return logical;
    }

    /**
     * Returns a new hybrid timestamp with incremented logical component.
     *
     * @return The hybrid timestamp.
     */
    public HybridTimestamp addTicks(int ticks) {
        return new HybridTimestamp(physical, this.logical + ticks);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HybridTimestamp that = (HybridTimestamp) o;

        if (physical != that.physical) {
            return false;
        }
        return logical == that.logical;
    }

    @Override
    public int hashCode() {
        int result = (int) (physical ^ (physical >>> 32));
        result = 31 * result + logical;
        return result;
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
        if (this.physical == other.physical) {
            return Integer.compare(this.logical, other.logical);
        }

        return Long.compare(this.physical, other.physical);
    }

    @Override
    public String toString() {
        return S.toString(HybridTimestamp.class, this);
    }
}
