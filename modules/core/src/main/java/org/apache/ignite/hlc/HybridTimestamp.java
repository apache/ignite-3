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

import java.io.Serializable;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

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
        // Value -1 is used in "org.apache.ignite.hlc.HybridClock.update" to produce "0" after the increment.
        // Real usable value cannot be negative.
        assert logical >= -1 : logical;

        this.physical = physical;
        this.logical = logical;
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

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof HybridTimestamp)) {
            return false;
        }

        return compareTo((HybridTimestamp) o) == 0;
    }

    /** {@inheritDoc} */
    @Override
    public int compareTo(HybridTimestamp other) {
        if (this.physical == other.physical) {
            return Integer.compare(this.logical, other.logical);
        }

        return Long.compare(this.physical, other.physical);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(HybridTimestamp.class, this);
    }
}
