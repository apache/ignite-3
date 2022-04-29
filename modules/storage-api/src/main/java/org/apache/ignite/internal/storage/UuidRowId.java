/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

/**
 * UUID-based ignite row id implementation.
 */
public final class UuidRowId implements RowId {
    /*
     * The most significant 64 bits.
     */
    private final long mostSigBits;

    /*
     * The least significant 64 bits.
     */
    private final long leastSigBits;

    /** Private constructor. */
    private UuidRowId(long mostSigBits, long leastSigBits) {
        this.mostSigBits = mostSigBits;
        this.leastSigBits = leastSigBits;
    }

    /**
     * Returns {@link UuidRowId} instance based on {@link UUID#randomUUID()}. Highest two bytes of the most significant bits part will
     * encode partition id.
     *
     * @param partitionId Partition id.
     */
    //TODO IGNITE-16912 Add the ability to use non-random UUIDs from newer specifications, like UUIDv8, for example.
    public static RowId randomRowId(int partitionId) {
        UUID randomUuid = UUID.randomUUID();

        long lsb = randomUuid.getLeastSignificantBits();

        lsb = (lsb & ~0xFFFFL) | partitionId;

        return new UuidRowId(randomUuid.getMostSignificantBits(), lsb);
    }

    /** {@inheritDoc} */
    @Override
    public int partitionId() {
        return (int) (leastSigBits & 0xFFFFL);
    }

    /**
     * Returns most significant bits as a long value.
     */
    public long mostSignificantBits() {
        return mostSigBits;
    }

    /**
     * Returns least significant bits as a long value.
     */
    public long leastSignificantBits() {
        return leastSigBits;
    }

    /**
     * Writes row id into a byte buffer. Binary row representation should match natural order defined by {@link #compareTo(Object)} when
     * comparing lexicographically.
     *
     * @param buf Output byte buffer with {@link java.nio.ByteOrder#BIG_ENDIAN} byte order.
     */
    public void writeTo(ByteBuffer buf) {
        assert buf.order() == ByteOrder.BIG_ENDIAN;

        buf.putLong(mostSigBits);
        buf.putLong(leastSigBits);
    }

    /**
     * Compares row id with a byte buffer, previously written by a {@link #writeTo(ByteBuffer)} method.
     *
     * @param buf Input byte buffer with {@link java.nio.ByteOrder#BIG_ENDIAN} byte order.
     * @return {@code true} if buffer contains same value.
     */
    public boolean matches(ByteBuffer buf) {
        assert buf.order() == ByteOrder.BIG_ENDIAN;

        return mostSigBits == buf.getLong() && leastSigBits == buf.getLong();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return new UUID(mostSigBits, leastSigBits).toString();
    }
}
