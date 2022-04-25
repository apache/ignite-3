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
import org.jetbrains.annotations.NotNull;

/**
 * UUID-based ignite row id implementation.
 */
public final class UuidIgniteRowId implements IgniteRowId {
    /*
     * The most significant 64 bits.
     */
    private final long mostSigBits;

    /*
     * The least significant 64 bits.
     */
    private final long leastSigBits;

    /**
     * Constructor.
     *
     * @param uuid UUID.
     */
    public UuidIgniteRowId(UUID uuid) {
        this(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    /** Private constructor. */
    private UuidIgniteRowId(long mostSigBits, long leastSigBits) {
        this.mostSigBits = mostSigBits;
        this.leastSigBits = leastSigBits;
    }

    /**
     * Returns {@link UuidIgniteRowId} instance based on {@link UUID#randomUUID()}. Highest two bytes of the most significant bits part will
     * encode partition id.
     *
     * @param partitionId Partition id.
     */
    public static IgniteRowId randomRowId(int partitionId) {
        UUID randomUuid = UUID.randomUUID();

        long lsb = randomUuid.getLeastSignificantBits();

        lsb = (lsb & ~0xFFFFL) | partitionId;

        return new UuidIgniteRowId(randomUuid.getMostSignificantBits(), lsb);
    }

    /** {@inheritDoc} */
    @Override
    public int partitionId() {
        return (int) (leastSigBits & 0xFFFFL);
    }

    /**
     * Writes row id into a byte buffer. Binary row representation should match natural order defined by {@link #compareTo(Object)} when
     * comparing lexicographically.
     *
     * @param buf Output byte buffer with {@link java.nio.ByteOrder#BIG_ENDIAN} byte order.
     * @param signedBytesCompare Defines properties of a target binary comparator. {@code true} if bytes are compared as signed values,
     *      {@code false} if unsigned.
     */
    public void writeTo(ByteBuffer buf, boolean signedBytesCompare) {
        assert buf.order() == ByteOrder.BIG_ENDIAN;

        long mask = longBytesSignsMask(signedBytesCompare);

        buf.putLong(mask ^ mostSigBits);
        buf.putLong(mask ^ leastSigBits);
    }

    /**
     * Compares row id with a byte buffer, previously written by a {@link #writeTo(ByteBuffer, boolean)} method.
     *
     * @param buf Input byte buffer with {@link java.nio.ByteOrder#BIG_ENDIAN} byte order.
     * @param signedBytesCompare Defines properties of a binary comparator. {@code true} if bytes are compared as signed values,
     *      {@code false} if unsigned.
     * @return A negative integer, zero, or a positive integer as this row id is less than, equal to, or greater than the specified row id.
     */
    public int compareTo(ByteBuffer buf, boolean signedBytesCompare) {
        assert buf.order() == ByteOrder.BIG_ENDIAN;

        long mask = longBytesSignsMask(signedBytesCompare);

        int cmp = Long.compare(mostSigBits, mask ^ buf.getLong());

        if (cmp != 0) {
            return cmp;
        }

        return Long.compare(leastSigBits, mask ^ buf.getLong());
    }

    /** {@inheritDoc} */
    @Override
    public int compareTo(@NotNull IgniteRowId o) {
        if (!(o instanceof UuidIgniteRowId)) {
            throw new IllegalArgumentException(
                    "Compare can only be performed with instances of " + getClass().getName()
                            + ", but a parameter with the following class has been passed: " + o.getClass().getName()
            );
        }

        UuidIgniteRowId that = (UuidIgniteRowId) o;

        int cmp = Long.compare(mostSigBits, that.mostSigBits);

        if (cmp != 0) {
            return cmp;
        }

        return Long.compare(leastSigBits, that.leastSigBits);
    }

    /**
     * Returns a mask to be xored with long value to make its bytes comparable in lexicographical order when written in Big Endian format.
     * {@code signedBytesCompare == false} means that all bytes should be compared as unsigned values. This holds true for 7 out of 8 long
     * bytes. Most significant byte has a sign bit. This bit needs to be flipped to convert a signed byte range of {@code [-128:127]} into
     * an unsigned range of {@code [0:255]}.
     * <p/>
     * {@code signedBytesCompare == true} means that we have to flip the most significant bit of other 7 bytes.
     */
    private long longBytesSignsMask(boolean signedBytesCompare) {
        return signedBytesCompare
                ? 0x0080808080808080L
                : 0x8000000000000000L;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return new UUID(mostSigBits, leastSigBits).toString();
    }
}
