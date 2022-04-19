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
public class UuidIgniteRowId implements IgniteRowId {
    /** Backing uuid value. */
    private final UUID uuid;

    /**
     * Constructor.
     *
     * @param uuid UUID.
     */
    public UuidIgniteRowId(UUID uuid) {
        this.uuid = uuid;
    }

    /**
     * Returns {@link UuidIgniteRowId} instance based on {@link UUID#randomUUID()}.
     */
    public static IgniteRowId randomRowId() {
        return new UuidIgniteRowId(UUID.randomUUID());
    }

    /** {@inheritDoc} */
    @Override
    public void writeTo(ByteBuffer buf, boolean signedBytesCompare) {
        assert buf.order() == ByteOrder.LITTLE_ENDIAN;

        long mask = signedBytesCompare ? 0x0080808080808080L : 0x8000000000000000L;

        buf.putLong(Long.reverseBytes(mask ^ uuid.getMostSignificantBits()));
        buf.putLong(Long.reverseBytes(mask ^ uuid.getLeastSignificantBits()));
    }

    /** {@inheritDoc} */
    @Override
    public int compare(ByteBuffer buf, boolean signedBytesCompare) {
        assert buf.order() == ByteOrder.LITTLE_ENDIAN;

        long mask = signedBytesCompare ? 0x0080808080808080L : 0x8000000000000000L;

        int cmp = Long.compare(uuid.getMostSignificantBits(), mask ^ Long.reverseBytes(buf.getLong()));

        if (cmp != 0) {
            return cmp;
        }

        return Long.compare(uuid.getLeastSignificantBits(), mask ^ Long.reverseBytes(buf.getLong()));
    }

    /** {@inheritDoc} */
    @Override
    public int compareTo(@NotNull IgniteRowId o) {
        if (!(o instanceof UuidIgniteRowId)) {
            throw new IllegalArgumentException(o.getClass().getName());
        }

        UuidIgniteRowId that = (UuidIgniteRowId) o;

        int cmp = Long.compare(uuid.getMostSignificantBits(), that.uuid.getMostSignificantBits());

        if (cmp != 0) {
            return cmp;
        }

        return Long.compare(uuid.getLeastSignificantBits(), that.uuid.getLeastSignificantBits());
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return uuid.toString();
    }
}
