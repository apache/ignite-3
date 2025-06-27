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

package org.apache.ignite.internal.metastorage.server.persistence;

import java.util.Arrays;
import org.apache.ignite.internal.util.HashUtils;

/**
 * Simple bloom-filter-based utility class to be used to cooperate compaction thread with an FSM thread. For a full explanation please
 * consider reading comments in the {@link RocksDbKeyValueStorage#compact(long)} implementation.
 */
class WriteBatchProtector {
    /**
     * Signifies that there are 2^14 bits in this bloom filter, which is roughly equal to 16 thousand. This is a reasonably large value.
     */
    private static final int BLOOM_BITS = 14;
    private static final int BLOOM_MASK = (1 << BLOOM_BITS) - 1;

    // Constants for least significant bits (LSB) of hash values. These bits would point to individual bit positions in "long" values.
    private static final int LSB_BITS = Integer.numberOfTrailingZeros(Long.SIZE);
    private static final int LSB_MASK = Long.SIZE - 1;

    /** Bit-set. */
    private final long[] bloom = new long[1 << BLOOM_BITS >>> LSB_BITS];

    /**
     * Called when {@code key} is updated in the storage. Immediate consecutive call of {@code maybeUpdated(key)} will return {@code true}.
     */
    public void onUpdate(byte[] key) {
        int h = hash(key);

        int msb = indexInsideArray(h);
        int lsb = indexInsideLongValue(h);
        bloom[msb] |= singleBitInLong(lsb);
    }

    /**
     * Checks if the {@code key} was probably updated in the storage. Returns {@code false} if it was definitely not updated. Returns
     * {@code true} if the answer is unknown.
     */
    public boolean maybeUpdated(byte[] key) {
        int h = hash(key);

        int msb = indexInsideArray(h);
        int lsb = indexInsideLongValue(h);
        return (bloom[msb] & singleBitInLong(lsb)) != 0L;
    }

    private static int indexInsideArray(int h) {
        return h >>> LSB_BITS;
    }

    private static int indexInsideLongValue(int h) {
        return h & LSB_MASK;
    }

    private static long singleBitInLong(int lsb) {
        return 1L << lsb;
    }

    /**
     * Clears all the information about previous calls of {@link #onUpdate(byte[])}. All calls of {@link #maybeUpdated(byte[])} will return
     * {@code false} after this call.
     */
    public void clear() {
        Arrays.fill(bloom, 0L);
    }

    private static int hash(byte[] key) {
        return HashUtils.hash32(key) & BLOOM_MASK;
    }
}
