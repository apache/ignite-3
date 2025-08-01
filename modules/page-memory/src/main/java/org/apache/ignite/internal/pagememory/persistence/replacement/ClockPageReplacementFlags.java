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

package org.apache.ignite.internal.pagememory.persistence.replacement;

import static org.apache.ignite.internal.util.GridUnsafe.compareAndSwapLong;
import static org.apache.ignite.internal.util.GridUnsafe.getLong;
import static org.apache.ignite.internal.util.GridUnsafe.putLong;
import static org.apache.ignite.internal.util.GridUnsafe.zeroMemory;

/**
 * Clock page replacement algorithm implementation.
 */
public class ClockPageReplacementFlags {
    /** Total pages count. */
    private final int pagesCnt;

    /** Index of the next candidate ("hand"). */
    private int curIdx;

    /** Pointer to memory region to store page hit flags. */
    private final long flagsPtr;

    /**
     * Constructor.
     *
     * @param totalPagesCnt Total pages count.
     * @param memPtr Pointer to memory region.
     */
    public ClockPageReplacementFlags(int totalPagesCnt, long memPtr) {
        pagesCnt = totalPagesCnt;
        flagsPtr = memPtr;

        zeroMemory(flagsPtr, (totalPagesCnt + 7) >> 3);
    }

    /**
     * Find page to replace.
     *
     * @return Page index to replace.
     */
    public int poll() {
        // This method is always executed under exclusive lock, no other synchronization or CAS required.
        while (true) {
            if (curIdx >= pagesCnt) {
                curIdx = 0;
            }

            long ptr = getPointer(curIdx);

            long flags = getLong(ptr);

            if (((curIdx & 63) == 0) && (flags == ~0L)) {
                putLong(ptr, 0L);

                curIdx += 64;

                continue;
            }

            long mask = ~0L << curIdx;

            int bitIdx = Long.numberOfTrailingZeros(~flags & mask);

            if (bitIdx == 64) {
                putLong(ptr, flags & ~mask);

                curIdx = (curIdx & ~63) + 64;
            } else {
                mask &= ~(~0L << bitIdx);

                putLong(ptr, flags & ~mask);

                curIdx = (curIdx & ~63) + bitIdx + 1;

                if (curIdx <= pagesCnt) {
                    return curIdx - 1;
                }
            }
        }
    }

    /**
     * Returns a pointer to the bitset that corresponds to provided page index.
     *
     * <p>Matches {@code this.flagsPtr + (pageIdx >> log2(Long.SIZE) << log2(Byte.SIZE))}, i.e. points to a {@code long} value, among which
     * there's a bit that identifies given {@code pageIdx}.
     */
    private long getPointer(int pageIdx) {
        return flagsPtr + ((pageIdx >> 3) & (~7L));
    }

    /**
     * Get page hit flag.
     *
     * @param pageIdx Page index.
     */
    boolean getFlag(int pageIdx) {
        long flags = getLong(getPointer(pageIdx));

        return (flags & (1L << pageIdx)) != 0L;
    }

    /**
     * Clear page hit flag.
     *
     * @param pageIdx Page index.
     */
    public void clearFlag(int pageIdx) {
        long ptr = getPointer(pageIdx);

        long oldFlags;
        long newFlags;
        long mask = ~(1L << pageIdx);

        do {
            oldFlags = getLong(ptr);
            newFlags = oldFlags & mask;

            if (oldFlags == newFlags) {
                return;
            }
        } while (!compareAndSwapLong(null, ptr, oldFlags, newFlags));
    }

    /**
     * Set page hit flag.
     *
     * @param pageIdx Page index.
     */
    public void setFlag(int pageIdx) {
        long ptr = getPointer(pageIdx);

        long oldFlags;
        long newFlags;
        long mask = 1L << pageIdx;

        do {
            oldFlags = getLong(ptr);
            newFlags = oldFlags | mask;

            if (oldFlags == newFlags) {
                return;
            }
        } while (!compareAndSwapLong(null, ptr, oldFlags, newFlags));
    }

    /**
     * Memory required to service {@code pagesCnt} pages.
     *
     * @param pagesCnt Pages count.
     */
    public static long requiredMemory(int pagesCnt) {
        return ((pagesCnt + 63) / 8) & (~7L) /* 1 bit per page + 8 byte align */;
    }
}
