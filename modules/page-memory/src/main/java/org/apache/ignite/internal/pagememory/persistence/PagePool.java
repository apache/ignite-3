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

package org.apache.ignite.internal.pagememory.persistence;

import static org.apache.ignite.internal.pagememory.persistence.PageHeader.fullPageId;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.initNew;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.isAcquired;
import static org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory.INVALID_REL_PTR;
import static org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory.PAGE_LOCK_OFFSET;
import static org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory.RELATIVE_PTR_MASK;
import static org.apache.ignite.internal.util.GridUnsafe.compareAndSwapLong;
import static org.apache.ignite.internal.util.GridUnsafe.getLongVolatile;
import static org.apache.ignite.internal.util.GridUnsafe.putLong;
import static org.apache.ignite.internal.util.GridUnsafe.putLongVolatile;
import static org.apache.ignite.internal.util.StringUtils.hexLong;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.pagememory.mem.DirectMemoryRegion;
import org.apache.ignite.internal.util.OffheapReadWriteLock;

/**
 * Page pool.
 */
public class PagePool {
    /** Relative pointer chunk index mask. */
    static final long SEGMENT_INDEX_MASK = 0xFFFFFF0000000000L;

    /** Address mask to avoid ABA problem. */
    private static final long ADDRESS_MASK = 0xFFFFFFFFFFFFFFL;

    /** Counter increment to avoid ABA problem. */
    private static final long COUNTER_INC = ADDRESS_MASK + 1;

    /** Counter mask to avoid ABA problem. */
    private static final long COUNTER_MASK = ~ADDRESS_MASK;

    /** Segment index. */
    protected final int idx;

    /** Direct memory region. */
    protected final DirectMemoryRegion region;

    /** Pool pages counter. */
    protected final AtomicInteger pagesCntr = new AtomicInteger();

    /** Index of the last allocated page pointer. */
    protected long lastAllocatedIdxPtr;

    /** Pointer to the address of the free page list. */
    protected long freePageListPtr;

    /** Pages base. */
    protected long pagesBase;

    /** System page size. */
    private final int sysPageSize;

    /** Instance of RW Lock Updater. */
    private OffheapReadWriteLock rwLock;

    /**
     * Constructor.
     *
     * @param idx Index.
     * @param region Region
     * @param sysPageSize System page size.
     * @param rwLock Instance of RW Lock Updater.
     */
    protected PagePool(
            int idx,
            DirectMemoryRegion region,
            int sysPageSize,
            OffheapReadWriteLock rwLock
    ) {
        this.idx = idx;
        this.region = region;
        this.sysPageSize = sysPageSize;
        this.rwLock = rwLock;

        long base = (region.address() + 7) & ~0x7;

        freePageListPtr = base;

        base += 8;

        lastAllocatedIdxPtr = base;

        base += 8;

        // Align page start by
        pagesBase = base;

        putLong(freePageListPtr, INVALID_REL_PTR);
        putLong(lastAllocatedIdxPtr, 0L);
    }

    /**
     * Allocates a new free page.
     *
     * @param tag Tag to initialize page RW lock.
     * @return Relative pointer to the allocated page.
     */
    public long borrowOrAllocateFreePage(int tag) {
        long relPtr = borrowFreePage();

        if (relPtr == INVALID_REL_PTR) {
            relPtr = allocateFreePage(tag);
        }

        if (relPtr != INVALID_REL_PTR && pagesCntr != null) {
            pagesCntr.incrementAndGet();
        }

        return relPtr;
    }

    /**
     * Returns relative pointer to a free page that was borrowed from the allocated pool.
     */
    private long borrowFreePage() {
        while (true) {
            long freePageRelPtrMasked = getLongVolatile(null, freePageListPtr);

            long freePageRelPtr = freePageRelPtrMasked & ADDRESS_MASK;

            if (freePageRelPtr != INVALID_REL_PTR) {
                long freePageAbsPtr = absolute(freePageRelPtr);

                long nextFreePageRelPtr = getLongVolatile(null, freePageAbsPtr) & ADDRESS_MASK;

                // nextFreePageRelPtr may be invalid because a concurrent thread may have already polled this value
                // and used it.
                long cnt = ((freePageRelPtrMasked & COUNTER_MASK) + COUNTER_INC) & COUNTER_MASK;

                if (compareAndSwapLong(null, freePageListPtr, freePageRelPtrMasked, nextFreePageRelPtr | cnt)) {
                    putLongVolatile(null, freePageAbsPtr, PageHeader.PAGE_MARKER);

                    return freePageRelPtr;
                }
            } else {
                return INVALID_REL_PTR;
            }
        }
    }

    /**
     * Allocates a new free page.
     *
     * @param tag Tag to initialize page RW lock.
     * @return Relative pointer of the allocated page.
     */
    private long allocateFreePage(int tag) {
        long limit = region.address() + region.size();

        while (true) {
            long lastIdx = getLongVolatile(null, lastAllocatedIdxPtr);

            // Check if we have enough space to allocate a page.
            if (pagesBase + (lastIdx + 1) * sysPageSize > limit) {
                return INVALID_REL_PTR;
            }

            if (compareAndSwapLong(null, lastAllocatedIdxPtr, lastIdx, lastIdx + 1)) {
                long absPtr = pagesBase + lastIdx * sysPageSize;

                assert (lastIdx & SEGMENT_INDEX_MASK) == 0L;

                long relative = relative(lastIdx);

                assert relative != INVALID_REL_PTR;

                initNew(absPtr, relative);

                rwLock.init(absPtr + PAGE_LOCK_OFFSET, tag);

                return relative;
            }
        }
    }

    /**
     * Returns resulting number of pages in pool if pages counter is enabled, 0 otherwise.
     *
     * @param relPtr Relative pointer to free.
     */
    public int releaseFreePage(long relPtr) {
        long absPtr = absolute(relPtr);

        assert !isAcquired(absPtr) : "Release pinned page: " + fullPageId(absPtr);

        int resCntr = 0;

        if (pagesCntr != null) {
            resCntr = pagesCntr.decrementAndGet();
        }

        while (true) {
            long freePageRelPtrMasked = getLongVolatile(null, freePageListPtr);

            long freePageRelPtr = freePageRelPtrMasked & RELATIVE_PTR_MASK;

            putLongVolatile(null, absPtr, freePageRelPtr);

            long cnt = freePageRelPtrMasked & COUNTER_MASK;

            long relPtrWithCnt = (relPtr & ADDRESS_MASK) | cnt;

            if (compareAndSwapLong(null, freePageListPtr, freePageRelPtrMasked, relPtrWithCnt)) {
                return resCntr;
            }
        }
    }

    /**
     * Returns absolute pointer.
     *
     * @param relativePtr Relative pointer.
     */
    long absolute(long relativePtr) {
        int segIdx = (int) ((relativePtr >> 40) & 0xFFFF);

        assert segIdx == idx : "expected=" + idx + ", actual=" + segIdx + ", relativePtr=" + hexLong(relativePtr);

        long pageIdx = relativePtr & ~SEGMENT_INDEX_MASK;

        long off = pageIdx * sysPageSize;

        return pagesBase + off;
    }

    /**
     * Returns relative pointer.
     *
     * @param pageIdx Page index in the pool.
     */
    long relative(long pageIdx) {
        return pageIdx | ((long) idx) << 40;
    }

    /**
     * Returns page index in the pool.
     *
     * @param relPtr Relative pointer.
     */
    long pageIndex(long relPtr) {
        return relPtr & ~SEGMENT_INDEX_MASK;
    }

    /**
     * Returns max number of pages in the pool.
     */
    public int pages() {
        return (int) ((region.size() - (pagesBase - region.address())) / sysPageSize);
    }

    /**
     * Returns number of pages in the list.
     */
    public int size() {
        return pagesCntr.get();
    }
}
