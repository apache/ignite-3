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

package org.apache.ignite.internal.pagememory.freelist;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.getInteger;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagememory.freelist.io.PagesListNodeIo.T_PAGE_LIST_NODE;
import static org.apache.ignite.internal.pagememory.io.PageIo.getPageId;
import static org.apache.ignite.internal.pagememory.io.PageIo.getType;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.MAX_ITEM_ID_NUM;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.changeFlag;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;
import static org.apache.ignite.internal.util.ArrayUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.ArrayUtils.remove;
import static org.apache.ignite.internal.util.IgniteUtils.isPow2;
import static org.apache.ignite.internal.util.StringUtils.hexLong;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.pagememory.PageIdAllocator;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.datastructure.DataStructure;
import org.apache.ignite.internal.pagememory.freelist.io.PagesListMetaIo;
import org.apache.ignite.internal.pagememory.freelist.io.PagesListNodeIo;
import org.apache.ignite.internal.pagememory.io.DataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.reuse.ReuseBag;
import org.apache.ignite.internal.pagememory.util.PageHandler;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.pagememory.util.PageLockListener;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Striped doubly-linked list of page IDs optionally organized in buckets.
 */
public abstract class PagesList extends DataStructure {
    /** Count of tries to lock stripe before fail back to blocking lock. */
    public static final String IGNITE_PAGES_LIST_TRY_LOCK_ATTEMPTS = "IGNITE_PAGES_LIST_TRY_LOCK_ATTEMPTS";

    /** Maximum count of the stripes. */
    public static final String IGNITE_PAGES_LIST_STRIPES_PER_BUCKET = "IGNITE_PAGES_LIST_STRIPES_PER_BUCKET";

    /**
     * Disable onheap caching of pages lists (free lists and reuse lists). If persistence is enabled changes to page lists are not stored to
     * page memory immediately, they are cached in onheap buffer and flushes to page memory on a checkpoint. This property allows to disable
     * such onheap caching.
     */
    public static final String IGNITE_PAGES_LIST_DISABLE_ONHEAP_CACHING = "IGNITE_PAGES_LIST_DISABLE_ONHEAP_CACHING";

    private final int tryLockAttempts = getInteger(IGNITE_PAGES_LIST_TRY_LOCK_ATTEMPTS, 10);

    private final int maxStripesPerBucket = getInteger(
            IGNITE_PAGES_LIST_STRIPES_PER_BUCKET,
            Math.max(8, Runtime.getRuntime().availableProcessors())
    );

    private final boolean pagesListCachingDisabled = getBoolean(IGNITE_PAGES_LIST_DISABLE_ONHEAP_CACHING, false);

    /** Logger. */
    protected final IgniteLogger log;

    /** Basket sizes. */
    protected final AtomicLongArray bucketsSize;

    /** Flag indicating that the {@link PagesList} has been changed. */
    protected volatile boolean changed;

    /** Page cache changed. */
    protected volatile boolean pageCacheChanged;

    /** Page ID to store list metadata. */
    private final long metaPageId;

    /** Number of buckets. */
    private final int buckets;

    /** Flag to enable/disable onheap list caching. */
    private volatile boolean onheapListCachingEnabled;

    private final PageHandler<Void, Boolean> cutTail = new CutTail();

    private final PageHandler<Void, Boolean> putBucket = new PutBucket();

    private final class CutTail implements PageHandler<Void, Boolean> {
        /** {@inheritDoc} */
        @Override
        public Boolean run(
                int cacheId,
                long pageId,
                long page,
                long pageAddr,
                PageIo iox,
                Void ignore,
                int bucket,
                IoStatisticsHolder statHolder
        ) {
            assert getPageId(pageAddr) == pageId;

            PagesListNodeIo io = (PagesListNodeIo) iox;

            long tailId = io.getNextId(pageAddr);

            assert tailId != 0;

            io.setNextId(pageAddr, 0L);

            updateTail(bucket, tailId, pageId);

            return TRUE;
        }
    }

    private final class PutBucket implements PageHandler<Void, Boolean> {
        /** {@inheritDoc} */
        @Override
        public Boolean run(
                int cacheId,
                long pageId,
                long page,
                long pageAddr,
                PageIo iox,
                Void ignore,
                int oldBucket,
                IoStatisticsHolder statHolder
        ) throws IgniteInternalCheckedException {
            decrementBucketSize(oldBucket);

            // Recalculate bucket because page free space can be changed concurrently.
            int freeSpace = ((DataPageIo) iox).getFreeSpace(pageAddr);

            int newBucket = getBucketIndex(freeSpace);

            if (newBucket != oldBucket) {
                log.debug("Bucket changed when moving from heap to PageMemory [list={}, oldBucket={}, newBucket={}, pageId={}]",
                        name(), oldBucket, newBucket, pageId);
            }

            if (newBucket >= 0) {
                put(null, pageId, pageAddr, newBucket, statHolder);
            }

            return TRUE;
        }
    }

    /**
     * Constructor.
     *
     * @param name Structure name (for debug purpose).
     * @param grpId Group ID.
     * @param partId Partition ID.
     * @param pageMem Page memory.
     * @param lockLsnr Page lock listener.
     * @param log Logger.
     * @param buckets Number of buckets.
     * @param metaPageId Metadata page ID.
     */
    protected PagesList(
            String name,
            int grpId,
            int partId,
            PageMemory pageMem,
            PageLockListener lockLsnr,
            IgniteLogger log,
            int buckets,
            long metaPageId
    ) {
        super(name, grpId, null, partId, pageMem, lockLsnr, FLAG_AUX);

        this.log = log;

        this.buckets = buckets;
        this.metaPageId = metaPageId;

        onheapListCachingEnabled = isCachingApplicable();

        bucketsSize = new AtomicLongArray(buckets);
    }

    /**
     * Initializes a {@link PagesList}.
     *
     * @param metaPageId Metadata page ID.
     * @param initNew {@code True} if new list if created, {@code false} if should be initialized from metadata.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected final void init(long metaPageId, boolean initNew) throws IgniteInternalCheckedException {
        if (metaPageId != 0L) {
            if (initNew) {
                init(metaPageId, PagesListMetaIo.VERSIONS.latest());
            } else {
                Map<Integer, LongArrayList> bucketsData = new HashMap<>();

                long nextId = metaPageId;

                while (nextId != 0) {
                    final long pageId = nextId;
                    final long page = acquirePage(pageId, IoStatisticsHolderNoOp.INSTANCE);

                    try {
                        long pageAddr = readLock(pageId, page); // No concurrent recycling on init.

                        assert pageAddr != 0L;

                        try {
                            PagesListMetaIo io = PagesListMetaIo.VERSIONS.forPage(pageAddr);

                            io.getBucketsData(pageAddr, bucketsData);

                            nextId = io.getNextMetaPageId(pageAddr);

                            assert nextId != pageId : "Loop detected [next=" + hexLong(nextId) + ", cur=" + hexLong(pageId) + ']';
                        } finally {
                            readUnlock(pageId, page, pageAddr);
                        }
                    } finally {
                        releasePage(pageId, page);
                    }
                }

                for (Map.Entry<Integer, LongArrayList> e : bucketsData.entrySet()) {
                    int bucket = e.getKey();
                    long bucketSize = 0;

                    Stripe[] old = getBucket(bucket);
                    assert old == null;

                    long[] upd = e.getValue().toLongArray();

                    Stripe[] tails = new Stripe[upd.length];

                    for (int i = 0; i < upd.length; i++) {
                        long tailId = upd[i];

                        long prevId = tailId;
                        int cnt = 0;

                        while (prevId != 0L) {
                            final long pageId = prevId;
                            final long page = acquirePage(pageId, IoStatisticsHolderNoOp.INSTANCE);
                            try {
                                long pageAddr = readLock(pageId, page);

                                assert pageAddr != 0L;

                                try {
                                    PagesListNodeIo io = PagesListNodeIo.VERSIONS.forPage(pageAddr);

                                    cnt += io.getCount(pageAddr);
                                    prevId = io.getPreviousId(pageAddr);

                                    // In reuse bucket the page itself can be used as a free page.
                                    if (isReuseBucket(bucket) && prevId != 0L) {
                                        cnt++;
                                    }
                                } finally {
                                    readUnlock(pageId, page, pageAddr);
                                }
                            } finally {
                                releasePage(pageId, page);
                            }
                        }

                        Stripe stripe = new Stripe(tailId, cnt == 0);
                        tails[i] = stripe;
                        bucketSize += cnt;
                    }

                    boolean ok = casBucket(bucket, null, tails);

                    assert ok;

                    bucketsSize.set(bucket, bucketSize);
                }
            }
        }
    }

    /**
     * Returns {@code True} if onheap caching is applicable for this pages list, or {@code false} if caching is disabled explicitly by
     * system property or if page list belongs to in-memory data region (in this case onheap caching makes no sense).
     */
    private boolean isCachingApplicable() {
        return !pagesListCachingDisabled;
    }

    /**
     * Save metadata without exclusive lock on it.
     *
     * @param statHolder Statistics holder to track IO operations.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected void saveMetadata(IoStatisticsHolder statHolder) throws IgniteInternalCheckedException {
        long nextPageId = metaPageId;

        assert nextPageId != 0;

        flushBucketsCache(statHolder);

        if (!changed) {
            return;
        }

        // This guaranteed that any concurrently changes of list will be detected.
        changed = false;

        try {
            long unusedPageId = writeFreeList(nextPageId);

            markUnusedPagesDirty(unusedPageId);
        } catch (Throwable e) {
            changed = true; // Return changed flag due to exception.

            throw e;
        }
    }

    /**
     * Flush onheap cached pages lists to page memory.
     *
     * @param statHolder Statistic holder.
     * @throws IgniteInternalCheckedException If failed to write a page.
     */
    private void flushBucketsCache(IoStatisticsHolder statHolder) throws IgniteInternalCheckedException {
        if (!isCachingApplicable() || !pageCacheChanged) {
            return;
        }

        pageCacheChanged = false;

        onheapListCachingEnabled = false;

        int lockedPages = 0;

        try {
            for (int bucket = 0; bucket < buckets; bucket++) {
                PagesCache pagesCache = getBucketCache(bucket, false);

                if (pagesCache == null) {
                    continue;
                }

                LongArrayList pages = pagesCache.flush();

                if (pages != null) {
                    log.debug("Move pages from heap to PageMemory [list={}, bucket={}, pages={}]", name(), bucket, pages);

                    for (int i = 0; i < pages.size(); i++) {
                        long pageId = pages.getLong(i);

                        log.debug("Move page from heap to PageMemory [list={}, bucket={}, pageId={}]", name(), bucket, pageId);

                        Boolean res = write(pageId, putBucket, bucket, null, statHolder);

                        if (res == null) {
                            // Return page to onheap pages list if can't lock it.
                            pagesCache.add(pageId);

                            lockedPages++;
                        }
                    }
                }
            }
        } finally {
            onheapListCachingEnabled = true;
        }

        if (lockedPages != 0) {
            log.info("Several pages were locked and weren't flushed on disk [grp={}, lockedPages={}]", grpName, lockedPages);

            pageCacheChanged = true;
        }
    }

    /**
     * Write free list data to page memory.
     *
     * @param nextPageId First free page id.
     * @return Unused free list page id.
     * @throws IgniteInternalCheckedException If failed.
     */
    private long writeFreeList(long nextPageId) throws IgniteInternalCheckedException {
        long curId = 0L;
        long curPage = 0L;
        long curAddr = 0L;

        PagesListMetaIo curIo = null;

        try {
            for (int bucket = 0; bucket < buckets; bucket++) {
                Stripe[] tails = getBucket(bucket);

                if (tails != null) {
                    int tailIdx = 0;

                    while (tailIdx < tails.length) {
                        int written = curPage != 0L ? curIo.addTails(pageMem.realPageSize(grpId), curAddr, bucket, tails, tailIdx) : 0;

                        if (written == 0) {
                            if (nextPageId == 0L) {
                                nextPageId = allocatePageNoReuse();

                                if (curPage != 0L) {
                                    curIo.setNextMetaPageId(curAddr, nextPageId);

                                    releaseAndWriteUnlock(curId, curPage, curAddr);
                                }

                                curId = nextPageId;
                                curPage = acquirePage(curId, IoStatisticsHolderNoOp.INSTANCE);
                                curAddr = writeLock(curId, curPage);

                                curIo = PagesListMetaIo.VERSIONS.latest();

                                curIo.initNewPage(curAddr, curId, pageSize());
                            } else {
                                releaseAndWriteUnlock(curId, curPage, curAddr);

                                curId = nextPageId;
                                curPage = acquirePage(curId, IoStatisticsHolderNoOp.INSTANCE);
                                curAddr = writeLock(curId, curPage);

                                curIo = PagesListMetaIo.VERSIONS.forPage(curAddr);

                                curIo.resetCount(curAddr);
                            }

                            nextPageId = curIo.getNextMetaPageId(curAddr);
                        } else {
                            tailIdx += written;
                        }
                    }
                }
            }
        } finally {
            releaseAndWriteUnlock(curId, curPage, curAddr);
        }

        return nextPageId;
    }

    /**
     * Mark unused pages as dirty.
     *
     * @param nextPageId First unused page.
     * @throws IgniteInternalCheckedException If failed.
     */
    private void markUnusedPagesDirty(long nextPageId) throws IgniteInternalCheckedException {
        while (nextPageId != 0L) {
            long pageId = nextPageId;

            long page = acquirePage(pageId, IoStatisticsHolderNoOp.INSTANCE);
            try {
                long pageAddr = writeLock(pageId, page);

                try {
                    PagesListMetaIo io = PagesListMetaIo.VERSIONS.forPage(pageAddr);

                    io.resetCount(pageAddr);

                    nextPageId = io.getNextMetaPageId(pageAddr);
                } finally {
                    writeUnlock(pageId, page, pageAddr, true);
                }
            } finally {
                releasePage(pageId, page);
            }
        }
    }

    /**
     * Releases the page and acquired write lock on it.
     *
     * @param pageId Page ID.
     * @param page Page absolute pointer.
     * @param pageAddr Page address.
     */
    private void releaseAndWriteUnlock(long pageId, long page, long pageAddr) {
        if (page != 0L) {
            try {
                writeUnlock(pageId, page, pageAddr, true);
            } finally {
                releasePage(pageId, page);
            }
        }
    }

    /**
     * Gets bucket index by page freespace.
     *
     * @return Bucket index or -1 if page doesn't belong to any bucket.
     */
    protected abstract int getBucketIndex(int freeSpace);

    /**
     * Returns bucket.
     *
     * @param bucket Bucket index.
     */
    protected abstract @Nullable Stripe[] getBucket(int bucket);

    /**
     * Sets a bucket through the "compare and set" operation.
     *
     * @param bucket Bucket index.
     * @param exp Expected bucket.
     * @param upd Updated bucket.
     * @return {@code true} If succeeded.
     */
    protected abstract boolean casBucket(int bucket, Stripe[] exp, Stripe[] upd);

    /**
     * Returns {@code true} if it is a reuse bucket.
     *
     * @param bucket Bucket index.
     */
    protected abstract boolean isReuseBucket(int bucket);

    /**
     * Return bucket cache.
     *
     * @param bucket Bucket index.
     * @param create Create if missing.
     */
    protected abstract @Nullable PagesCache getBucketCache(int bucket, boolean create);

    /**
     * Initializes a new {@link PagesListNodeIo}.
     *
     * @param io IO.
     * @param prevId Previous page ID.
     * @param prev Previous page buffer.
     * @param nextId Next page ID.
     * @param next Next page buffer.
     */
    private void setupNextPage(PagesListNodeIo io, long prevId, long prev, long nextId, long next) {
        assert io.getNextId(prev) == 0L;

        io.initNewPage(next, nextId, pageSize());
        io.setPreviousId(next, prevId);

        io.setNextId(prev, nextId);
    }

    /**
     * Adds stripe to the given bucket.
     *
     * @param bucket Bucket.
     * @param bag Reuse bag.
     * @param reuse {@code True} if possible to use reuse list.
     * @return Tail page ID.
     * @throws IgniteInternalCheckedException If failed.
     */
    private Stripe addStripe(int bucket, ReuseBag bag, boolean reuse) throws IgniteInternalCheckedException {
        long pageId = allocatePage(bag, reuse);

        init(pageId, PagesListNodeIo.VERSIONS.latest());

        Stripe stripe = new Stripe(pageId, true);

        for (; ; ) {
            Stripe[] old = getBucket(bucket);
            Stripe[] upd;

            if (old != null) {
                int len = old.length;

                upd = Arrays.copyOf(old, len + 1);

                upd[len] = stripe;
            } else {
                upd = new Stripe[]{stripe};
            }

            if (casBucket(bucket, old, upd)) {
                changed();

                return stripe;
            }
        }
    }

    /**
     * Stripe tail update.
     *
     * @param bucket Bucket index.
     * @param oldTailId Old tail page ID to replace.
     * @param newTailId New tail page ID.
     * @return {@code True} if stripe was removed.
     */
    private boolean updateTail(int bucket, long oldTailId, long newTailId) {
        int idx = -1;

        try {
            for (; ; ) {
                Stripe[] tails = getBucket(bucket);

                if (log.isDebugEnabled()) {
                    log.debug("Update tail [list={}, bucket={}, oldTailId={}, newTailId={}, tails={}]",
                            name(), bucket, oldTailId, newTailId, Arrays.toString(tails));
                }

                // Tail must exist to be updated.
                assert !nullOrEmpty(tails) : "Missing tails [bucket=" + bucket + ", tails=" + Arrays.toString(tails)
                        + ", metaPage=" + hexLong(metaPageId) + ", grpId=" + grpId + ']';

                idx = findTailIndex(tails, oldTailId, idx);

                assert tails[idx].tailId == oldTailId;

                if (newTailId == 0L) {
                    if (tails.length <= maxStripesPerBucket / 2) {
                        tails[idx].empty = true;

                        return false;
                    }

                    Stripe[] newTails;

                    if (tails.length != 1) {
                        newTails = remove(tails, idx);
                    } else {
                        newTails = null; // Drop the bucket completely.
                    }

                    if (casBucket(bucket, tails, newTails)) {
                        // Reset tailId for invalidation of locking when stripe was taken concurrently.
                        tails[idx].tailId = 0L;

                        return true;
                    }
                } else {
                    // It is safe to assign new tail since we do it only when write lock on tail is held.
                    tails[idx].tailId = newTailId;

                    return true;
                }
            }
        } finally {
            changed();
        }
    }

    /**
     * Returns first found index of the given tail ID.
     *
     * @param tails Tails.
     * @param tailId Tail ID to find.
     * @param expIdx Expected index.
     * @throws IllegalStateException If tail not found.
     */
    private static int findTailIndex(Stripe[] tails, long tailId, int expIdx) {
        if (expIdx != -1 && tails.length > expIdx && tails[expIdx].tailId == tailId) {
            return expIdx;
        }

        for (int i = 0; i < tails.length; i++) {
            if (tails[i].tailId == tailId) {
                return i;
            }
        }

        throw new IllegalStateException("Tail not found: " + tailId);
    }

    /**
     * Returns stripe for put page.
     *
     * @param bucket Bucket.
     * @param bag Reuse bag.
     * @throws IgniteInternalCheckedException If failed.
     */
    private Stripe getStripeForPut(int bucket, ReuseBag bag) throws IgniteInternalCheckedException {
        Stripe[] tails = getBucket(bucket);

        if (tails == null) {
            return addStripe(bucket, bag, true);
        }

        return randomTail(tails);
    }

    /**
     * Returns random tail.
     *
     * @param tails Tails.
     */
    private static Stripe randomTail(Stripe[] tails) {
        int len = tails.length;

        assert len != 0;

        return tails[randomInt(len)];
    }

    /**
     * !!! For tests only, does not provide any correctness guarantees for concurrent access.
     *
     * @param bucket Bucket index.
     * @return Number of pages stored in this list.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected final long storedPagesCount(int bucket) throws IgniteInternalCheckedException {
        long res = 0;

        Stripe[] tails = getBucket(bucket);

        if (tails != null) {
            for (Stripe tail : tails) {
                long tailId = tail.tailId;

                while (tailId != 0L) {
                    final long pageId = tailId;
                    final long page = acquirePage(pageId, IoStatisticsHolderNoOp.INSTANCE);
                    try {
                        long pageAddr = readLock(pageId, page);

                        assert pageAddr != 0L;

                        try {
                            PagesListNodeIo io = PagesListNodeIo.VERSIONS.forPage(pageAddr);

                            int cnt = io.getCount(pageAddr);

                            assert cnt >= 0;

                            res += cnt;
                            tailId = io.getPreviousId(pageAddr);

                            // In reuse bucket the page itself can be used as a free page.
                            if (isReuseBucket(bucket) && tailId != 0L) {
                                res++;
                            }
                        } finally {
                            readUnlock(pageId, page, pageAddr);
                        }
                    } finally {
                        releasePage(pageId, page);
                    }
                }
            }
        }

        assert res == bucketsSize.get(bucket) : "Wrong bucket size counter [exp=" + res + ", cntr=" + bucketsSize.get(bucket) + ']';

        return res;
    }

    /**
     * Puts data page.
     *
     * @param bag Reuse bag.
     * @param dataId Data page ID.
     * @param dataAddr Data page address.
     * @param bucket Bucket.
     * @param statHolder Statistics holder to track IO operations.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected final void put(
            @Nullable ReuseBag bag,
            final long dataId,
            final long dataAddr,
            int bucket,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        assert bag == null ^ dataAddr == 0L;

        if (bag != null && bag.isEmpty()) {
            // Skip allocating stripe for empty bag.
            return;
        }

        if (bag == null && onheapListCachingEnabled && putDataPage(getBucketCache(bucket, true), dataId, dataAddr, bucket)) {
            // Successfully put page to the onheap pages list cache.
            log.debug("Put page to pages list cache [list={}, bucket={}, dataId={}]", name(), bucket, dataId);

            return;
        }

        for (int lockAttempt = 0; ; ) {
            Stripe stripe = getStripeForPut(bucket, bag);

            // No need to continue if bag has been utilized at getPageForPut (free page can be used for pagelist).
            if (bag != null && bag.isEmpty()) {
                return;
            }

            final long tailId = stripe.tailId;

            // Stripe was removed from bucket concurrently.
            if (tailId == 0L) {
                continue;
            }

            final long tailPage = acquirePage(tailId, statHolder);

            try {
                long tailAddr = writeLockPage(tailId, tailPage, bucket, lockAttempt++, bag); // Explicit check.

                if (tailAddr == 0L) {
                    // No need to continue if bag has been utilized at writeLockPage.
                    if (bag != null && bag.isEmpty()) {
                        return;
                    } else {
                        continue;
                    }
                }

                if (stripe.tailId != tailId) {
                    // Another thread took the last page.
                    writeUnlock(tailId, tailPage, tailAddr, false);

                    lockAttempt--; // Ignore current attempt.

                    continue;
                }

                assert getPageId(tailAddr) == tailId
                        : "tailId = " + hexLong(tailId) + ", pageId = " + hexLong(getPageId(tailAddr));
                assert getType(tailAddr) == T_PAGE_LIST_NODE
                        : "tailId = " + hexLong(tailId) + ", type = " + getType(tailAddr);

                boolean ok = false;

                try {
                    PagesListNodeIo io = pageMem.ioRegistry().resolve(tailAddr);

                    ok = bag != null
                            // Here we can always take pages from the bag to build our list.
                            ? putReuseBag(tailId, tailAddr, io, bag, bucket, statHolder) :
                            // Here we can use the data page to build list only if it is empty and
                            // it is being put into reuse bucket. Usually this will be true, but there is
                            // a case when there is no reuse bucket in the free list, but then deadlock
                            // on node page allocation from separate reuse list is impossible.
                            // If the data page is not empty it can not be put into reuse bucket and thus
                            // the deadlock is impossible as well.
                            putDataPage(tailId, tailAddr, io, dataId, dataAddr, bucket, statHolder);

                    if (ok) {
                        log.debug("Put page to pages list [list={}, bucket={}, dataId={}, tailId={}]", name(), bucket, dataId, tailId);

                        stripe.empty = false;

                        return;
                    }
                } finally {
                    writeUnlock(tailId, tailPage, tailAddr, ok);
                }
            } finally {
                releasePage(tailId, tailPage);
            }
        }
    }

    /**
     * Puts data page.
     *
     * @param pageId Page ID.
     * @param pageAddr Page address.
     * @param io IO.
     * @param dataId Data page ID.
     * @param dataAddr Data page address.
     * @param bucket Bucket.
     * @param statHolder Statistics holder to track IO operations.
     * @return {@code true} If succeeded.
     * @throws IgniteInternalCheckedException If failed.
     */
    private boolean putDataPage(
            final long pageId,
            final long pageAddr,
            PagesListNodeIo io,
            final long dataId,
            final long dataAddr,
            int bucket,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        if (io.getNextId(pageAddr) != 0L) {
            return false; // Splitted.
        }

        int idx = io.addPage(pageAddr, dataId, pageSize());

        if (idx == -1) {
            handlePageFull(pageId, pageAddr, io, dataId, dataAddr, bucket, statHolder);
        } else {
            incrementBucketSize(bucket);

            DataPageIo dataIo = pageMem.ioRegistry().resolve(dataAddr);
            dataIo.setFreeListPageId(dataAddr, pageId);
        }

        return true;
    }

    /**
     * Puts data page to page-list cache.
     *
     * @param pagesCache Page-list cache onheap.
     * @param dataId Data page ID.
     * @param dataAddr Data page address.
     * @param bucket Bucket.
     * @return {@code true} If succeeded.
     * @throws IgniteInternalCheckedException If failed.
     */
    private boolean putDataPage(
            PagesCache pagesCache,
            final long dataId,
            final long dataAddr,
            int bucket
    ) throws IgniteInternalCheckedException {
        if (pagesCache.add(dataId)) {
            incrementBucketSize(bucket);

            DataPageIo dataIo = pageMem.ioRegistry().resolve(dataAddr);

            if (dataIo.getFreeListPageId(dataAddr) != 0L) {
                dataIo.setFreeListPageId(dataAddr, 0L);
            }

            pageCacheChanged();

            return true;
        } else {
            return false;
        }
    }

    /**
     * Handles the added page.
     *
     * @param pageId Page ID.
     * @param pageAddr Page address.
     * @param io IO.
     * @param dataId Data page ID.
     * @param dataAddr Data page address.
     * @param bucket Bucket index.
     * @param statHolder Statistics holder to track IO operations.
     * @throws IgniteInternalCheckedException If failed.
     */
    private void handlePageFull(
            final long pageId,
            final long pageAddr,
            PagesListNodeIo io,
            final long dataId,
            final long dataAddr,
            int bucket,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        DataPageIo dataIo = pageMem.ioRegistry().resolve(dataAddr);

        // Attempt to add page failed: the node page is full.
        if (isReuseBucket(bucket)) {
            // If we are on the reuse bucket, we can not allocate new page, because it may cause deadlock.
            assert dataIo.isEmpty(dataAddr); // We can put only empty data pages to reuse bucket.

            // Change page type to index and add it as next node page to this list.
            long newDataId = changeFlag(dataId, FLAG_AUX);

            setupNextPage(io, pageId, pageAddr, newDataId, dataAddr);

            // In reuse bucket the page itself can be used as a free page.
            incrementBucketSize(bucket);

            updateTail(bucket, pageId, newDataId);
        } else {
            // Just allocate a new node page and add our data page there.
            final long nextId = allocatePage(null);
            final long nextPage = acquirePage(nextId, statHolder);

            try {
                long nextPageAddr = writeLock(nextId, nextPage); // Newly allocated page.

                assert nextPageAddr != 0L;

                try {
                    setupNextPage(io, pageId, pageAddr, nextId, nextPageAddr);

                    int idx = io.addPage(nextPageAddr, dataId, pageSize());

                    assert idx != -1;

                    dataIo.setFreeListPageId(dataAddr, nextId);

                    incrementBucketSize(bucket);

                    updateTail(bucket, pageId, nextId);
                } finally {
                    writeUnlock(nextId, nextPage, nextPageAddr, true);
                }
            } finally {
                releasePage(nextId, nextPage);
            }
        }
    }

    /**
     * Puts page to reuse bag.
     *
     * @param pageId Page ID.
     * @param pageAddr Page address.
     * @param io IO.
     * @param bag Reuse bag.
     * @param bucket Bucket.
     * @param statHolder Statistics holder to track IO operations.
     * @return {@code true} If succeeded.
     * @throws IgniteInternalCheckedException if failed.
     */
    private boolean putReuseBag(
            final long pageId,
            final long pageAddr,
            PagesListNodeIo io,
            ReuseBag bag,
            int bucket,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        assert bag != null : "bag is null";
        assert !bag.isEmpty() : "bag is empty";

        if (io.getNextId(pageAddr) != 0L) {
            return false; // Splitted.
        }

        long nextId;

        long prevId = pageId;
        long prevAddr = pageAddr;

        // TODO: https://issues.apache.org/jira/browse/IGNITE-16350
        // TODO: may be unlock right away and do not keep all these pages locked?
        LongArrayList locked = null;

        try {
            while ((nextId = bag.pollFreePage()) != 0L) {
                assert itemId(nextId) > 0 && itemId(nextId) <= MAX_ITEM_ID_NUM : hexLong(nextId);

                int idx = io.addPage(prevAddr, nextId, pageSize());

                if (idx == -1) { // Attempt to add page failed: the node page is full.
                    final long nextPage = acquirePage(nextId, statHolder);

                    try {
                        long nextPageAddr = writeLock(nextId, nextPage); // Page from reuse bag can't be concurrently recycled.

                        assert nextPageAddr != 0L;

                        if (locked == null) {
                            locked = new LongArrayList(6);
                        }

                        locked.add(nextId);
                        locked.add(nextPage);
                        locked.add(nextPageAddr);

                        setupNextPage(io, prevId, prevAddr, nextId, nextPageAddr);

                        // In reuse bucket the page itself can be used as a free page.
                        if (isReuseBucket(bucket)) {
                            incrementBucketSize(bucket);
                        }

                        // Switch to this new page, which is now a part of our list
                        // to add the rest of the bag to the new page.
                        prevAddr = nextPageAddr;
                        prevId = nextId;
                    } finally {
                        releasePage(nextId, nextPage);
                    }
                } else {
                    incrementBucketSize(bucket);
                }
            }
        } finally {
            if (locked != null) {
                // We have to update our bucket with the new tail.
                updateTail(bucket, pageId, prevId);

                // Release write.
                for (int i = 0; i < locked.size(); i += 3) {
                    writeUnlock(locked.getLong(i), locked.getLong(i + 1), locked.getLong(i + 2), true);
                }
            }
        }

        return true;
    }

    /**
     * Returns stripe for take.
     *
     * @param bucket Bucket index.
     */
    private @Nullable Stripe getStripeForTake(int bucket) {
        Stripe[] tails = getBucket(bucket);

        if (tails == null || bucketsSize.get(bucket) == 0) {
            return null;
        }

        int len = tails.length;

        int init = randomInt(len);
        int cur = init;

        while (true) {
            Stripe stripe = tails[cur];

            if (!stripe.empty) {
                return stripe;
            }

            if ((cur = (cur + 1) % len) == init) {
                return null;
            }
        }
    }

    /**
     * Returns page address if page is locked of {@code 0} if it can retry lock.
     *
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param bucket Bucket.
     * @param lockAttempt Lock attempts counter.
     * @param bag Reuse bag.
     * @throws IgniteInternalCheckedException If failed.
     */
    private long writeLockPage(long pageId, long page, int bucket, int lockAttempt, ReuseBag bag) throws IgniteInternalCheckedException {
        long pageAddr = tryWriteLock(pageId, page);

        if (pageAddr != 0L) {
            return pageAddr;
        }

        if (lockAttempt == tryLockAttempts) {
            Stripe[] stripes = getBucket(bucket);

            if (stripes == null || stripes.length < maxStripesPerBucket) {
                addStripe(bucket, bag, !isReuseBucket(bucket));

                return 0L;
            }
        }

        return lockAttempt < tryLockAttempts ? 0L : writeLock(pageId, page); // Must be explicitly checked further.
    }

    /**
     * Takes empty page from free list.
     *
     * @param bucket Bucket index.
     * @param initIoVers Optional IO to initialize page.
     * @param statHolder Statistics holder to track IO operations.
     * @return Removed page ID.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected long takeEmptyPage(
            int bucket,
            @Nullable IoVersions<?> initIoVers,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        PagesCache pagesCache = getBucketCache(bucket, false);

        long pageId;

        if (pagesCache != null && (pageId = pagesCache.poll()) != 0L) {
            decrementBucketSize(bucket);

            log.debug("Take page from pages list cache [list={}, bucket={}, pageId={}]", name(), bucket, pageId);

            assert !isReuseBucket(bucket) : "reuse bucket detected";

            return pageId;
        }

        for (int lockAttempt = 0; ; ) {
            Stripe stripe = getStripeForTake(bucket);

            if (stripe == null) {
                return 0L;
            }

            final long tailId = stripe.tailId;

            // Stripe was removed from bucket concurrently.
            if (tailId == 0L) {
                continue;
            }

            final long tailPage = acquirePage(tailId, statHolder);

            try {
                long tailAddr = writeLockPage(tailId, tailPage, bucket, lockAttempt++, null); // Explicit check.

                if (tailAddr == 0L) {
                    continue;
                }

                if (stripe.empty || stripe.tailId != tailId) {
                    // Another thread took the last page.
                    writeUnlock(tailId, tailPage, tailAddr, false);

                    if (bucketsSize.get(bucket) > 0) {
                        lockAttempt--; // Ignore current attempt.

                        continue;
                    } else {
                        return 0L;
                    }
                }

                assert getPageId(tailAddr) == tailId
                        : "tailId = " + hexLong(tailId) + ", pageId = " + hexLong(getPageId(tailAddr));
                assert getType(tailAddr) == T_PAGE_LIST_NODE
                        : "tailId = " + hexLong(tailId) + ", type = " + getType(tailAddr);

                boolean dirty = false;
                long dataPageId;
                long recycleId = 0L;

                try {
                    PagesListNodeIo io = PagesListNodeIo.VERSIONS.forPage(tailAddr);

                    if (io.getNextId(tailAddr) != 0) {
                        // It is not a tail anymore, retry.
                        continue;
                    }

                    pageId = io.takeAnyPage(tailAddr);

                    if (pageId != 0L) {
                        decrementBucketSize(bucket);

                        dirty = true;

                        if (isReuseBucket(bucket) && !(itemId(pageId) > 0 && itemId(pageId) <= MAX_ITEM_ID_NUM)) {
                            throw corruptedFreeListException("Incorrectly recycled pageId in reuse bucket: " + hexLong(pageId), pageId);
                        }

                        if (isReuseBucket(bucket)) {
                            byte flag = getFlag(initIoVers);

                            PageIo initIo = initIoVers == null ? null : initIoVers.latest();

                            dataPageId = initRecycledPage0(pageId, flag, initIo);
                        } else {
                            dataPageId = pageId;
                        }

                        if (io.isEmpty(tailAddr)) {
                            long prevId = io.getPreviousId(tailAddr);

                            // If we got an empty page in non-reuse bucket, move it back to reuse list
                            // to prevent empty page leak to data pages.
                            if (!isReuseBucket(bucket)) {
                                if (prevId != 0L) {
                                    Boolean ok = write(prevId, cutTail, null, bucket, FALSE, statHolder);

                                    assert ok == TRUE : ok;

                                    recycleId = recyclePage(tailId, tailAddr);
                                } else {
                                    stripe.empty = true;
                                }
                            } else {
                                stripe.empty = prevId == 0L;
                            }
                        }
                    } else {
                        // The tail page is empty, but stripe is not. It might
                        // happen only if we are in reuse bucket and it has
                        // a previous page, so, the current page can be collected
                        assert isReuseBucket(bucket);

                        long prevId = io.getPreviousId(tailAddr);

                        assert prevId != 0L;

                        Boolean ok = write(prevId, cutTail, bucket, FALSE, statHolder);

                        assert ok == TRUE : ok;

                        decrementBucketSize(bucket);

                        byte flag = getFlag(initIoVers);

                        PageIo pageIo = initIoVers != null ? initIoVers.latest() : null;

                        dataPageId = initReusedPage(tailId, tailAddr, partitionId(tailId), flag, pageIo);

                        dirty = true;
                    }

                    // If we do not have a previous page (we are at head), then we still can return
                    // current page but we have to drop the whole stripe. Since it is a reuse bucket,
                    // we will not do that, but just return 0L, because this may produce contention on
                    // meta page.
                } finally {
                    writeUnlock(tailId, tailPage, tailAddr, dirty);
                }

                // Put recycled page (if any) to the reuse bucket after tail is unlocked.
                if (recycleId != 0L) {
                    assert !isReuseBucket(bucket);

                    reuseList.addForRecycle(new SingletonReuseBag(recycleId));
                }

                log.debug("Take page from pages list [list={}, bucket={}, dataPageId={}, tailId={}]", name(), bucket, dataPageId, tailId);

                return dataPageId;
            } finally {
                releasePage(tailId, tailPage);
            }
        }
    }

    /**
     * Returns {@link PageIdAllocator#FLAG_DATA} for group metas and data pages, {@link #defaultPageFlag} otherwise.
     *
     * @param initIoVers Optional IO versions list that will be used later to init the page.
     */
    private byte getFlag(@Nullable IoVersions<?> initIoVers) {
        if (initIoVers != null) {
            PageIo pageIo = initIoVers.latest();

            return pageIo.getFlag();
        }

        return defaultPageFlag;
    }

    /**
     * Create new page id and update page content accordingly if it's necessary.
     *
     * @param pageId Id of the recycled page from reuse bucket.
     * @param flag New flag for the page.
     * @return New page id.
     * @throws IgniteInternalCheckedException If failed.
     * @see PagesList#initReusedPage(long, long, int, byte, PageIo)
     */
    protected long initRecycledPage0(long pageId, byte flag, PageIo initIo) throws IgniteInternalCheckedException {
        long page = pageMem.acquirePage(grpId, pageId);

        try {
            long pageAddr = pageMem.writeLock(grpId, pageId, page);

            assert pageAddr != 0;

            try {
                return initReusedPage(pageId, pageAddr, partitionId(pageId), flag, initIo);
            } finally {
                pageMem.writeUnlock(grpId, pageId, page, true);
            }
        } finally {
            pageMem.releasePage(grpId, pageId, page);
        }
    }

    /**
     * Reused page must obtain correctly assaembled page id, then initialized by proper {@link PageIo} instance and non-zero {@code itemId}
     * of reused page id must be saved into special place.
     *
     * @param reusedPageId Reused page id.
     * @param reusedPageAddr Reused page address.
     * @param partId Partition id.
     * @param flag Flag.
     * @param initIo Initial io.
     * @return Prepared page id.
     */
    protected final long initReusedPage(
            long reusedPageId,
            long reusedPageAddr,
            int partId,
            byte flag,
            PageIo initIo
    ) {
        long newPageId = pageId(partId, flag, PageIdUtils.pageIndex(reusedPageId));

        if (initIo != null) {
            initIo.initNewPage(reusedPageAddr, newPageId, pageSize());
        }

        int itemId = itemId(reusedPageId);

        if (itemId != 0) {
            if (flag == FLAG_DATA) {
                PageIo.setRotatedIdPart(reusedPageAddr, itemId);
            } else {
                newPageId = PageIdUtils.link(newPageId, itemId);
            }
        }

        long storedPageId = getPageId(reusedPageAddr);

        if (storedPageId != newPageId) {
            PageIo.setPageId(reusedPageAddr, newPageId);
        }

        return newPageId;
    }

    /**
     * Removes data page from bucket, merges bucket list if needed.
     *
     * @param dataId Data page ID.
     * @param dataAddr Data page address.
     * @param dataIo Data page IO.
     * @param bucket Bucket index.
     * @param statHolder Statistics holder to track IO operations.
     * @return {@code True} if page was removed.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected final boolean removeDataPage(
            final long dataId,
            final long dataAddr,
            DataPageIo dataIo,
            int bucket,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        final long pageId = dataIo.getFreeListPageId(dataAddr);

        if (pageId == 0L) { // Page cached in onheap list.
            assert isCachingApplicable() : "pageId==0L, but caching is not applicable for this pages list: " + name();

            PagesCache pagesCache = getBucketCache(bucket, false);

            // Pages cache can be null here if page was taken for put from free list concurrently.
            if (pagesCache == null || !pagesCache.removePage(dataId)) {
                log.debug("Remove page from pages list cache failed [list={}, bucket={}, dataId={}, reason={}]",
                        name(), bucket, dataId, ((pagesCache == null) ? "cache is null" : "page not found"));

                return false;
            }

            decrementBucketSize(bucket);

            log.debug("Remove page from pages list cache [list={}, bucket={}, dataId={}]", name(), bucket, dataId);

            return true;
        }

        log.debug("Remove page from pages list [list={}, bucket={}, dataId={}, pageId={}]", name(), bucket, dataId, pageId);

        final long page = acquirePage(pageId, statHolder);

        try {
            long nextId;

            long recycleId = 0L;

            long pageAddr = writeLock(pageId, page); // Explicit check.

            if (pageAddr == 0L) {
                return false;
            }

            boolean rmvd = false;

            try {
                PagesListNodeIo io = PagesListNodeIo.VERSIONS.forPage(pageAddr);

                rmvd = io.removePage(pageAddr, dataId);

                if (!rmvd) {
                    return false;
                }

                decrementBucketSize(bucket);

                // Reset free list page ID.
                dataIo.setFreeListPageId(dataAddr, 0L);

                if (!io.isEmpty(pageAddr)) {
                    return true; // In optimistic case we still have something in the page and can leave it as is.
                }

                // If the page is empty, we have to try to drop it and link next and previous with each other.
                nextId = io.getNextId(pageAddr);

                // If there are no next page, then we can try to merge without releasing current write lock,
                // because if we will need to lock previous page, the locking order will be already correct.
                if (nextId == 0L) {
                    long prevId = io.getPreviousId(pageAddr);

                    recycleId = mergeNoNext(pageId, pageAddr, prevId, bucket, statHolder);
                }
            } finally {
                writeUnlock(pageId, page, pageAddr, rmvd);
            }

            // Perform a fair merge after lock release (to have a correct locking order).
            if (nextId != 0L) {
                recycleId = merge(pageId, page, nextId, bucket, statHolder);
            }

            if (recycleId != 0L) {
                reuseList.addForRecycle(new SingletonReuseBag(recycleId));
            }

            return true;
        } finally {
            releasePage(pageId, page);
        }
    }

    private long mergeNoNext(
            long pageId,
            long pageAddr,
            long prevId,
            int bucket,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        // If we do not have a next page (we are tail) and we are on reuse bucket,
        // then we can leave as is as well, because it is normal to have an empty tail page here.
        if (isReuseBucket(bucket)) {
            return 0L;
        }

        if (prevId != 0L) { // Cut tail if we have a previous page.
            Boolean ok = write(prevId, cutTail, null, bucket, FALSE, statHolder);

            assert ok == TRUE : ok;
        } else {
            // If we don't have a previous, then we are tail page of free list, just drop the stripe.
            boolean rmvd = updateTail(bucket, pageId, 0L);

            if (!rmvd) {
                return 0L;
            }
        }

        return recyclePage(pageId, pageAddr);
    }

    private long merge(
            final long pageId,
            final long page,
            long nextId,
            int bucket,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        assert nextId != 0; // We should do mergeNoNext then.

        // Lock all the pages in correct order (from next to previous) and do the merge in retry loop.
        for (; ; ) {
            final long curId = nextId;
            final long curPage = curId == 0L ? 0L : acquirePage(curId, statHolder);
            try {
                boolean write = false;

                final long curAddr = curPage == 0L ? 0L : writeLock(curId, curPage); // Explicit check.
                final long pageAddr = writeLock(pageId, page); // Explicit check.

                if (pageAddr == 0L) {
                    if (curAddr != 0L) {
                        // Unlock next page if needed.
                        writeUnlock(curId, curPage, curAddr, false);
                    }

                    return 0L; // Someone has merged or taken our empty page concurrently. Nothing to do here.
                }

                try {
                    PagesListNodeIo io = PagesListNodeIo.VERSIONS.forPage(pageAddr);

                    if (!io.isEmpty(pageAddr)) {
                        return 0L; // No need to merge anymore.
                    }

                    // Check if we see a consistent state of the world.
                    if (io.getNextId(pageAddr) == curId && (curId == 0L) == (curAddr == 0L)) {
                        long recycleId = doMerge(pageId, pageAddr, io, curId, curAddr, bucket, statHolder);

                        write = true;

                        return recycleId; // Done.
                    }

                    // Reread next page ID and go for retry.
                    nextId = io.getNextId(pageAddr);
                } finally {
                    if (curAddr != 0L) {
                        writeUnlock(curId, curPage, curAddr, write);
                    }

                    writeUnlock(pageId, page, pageAddr, write);
                }
            } finally {
                if (curPage != 0L) {
                    releasePage(curId, curPage);
                }
            }
        }
    }

    private long doMerge(
            long pageId,
            long pageAddr,
            PagesListNodeIo io,
            long nextId,
            long nextAddr,
            int bucket,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        long prevId = io.getPreviousId(pageAddr);

        if (nextId == 0L) {
            return mergeNoNext(pageId, pageAddr, prevId, bucket, statHolder);
        } else {
            // No one must be able to merge it while we keep a reference.
            assert getPageId(nextAddr) == nextId;

            if (prevId == 0L) { // No previous page: we are at head.
                // These references must be updated at the same time in write locks.
                assert PagesListNodeIo.VERSIONS.forPage(nextAddr).getPreviousId(nextAddr) == pageId;

                PagesListNodeIo nextIo = PagesListNodeIo.VERSIONS.forPage(nextAddr);
                nextIo.setPreviousId(nextAddr, 0);
            } else {
                // Do a fair merge: link previous and next to each other.
                fairMerge(prevId, pageId, nextId, nextAddr, statHolder);
            }

            return recyclePage(pageId, pageAddr);
        }
    }

    /**
     * Link previous and next to each other.
     *
     * @param prevId Previous Previous page ID.
     * @param pageId Page ID.
     * @param nextId Next page ID.
     * @param nextAddr Next page address.
     * @param statHolder Statistics holder to track IO operations.
     * @throws IgniteInternalCheckedException If failed.
     */
    private void fairMerge(
            final long prevId,
            long pageId,
            long nextId,
            long nextAddr,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        long prevPage = acquirePage(prevId, statHolder);

        try {
            final long prevAddr = writeLock(prevId, prevPage); // No check, we keep a reference.
            assert prevAddr != 0L;
            try {
                PagesListNodeIo prevIo = PagesListNodeIo.VERSIONS.forPage(prevAddr);
                PagesListNodeIo nextIo = PagesListNodeIo.VERSIONS.forPage(nextAddr);

                // These references must be updated at the same time in write locks.
                assert prevIo.getNextId(prevAddr) == pageId;
                assert nextIo.getPreviousId(nextAddr) == pageId;

                prevIo.setNextId(prevAddr, nextId);

                nextIo.setPreviousId(nextAddr, prevId);
            } finally {
                writeUnlock(prevId, prevPage, prevAddr, true);
            }
        } finally {
            releasePage(prevId, prevPage);
        }
    }

    /**
     * Increments bucket size and updates changed flag.
     *
     * @param bucket Bucket number.
     */
    private void incrementBucketSize(int bucket) {
        bucketsSize.incrementAndGet(bucket);
    }

    /**
     * Increments bucket size and updates changed flag.
     *
     * @param bucket Bucket number.
     */
    private void decrementBucketSize(int bucket) {
        bucketsSize.decrementAndGet(bucket);
    }

    /**
     * Mark free list was changed.
     */
    private void changed() {
        // Ok to have a race here, see the field javadoc.
        if (!changed) {
            changed = true;
        }
    }

    /**
     * Mark free list page cache was changed.
     */
    private void pageCacheChanged() {
        // Ok to have a race here, see the field javadoc.
        if (!pageCacheChanged) {
            pageCacheChanged = true;
        }
    }

    /**
     * Returns buckets count.
     */
    public int bucketsCount() {
        return buckets;
    }

    /**
     * Bucket size.
     *
     * @param bucket Bucket.
     */
    public long bucketSize(int bucket) {
        return bucketsSize.get(bucket);
    }

    /**
     * Returns stripes count.
     *
     * @param bucket Bucket.
     */
    public int stripesCount(int bucket) {
        Stripe[] stripes = getBucket(bucket);

        return stripes == null ? 0 : stripes.length;
    }

    /**
     * Returns cached pages count.
     *
     * @param bucket Bucket.
     */
    public int cachedPagesCount(int bucket) {
        PagesCache pagesCache = getBucketCache(bucket, false);

        return pagesCache == null ? 0 : pagesCache.size();
    }

    /**
     * Returns meta page id.
     */
    public long metaPageId() {
        return metaPageId;
    }

    /**
     * Returns {@link CorruptedFreeListException} that wraps original error and ids of possibly corrupted pages.
     *
     * @param err Error that caused this exception.
     * @param pageIds Ids of possibly corrupted pages.
     */
    protected CorruptedFreeListException corruptedFreeListException(Throwable err, long... pageIds) {
        return corruptedFreeListException(err.getMessage(), err, pageIds);
    }

    /**
     * Returns {@link CorruptedFreeListException} that wraps original error and ids of possibly corrupted pages.
     *
     * @param msg Exception message.
     * @param pageIds Ids of possibly corrupted pages.
     */
    protected CorruptedFreeListException corruptedFreeListException(String msg, long... pageIds) {
        return corruptedFreeListException(msg, null, pageIds);
    }

    /**
     * Returns {@link CorruptedFreeListException} that wraps original error and ids of possibly corrupted pages.
     *
     * @param msg Exception message.
     * @param err Error that caused this exception.
     * @param pageIds Ids of possibly corrupted pages.
     */
    protected CorruptedFreeListException corruptedFreeListException(String msg, @Nullable Throwable err, long... pageIds) {
        return new CorruptedFreeListException(msg, err, grpId, pageIds);
    }

    /**
     * Singleton reuse bag.
     */
    private static final class SingletonReuseBag implements ReuseBag {
        /** Page ID. */
        long pageId;

        /**
         * Constructor.
         *
         * @param pageId Page ID.
         */
        SingletonReuseBag(long pageId) {
            this.pageId = pageId;
        }

        /** {@inheritDoc} */
        @Override
        public void addFreePage(long pageId) {
            throw new IllegalStateException("Should never be called.");
        }

        /** {@inheritDoc} */
        @Override
        public long pollFreePage() {
            long res = pageId;

            pageId = 0L;

            return res;
        }

        /** {@inheritDoc} */
        @Override
        public boolean isEmpty() {
            return pageId == 0L;
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return S.toString(SingletonReuseBag.class, this, "pageId", hexLong(pageId));
        }
    }

    /**
     * Class to store page-list cache onheap.
     */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-16350
    public static class PagesCache {
        /** Pages cache maximum size. */
        public static final String IGNITE_PAGES_LIST_CACHING_MAX_CACHE_SIZE = "IGNITE_PAGES_LIST_CACHING_MAX_CACHE_SIZE";

        /** Stripes count. Must be power of 2. */
        public static final String IGNITE_PAGES_LIST_CACHING_STRIPES_COUNT = "IGNITE_PAGES_LIST_CACHING_STRIPES_COUNT";

        /** The threshold of flush calls on empty caches to allow GC of stripes (the flush is triggered two times per checkpoint). */
        public static final String IGNITE_PAGES_LIST_CACHING_EMPTY_FLUSH_GC_THRESHOLD =
                "IGNITE_PAGES_LIST_CACHING_EMPTY_FLUSH_GC_THRESHOLD";

        private static final int MAX_SIZE = getInteger(IGNITE_PAGES_LIST_CACHING_MAX_CACHE_SIZE, 64);

        private static final int STRIPES_COUNT = getInteger(IGNITE_PAGES_LIST_CACHING_STRIPES_COUNT, 4);

        private static final int EMPTY_FLUSH_GC_THRESHOLD = getInteger(IGNITE_PAGES_LIST_CACHING_EMPTY_FLUSH_GC_THRESHOLD, 10);

        /** Mutexes for each stripe. */
        private final Object[] stripeLocks = new Object[STRIPES_COUNT];

        /** Page lists. */
        private final LongArrayList[] stripes = new LongArrayList[STRIPES_COUNT];

        /** Atomic updater for {@link #nextStripeIdx} field. */
        private static final AtomicIntegerFieldUpdater<PagesCache> nextStripeUpdater = newUpdater(PagesCache.class, "nextStripeIdx");

        /** Atomic updater for size field. */
        private static final AtomicIntegerFieldUpdater<PagesCache> sizeUpdater = newUpdater(PagesCache.class, "size");

        /** Access counter to provide round-robin stripes polling. */
        @SuppressWarnings("PMD.UnusedPrivateField")
        private volatile int nextStripeIdx;

        /** Cache size. */
        private volatile int size;

        /** Count of flush calls with empty cache. */
        private int emptyFlushCnt;

        /** Global (per data region) limit of caches for page lists. */
        private final @Nullable AtomicLong pagesCacheLimit;

        /**
         * Constructor.
         *
         * @param pagesCacheLimit Global (per data region) limit of caches for page lists.
         */
        public PagesCache(@Nullable AtomicLong pagesCacheLimit) {
            assert isPow2(STRIPES_COUNT) : STRIPES_COUNT;

            for (int i = 0; i < STRIPES_COUNT; i++) {
                stripeLocks[i] = new Object();
            }

            this.pagesCacheLimit = pagesCacheLimit;
        }

        /**
         * Remove page from the list.
         *
         * @param pageId Page id.
         * @return {@code True} if page was found and successfully removed, {@code false} if page not found.
         */
        public boolean removePage(long pageId) {
            int stripeIdx = (int) pageId & (STRIPES_COUNT - 1);

            synchronized (stripeLocks[stripeIdx]) {
                LongArrayList stripe = stripes[stripeIdx];

                boolean rmvd = stripe != null && stripe.rem(pageId);

                if (rmvd) {
                    if (sizeUpdater.decrementAndGet(this) == 0 && pagesCacheLimit != null) {
                        pagesCacheLimit.incrementAndGet();
                    }
                }

                return rmvd;
            }
        }

        /**
         * Poll next page from the list.
         *
         * @return Page ID.
         */
        public long poll() {
            if (size == 0) {
                return 0L;
            }

            for (int i = 0; i < STRIPES_COUNT; i++) {
                int stripeIdx = nextStripeUpdater.getAndIncrement(this) & (STRIPES_COUNT - 1);

                synchronized (stripeLocks[stripeIdx]) {
                    LongArrayList stripe = stripes[stripeIdx];

                    if (stripe != null && !stripe.isEmpty()) {
                        if (sizeUpdater.decrementAndGet(this) == 0 && pagesCacheLimit != null) {
                            pagesCacheLimit.incrementAndGet();
                        }

                        return stripe.removeLong(stripe.size() - 1);
                    }
                }
            }

            return 0L;
        }

        /**
         * Flush all stripes to one list and clear stripes.
         */
        public LongArrayList flush() {
            LongArrayList res = null;

            if (size == 0) {
                boolean stripesChanged = false;

                if (emptyFlushCnt >= 0 && ++emptyFlushCnt >= EMPTY_FLUSH_GC_THRESHOLD) {
                    for (int i = 0; i < STRIPES_COUNT; i++) {
                        synchronized (stripeLocks[i]) {
                            LongArrayList stripe = stripes[i];

                            if (stripe != null) {
                                if (stripe.isEmpty()) {
                                    stripes[i] = null;
                                } else {
                                    // Pages were concurrently added to the stripe.
                                    stripesChanged = true;

                                    break;
                                }
                            }
                        }
                    }

                    if (!stripesChanged) {
                        emptyFlushCnt = -1;
                    }
                }

                if (!stripesChanged) {
                    return null;
                }
            }

            emptyFlushCnt = 0;

            for (int i = 0; i < STRIPES_COUNT; i++) {
                synchronized (stripeLocks[i]) {
                    LongArrayList stripe = stripes[i];

                    if (stripe != null && !stripe.isEmpty()) {
                        if (res == null) {
                            res = new LongArrayList(size);
                        }

                        if (sizeUpdater.addAndGet(this, -stripe.size()) == 0 && pagesCacheLimit != null) {
                            pagesCacheLimit.incrementAndGet();
                        }

                        res.addAll(stripe);

                        stripe.clear();
                    }
                }
            }

            return res;
        }

        /**
         * Add pageId to the tail of the list.
         *
         * @param pageId Page id.
         * @return {@code True} if page can be added, {@code false} if list is full.
         */
        public boolean add(long pageId) {
            assert pageId != 0L;

            // Ok with race here.
            if (size == 0 && pagesCacheLimit != null && pagesCacheLimit.get() <= 0) {
                return false; // Pages cache limit exceeded.
            }

            // Ok with race here.
            if (size >= MAX_SIZE) {
                return false;
            }

            int stripeIdx = (int) pageId & (STRIPES_COUNT - 1);

            synchronized (stripeLocks[stripeIdx]) {
                LongArrayList stripe = stripes[stripeIdx];

                if (stripe == null) {
                    stripes[stripeIdx] = stripe = new LongArrayList(MAX_SIZE / STRIPES_COUNT);
                }

                if (stripe.size() >= MAX_SIZE / STRIPES_COUNT) {
                    return false;
                } else {
                    stripe.add(pageId);

                    if (sizeUpdater.getAndIncrement(this) == 0 && pagesCacheLimit != null) {
                        pagesCacheLimit.decrementAndGet();
                    }

                    return true;
                }
            }
        }

        /**
         * Returns cache size.
         */
        public int size() {
            return size;
        }
    }

    /**
     * Stripe.
     */
    public static final class Stripe {
        /** Tail ID. */
        public volatile long tailId;

        /** Empty flag. */
        public volatile boolean empty;

        /**
         * Constructor.
         *
         * @param tailId Tail ID.
         * @param empty Empty flag.
         */
        Stripe(long tailId, boolean empty) {
            this.tailId = tailId;
            this.empty = empty;
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Stripe stripe = (Stripe) o;

            return tailId == stripe.tailId && empty == stripe.empty;
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return (31 + Long.hashCode(tailId)) * 31 + Boolean.hashCode(empty);
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return Long.toString(tailId);
        }
    }
}
