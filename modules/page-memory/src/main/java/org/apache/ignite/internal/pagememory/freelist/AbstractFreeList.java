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

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.util.IgniteUtils.isPow2;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.pagememory.PageIdAllocator;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.Storable;
import org.apache.ignite.internal.pagememory.evict.PageEvictionTracker;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.reuse.LongListReuseBag;
import org.apache.ignite.internal.pagememory.reuse.ReuseBag;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.util.PageHandler;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.pagememory.util.PageLockListener;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract free list.
 */
public abstract class AbstractFreeList<T extends Storable> extends PagesList implements FreeList<T>, ReuseList {
    private static final int BUCKETS = 256; // Must be power of 2.

    private static final int REUSE_BUCKET = BUCKETS - 1;

    private static final Integer COMPLETE = Integer.MAX_VALUE;

    private static final Integer FAIL_I = Integer.MIN_VALUE;

    private static final Long FAIL_L = Long.MAX_VALUE;

    private static final int MIN_PAGE_FREE_SPACE = 8;

    /**
     * Step between buckets in free list, measured in powers of two. For example, for page size 4096 and 256 buckets, shift is 4 and step is
     * 16 bytes.
     */
    private final int shift;

    /** Buckets. */
    private final AtomicReferenceArray<Stripe[]> buckets = new AtomicReferenceArray<>(BUCKETS);

    /** Onheap bucket page list caches. */
    private final AtomicReferenceArray<PagesCache> bucketCaches = new AtomicReferenceArray<>(BUCKETS);

    /** Min size for data page in bytes. */
    private final int minSizeForDataPage;

    /** Page list cache limit. */
    private final @Nullable AtomicLong pageListCacheLimit;

    /** Page eviction tracker. */
    protected final PageEvictionTracker evictionTracker;

    /** Write a single row on a single page. */
    private final WriteRowHandler writeRowHnd = new WriteRowHandler();

    /** Write multiple rows on a single page. */
    private final WriteRowsHandler writeRowsHnd = new WriteRowsHandler();

    private final PageHandler<ReuseBag, Long> rmvRow;

    private class WriteRowHandler implements PageHandler<T, Integer> {
        /** {@inheritDoc} */
        @Override
        public Integer run(
                int cacheId,
                long pageId,
                long page,
                long pageAddr,
                PageIo iox,
                T row,
                int written,
                IoStatisticsHolder statHolder
        ) throws IgniteInternalCheckedException {
            written = addRow(pageId, pageAddr, iox, row, written);

            putPage(((AbstractDataPageIo<Storable>) iox).getFreeSpace(pageAddr), pageId, pageAddr, statHolder);

            return written;
        }

        /**
         * Writes row to data page.
         *
         * @param pageId Page ID.
         * @param pageAddr Page address.
         * @param iox IO.
         * @param row Row to write.
         * @param written Written size.
         * @return Number of bytes written, {@link #COMPLETE} if the row was fully written.
         * @throws IgniteInternalCheckedException If failed.
         */
        protected Integer addRow(
                long pageId,
                long pageAddr,
                PageIo iox,
                T row,
                int written
        ) throws IgniteInternalCheckedException {
            AbstractDataPageIo<T> io = (AbstractDataPageIo<T>) iox;

            int rowSize = row.size();
            int oldFreeSpace = io.getFreeSpace(pageAddr);

            assert oldFreeSpace > 0 : oldFreeSpace;

            // If the full row does not fit into this page write only a fragment.
            written = (written == 0 && oldFreeSpace >= rowSize) ? addRowFull(pageId, pageAddr, io, row, rowSize) :
                    addRowFragment(pageId, pageAddr, io, row, written, rowSize);

            if (written == rowSize) {
                evictionTracker.touchPage(pageId);
            }

            // Avoid boxing with garbage generation for usual case.
            return written == rowSize ? COMPLETE : written;
        }

        /**
         * Adds row to this data page and sets respective link to the given row object.
         *
         * @param pageId Page ID.
         * @param pageAddr Page address.
         * @param io IO.
         * @param row Row.
         * @param rowSize Row size.
         * @return Written size which is always equal to row size here.
         * @throws IgniteInternalCheckedException If failed.
         */
        protected int addRowFull(
                long pageId,
                long pageAddr,
                AbstractDataPageIo<T> io,
                T row,
                int rowSize
        ) throws IgniteInternalCheckedException {
            io.addRow(pageId, pageAddr, row, rowSize, pageSize());

            return rowSize;
        }

        /**
         * Adds maximum possible fragment of the given row to this data page and sets respective link to the row.
         *
         * @param pageId Page ID.
         * @param pageAddr Page address.
         * @param io IO.
         * @param row Row.
         * @param written Written size.
         * @param rowSize Row size.
         * @return Updated written size.
         * @throws IgniteInternalCheckedException If failed.
         */
        protected int addRowFragment(
                long pageId,
                long pageAddr,
                AbstractDataPageIo<T> io,
                T row,
                int written,
                int rowSize
        ) throws IgniteInternalCheckedException {
            int payloadSize = io.addRowFragment(pageMem, pageId, pageAddr, row, written, rowSize, pageSize());

            assert payloadSize > 0 : payloadSize;

            return written + payloadSize;
        }

        /**
         * Put page into the free list if needed.
         *
         * @param freeSpace Page free space.
         * @param pageId Page ID.
         * @param pageAddr Page address.
         * @param statHolder Statistics holder to track IO operations.
         */
        protected void putPage(
                int freeSpace,
                long pageId,
                long pageAddr,
                IoStatisticsHolder statHolder
        ) throws IgniteInternalCheckedException {
            if (freeSpace > MIN_PAGE_FREE_SPACE) {
                int bucket = bucket(freeSpace, false);

                put(null, pageId, pageAddr, bucket, statHolder);
            }
        }
    }

    private final class WriteRowsHandler implements PageHandler<CachedIterator<T>, Integer> {
        /** {@inheritDoc} */
        @Override
        public Integer run(
                int cacheId,
                long pageId,
                long page,
                long pageAddr,
                PageIo iox,
                CachedIterator<T> it,
                int written,
                IoStatisticsHolder statHolder
        ) throws IgniteInternalCheckedException {
            AbstractDataPageIo<T> io = (AbstractDataPageIo<T>) iox;

            // Fill the page up to the end.
            while (written != COMPLETE || (!evictionTracker.evictionRequired() && it.hasNext())) {
                T row = it.get();

                if (written == COMPLETE) {
                    row = it.next();

                    // If the data row was completely written without remainder, proceed to the next.
                    if ((written = writeWholePages(row, statHolder)) == COMPLETE) {
                        continue;
                    }

                    if (io.getFreeSpace(pageAddr) < row.size() - written) {
                        break;
                    }
                }

                written = writeRowHnd.addRow(pageId, pageAddr, io, row, written);

                assert written == COMPLETE;

                evictionTracker.touchPage(pageId);
            }

            writeRowHnd.putPage(io.getFreeSpace(pageAddr), pageId, pageAddr, statHolder);

            return written;
        }
    }

    private final class RemoveRowHandler implements PageHandler<ReuseBag, Long> {
        /** Indicates whether partition ID should be masked from page ID. */
        private final boolean maskPartId;

        /**
         * Constructor.
         *
         * @param maskPartId Indicates whether partition ID should be masked from page ID.
         */
        RemoveRowHandler(boolean maskPartId) {
            this.maskPartId = maskPartId;
        }

        /** {@inheritDoc} */
        @Override
        public Long run(
                int cacheId,
                long pageId,
                long page,
                long pageAddr,
                PageIo iox,
                ReuseBag reuseBag,
                int itemId,
                IoStatisticsHolder statHolder
        ) throws IgniteInternalCheckedException {
            AbstractDataPageIo<T> io = (AbstractDataPageIo<T>) iox;

            int oldFreeSpace = io.getFreeSpace(pageAddr);

            assert oldFreeSpace >= 0 : oldFreeSpace;

            long nextLink = io.removeRow(pageAddr, itemId, pageSize());

            int newFreeSpace = io.getFreeSpace(pageAddr);

            if (newFreeSpace > MIN_PAGE_FREE_SPACE) {
                int newBucket = bucket(newFreeSpace, false);

                boolean putIsNeeded = oldFreeSpace <= MIN_PAGE_FREE_SPACE;

                if (!putIsNeeded) {
                    int oldBucket = bucket(oldFreeSpace, false);

                    if (oldBucket != newBucket) {
                        // It is possible that page was concurrently taken for put, in this case put will handle bucket change.
                        pageId = maskPartId ? PageIdUtils.maskPartitionId(pageId) : pageId;

                        putIsNeeded = removeDataPage(pageId, pageAddr, io, oldBucket, statHolder);
                    }
                }

                if (io.isEmpty(pageAddr)) {
                    evictionTracker.forgetPage(pageId);

                    if (putIsNeeded) {
                        reuseBag.addFreePage(recyclePage(pageId, pageAddr));
                    }
                } else if (putIsNeeded) {
                    put(null, pageId, pageAddr, newBucket, statHolder);
                }
            }

            // For common case boxed 0L will be cached inside of Long, so no garbage will be produced.
            return nextLink;
        }
    }

    /**
     * Constructor.
     *
     * @param grpId Group ID.
     * @param partId Partition ID.
     * @param name Structure name (for debug purpose).
     * @param pageMem Page memory.
     * @param reuseList Reuse list or {@code null} if this free list will be a reuse list for itself.
     * @param lockLsnr Page lock listener.
     * @param log Logger.
     * @param metaPageId Metadata page ID.
     * @param initNew {@code True} if new metadata should be initialized.
     * @param pageListCacheLimit Page list cache limit.
     * @param evictionTracker Page eviction tracker.
     * @throws IgniteInternalCheckedException If failed.
     */
    public AbstractFreeList(
            int grpId,
            int partId,
            String name,
            PageMemory pageMem,
            @Nullable ReuseList reuseList,
            PageLockListener lockLsnr,
            IgniteLogger log,
            long metaPageId,
            boolean initNew,
            @Nullable AtomicLong pageListCacheLimit,
            PageEvictionTracker evictionTracker
    ) throws IgniteInternalCheckedException {
        super(
                name,
                grpId,
                partId,
                pageMem,
                lockLsnr,
                log,
                BUCKETS,
                metaPageId
        );

        this.evictionTracker = evictionTracker;
        this.pageListCacheLimit = pageListCacheLimit;

        this.reuseList = reuseList == null ? this : reuseList;

        rmvRow = new RemoveRowHandler(grpId == 0);

        int pageSize = pageMem.pageSize();

        assert isPow2(pageSize) : "Page size must be a power of 2: " + pageSize;
        assert isPow2(BUCKETS);
        assert BUCKETS <= pageSize : pageSize;

        // TODO: https://issues.apache.org/jira/browse/IGNITE-16350
        // TODO: this constant is used because currently we cannot reuse data pages as index pages
        // TODO: and vice-versa. It should be removed when data storage format is finalized.
        minSizeForDataPage = pageSize - AbstractDataPageIo.MIN_DATA_PAGE_OVERHEAD;

        int shift = 0;

        while (pageSize > BUCKETS) {
            shift++;
            pageSize >>>= 1;
        }

        this.shift = shift;

        init(metaPageId, initNew);
    }

    /**
     * Calculates free space tracked by this FreeListImpl instance.
     *
     * @return Free space available for use, in bytes.
     */
    public long freeSpace() {
        long freeSpace = 0;

        for (int b = BUCKETS - 2; b > 0; b--) {
            long perPageFreeSpace = b << shift;

            long pages = bucketsSize.get(b);

            freeSpace += pages * perPageFreeSpace;
        }

        return freeSpace;
    }

    /** {@inheritDoc} */
    @Override
    public void dumpStatistics(IgniteLogger log) {
        long dataPages = 0;

        final boolean dumpBucketsInfo = false;

        for (int b = 0; b < BUCKETS; b++) {
            long size = bucketsSize.get(b);

            if (!isReuseBucket(b)) {
                dataPages += size;
            }

            if (dumpBucketsInfo) {
                Stripe[] stripes = getBucket(b);

                boolean empty = true;

                if (stripes != null) {
                    for (Stripe stripe : stripes) {
                        if (!stripe.empty) {
                            empty = false;

                            break;
                        }
                    }
                }

                if (log.isInfoEnabled()) {
                    log.info("Bucket [b={}, size={}, stripes={}, stripesEmpty={}]",
                            b, size, stripes != null ? stripes.length : 0, empty);
                }
            }
        }

        if (dataPages > 0) {
            if (log.isInfoEnabled()) {
                log.info("FreeList [name={}, buckets={}, dataPages={}, reusePages={}]",
                        name(), BUCKETS, dataPages, bucketsSize.get(REUSE_BUCKET));
            }
        }
    }

    /**
     * Returns bucket.
     *
     * @param freeSpace Page free space.
     * @param allowReuse {@code True} if it is allowed to get reuse bucket.
     */
    private int bucket(int freeSpace, boolean allowReuse) {
        assert freeSpace > 0 : freeSpace;

        int bucket = freeSpace >>> shift;

        assert bucket >= 0 && bucket < BUCKETS : bucket;

        if (!allowReuse && isReuseBucket(bucket)) {
            bucket--;
        }

        return bucket;
    }

    /** {@inheritDoc} */
    @Override
    protected int getBucketIndex(int freeSpace) {
        return freeSpace > MIN_PAGE_FREE_SPACE ? bucket(freeSpace, false) : -1;
    }

    /**
     * Allocates a data page.
     *
     * @param part Partition.
     * @return Page ID.
     * @throws IgniteInternalCheckedException If failed.
     */
    private long allocateDataPage(int part) throws IgniteInternalCheckedException {
        assert part <= PageIdAllocator.MAX_PARTITION_ID;

        return pageMem.allocatePage(grpId, part, FLAG_DATA);
    }

    /** {@inheritDoc} */
    @Override
    public void insertDataRow(T row, IoStatisticsHolder statHolder) throws IgniteInternalCheckedException {
        int written = 0;

        try {
            do {
                written = writeSinglePage(row, written, statHolder);
            } while (written != COMPLETE);
        } catch (IgniteInternalCheckedException | Error e) {
            throw e;
        } catch (Throwable t) {
            throw new CorruptedFreeListException("Failed to insert data row", t, grpId);
        }
    }

    /**
     * Reduces the workload on the free list by writing multiple rows into a single memory page at once.
     *
     * <p>Rows are sequentially added to the page as long as there is enough free space on it. If the row is large then those fragments
     * that occupy the whole memory page are written to other pages, and the remainder is added to the current one.
     *
     * @param rows Rows.
     * @param statHolder Statistics holder to track IO operations.
     * @throws IgniteInternalCheckedException If failed.
     */
    @Override
    public void insertDataRows(Collection<T> rows, IoStatisticsHolder statHolder) throws IgniteInternalCheckedException {
        try {
            CachedIterator<T> it = new CachedIterator<>(rows.iterator());

            int written = COMPLETE;

            while (written != COMPLETE || it.hasNext()) {
                // If eviction is required - free up memory before locking the next page.
                while (evictionTracker.evictionRequired()) {
                    evictionTracker.evictDataPage();
                }

                if (written == COMPLETE) {
                    written = writeWholePages(it.next(), statHolder);

                    continue;
                }

                T row = it.get();

                AbstractDataPageIo initIo = null;

                long pageId = takePage(row.size() - written, row, statHolder);

                if (pageId == 0L) {
                    pageId = allocateDataPage(row.partition());

                    initIo = row.ioVersions().latest();
                }

                written = write(pageId, writeRowsHnd, initIo, it, written, FAIL_I, statHolder);

                assert written != FAIL_I; // We can't fail here.
            }
        } catch (RuntimeException e) {
            throw new CorruptedFreeListException("Failed to insert data rows", e, grpId);
        }
    }

    /**
     * {@link Iterator} implementation that allows to access the current element multiple times.
     */
    private static class CachedIterator<T> implements Iterator<T> {
        private final Iterator<T> it;

        private T next;

        CachedIterator(Iterator<T> it) {
            this.it = it;
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public T next() {
            next = it.next();

            return next;
        }

        T get() {
            return next;
        }
    }

    /**
     * Write fragments of the row, which occupy the whole memory page. A data row is ignored if it is less than the max payload of an empty
     * data page.
     *
     * @param row Row to process.
     * @param statHolder Statistics holder to track IO operations.
     * @return Number of bytes written, {@link #COMPLETE} if the row was fully written, {@code 0} if data row was ignored because it is less
     *      than the max payload of an empty data page.
     * @throws IgniteInternalCheckedException If failed.
     */
    private int writeWholePages(T row, IoStatisticsHolder statHolder) throws IgniteInternalCheckedException {
        assert row.link() == 0 : row.link();

        int written = 0;
        int rowSize = row.size();

        while (rowSize - written >= minSizeForDataPage) {
            written = writeSinglePage(row, written, statHolder);
        }

        return written;
    }

    /**
     * Take a page and write row on it.
     *
     * @param row Row to write.
     * @param written Written size.
     * @param statHolder Statistics holder to track IO operations.
     * @return Number of bytes written, {@link #COMPLETE} if the row was fully written.
     * @throws IgniteInternalCheckedException If failed.
     */
    private int writeSinglePage(T row, int written, IoStatisticsHolder statHolder) throws IgniteInternalCheckedException {
        AbstractDataPageIo initIo = null;

        long pageId = takePage(row.size() - written, row, statHolder);

        if (pageId == 0L) {
            pageId = allocateDataPage(row.partition());

            initIo = row.ioVersions().latest();
        }

        written = write(pageId, writeRowHnd, initIo, row, written, FAIL_I, statHolder);

        assert written != FAIL_I; // We can't fail here.

        return written;
    }

    /**
     * Take page from free list.
     *
     * @param size Required free space on page.
     * @param row Row to write.
     * @param statHolder Statistics holder to track IO operations.
     * @return Page identifier or {@code 0} if no page found in free list.
     * @throws IgniteInternalCheckedException If failed.
     */
    private long takePage(int size, T row, IoStatisticsHolder statHolder) throws IgniteInternalCheckedException {
        long pageId = 0;

        if (size < minSizeForDataPage) {
            for (int b = bucket(size, false) + 1; b < REUSE_BUCKET; b++) {
                pageId = takeEmptyPage(b, row.ioVersions(), statHolder);

                if (pageId != 0L) {
                    break;
                }
            }
        }

        if (pageId == 0L) { // Handle reuse bucket.
            if (reuseList == this) {
                pageId = takeEmptyPage(REUSE_BUCKET, row.ioVersions(), statHolder);
            } else {
                pageId = reuseList.takeRecycledPage();

                if (pageId != 0) {
                    pageId = reuseList.initRecycledPage(pageId, FLAG_DATA, row.ioVersions().latest());
                }
            }
        }

        if (pageId == 0L) {
            return 0;
        }

        assert PageIdUtils.flag(pageId) == FLAG_DATA
                : "rowVersions=" + row.ioVersions() + ", pageId=" + PageIdUtils.toDetailString(pageId);

        return PageIdUtils.changePartitionId(pageId, row.partition());
    }

    /**
     * Reused page must obtain correctly assaembled page id, then initialized by proper {@link PageIo} instance and non-zero {@code itemId}
     * of reused page id must be saved into special place.
     *
     * @param row Row.
     * @param reusedPageId Reused page id.
     * @param statHolder Statistics holder to track IO operations.
     * @return Prepared page id.
     * @see PagesList#initReusedPage(long, long, int, byte, PageIo)
     */
    private long initReusedPage(T row, long reusedPageId, IoStatisticsHolder statHolder) throws IgniteInternalCheckedException {
        long reusedPage = acquirePage(reusedPageId, statHolder);
        try {
            long reusedPageAddr = writeLock(reusedPageId, reusedPage);

            assert reusedPageAddr != 0;

            try {
                return initReusedPage(
                        reusedPageId,
                        reusedPageAddr,
                        row.partition(),
                        FLAG_DATA,
                        row.ioVersions().latest()
                );
            } finally {
                writeUnlock(reusedPageId, reusedPage, reusedPageAddr, true);
            }
        } finally {
            releasePage(reusedPageId, reusedPage);
        }
    }

    /** {@inheritDoc} */
    @Override
    public <S, R> R updateDataRow(
            long link,
            PageHandler<S, R> pageHnd,
            S arg,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        assert link != 0;

        try {
            long pageId = pageId(link);
            int itemId = itemId(link);

            R updRes = write(pageId, pageHnd, arg, itemId, null, statHolder);

            assert updRes != null; // Can't fail here.

            return updRes;
        } catch (AssertionError e) {
            throw corruptedFreeListException(e);
        } catch (IgniteInternalCheckedException | Error e) {
            throw e;
        } catch (Throwable t) {
            throw new CorruptedFreeListException("Failed to update data row", t, grpId);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void removeDataRowByLink(long link, IoStatisticsHolder statHolder) throws IgniteInternalCheckedException {
        assert link != 0;

        try {
            long pageId = pageId(link);
            int itemId = itemId(link);

            ReuseBag bag = new LongListReuseBag();

            long nextLink = write(pageId, rmvRow, bag, itemId, FAIL_L, statHolder);

            assert nextLink != FAIL_L; // Can't fail here.

            while (nextLink != 0L) {
                itemId = itemId(nextLink);
                pageId = pageId(nextLink);

                nextLink = write(pageId, rmvRow, bag, itemId, FAIL_L, statHolder);

                assert nextLink != FAIL_L; // Can't fail here.
            }

            reuseList.addForRecycle(bag);
        } catch (AssertionError e) {
            throw corruptedFreeListException(e);
        } catch (IgniteInternalCheckedException | Error e) {
            throw e;
        } catch (Throwable t) {
            throw new CorruptedFreeListException("Failed to remove data by link", t, grpId);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected Stripe[] getBucket(int bucket) {
        return buckets.get(bucket);
    }

    /** {@inheritDoc} */
    @Override
    protected boolean casBucket(int bucket, Stripe[] exp, Stripe[] upd) {
        boolean res = buckets.compareAndSet(bucket, exp, upd);

        if (log.isDebugEnabled()) {
            log.debug("CAS bucket [list=" + name() + ", bucket=" + bucket + ", old=" + Arrays.toString(exp)
                    + ", new=" + Arrays.toString(upd) + ", res=" + res + ']');
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override
    protected boolean isReuseBucket(int bucket) {
        return bucket == REUSE_BUCKET;
    }

    /** {@inheritDoc} */
    @Override
    protected PagesCache getBucketCache(int bucket, boolean create) {
        PagesCache pagesCache = bucketCaches.get(bucket);

        if (pagesCache == null && create && !bucketCaches.compareAndSet(bucket, null, pagesCache = new PagesCache(pageListCacheLimit))) {
            pagesCache = bucketCaches.get(bucket);
        }

        return pagesCache;
    }

    /**
     * Returns number of empty data pages in free list.
     */
    public int emptyDataPages() {
        return (int) bucketsSize.get(REUSE_BUCKET);
    }

    /** {@inheritDoc} */
    @Override
    public void addForRecycle(ReuseBag bag) throws IgniteInternalCheckedException {
        assert reuseList == this : "not allowed to be a reuse list";

        try {
            put(bag, 0, 0L, REUSE_BUCKET, IoStatisticsHolderNoOp.INSTANCE);
        } catch (AssertionError e) {
            throw corruptedFreeListException(e);
        } catch (IgniteInternalCheckedException | Error e) {
            throw e;
        } catch (Throwable t) {
            throw new CorruptedFreeListException("Failed to add page for recycle", t, grpId);
        }
    }

    /** {@inheritDoc} */
    @Override
    public long takeRecycledPage() throws IgniteInternalCheckedException {
        assert reuseList == this : "not allowed to be a reuse list";

        try {
            return takeEmptyPage(REUSE_BUCKET, null, IoStatisticsHolderNoOp.INSTANCE);
        } catch (AssertionError e) {
            throw corruptedFreeListException(e);
        } catch (IgniteInternalCheckedException | Error e) {
            throw e;
        } catch (Throwable t) {
            throw new CorruptedFreeListException("Failed to take recycled page", t, grpId);
        }
    }

    /** {@inheritDoc} */
    @Override
    public long initRecycledPage(long pageId, byte flag, PageIo initIo) throws IgniteInternalCheckedException {
        return initRecycledPage0(pageId, flag, initIo);
    }

    /** {@inheritDoc} */
    @Override
    public long recycledPagesCount() throws IgniteInternalCheckedException {
        assert reuseList == this : "not allowed to be a reuse list";

        try {
            return storedPagesCount(REUSE_BUCKET);
        } catch (AssertionError e) {
            throw corruptedFreeListException(e);
        } catch (IgniteInternalCheckedException | Error e) {
            throw e;
        } catch (Throwable t) {
            throw new CorruptedFreeListException("Failed to count recycled pages", t, grpId);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "FreeList [name=" + name() + ']';
    }
}
