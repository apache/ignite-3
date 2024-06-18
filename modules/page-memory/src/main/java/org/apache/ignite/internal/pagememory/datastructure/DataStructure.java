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

package org.apache.ignite.internal.pagememory.datastructure;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.MAX_PARTITION_ID;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.MAX_ITEM_ID_NUM;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.flag;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.toDetailString;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.PageIdAllocator;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.reuse.ReuseBag;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.util.PageHandler;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.pagememory.util.PageLockListener;
import org.apache.ignite.internal.util.StringUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for all the data structures based on {@link PageMemory}.
 */
public abstract class DataStructure implements ManuallyCloseable {
    /** For tests. */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-16350
    public static Random rnd;

    /** Structure name. */
    private final String name;

    /** Page lock listener. */
    private final PageLockListener lockLsnr;

    /** Group id. */
    protected final int grpId;

    /** Group name. */
    protected final String grpName;

    /** Page memory. */
    protected final PageMemory pageMem;

    /** Reuse list. */
    @Nullable
    protected ReuseList reuseList;

    /** Default flag value for allocated pages. One of {@link PageIdAllocator#FLAG_DATA} or {@link PageIdAllocator#FLAG_AUX}. */
    protected final byte defaultPageFlag;

    /** Partition id. */
    protected final int partId;

    /**
     * Constructor.
     *
     * @param name Structure name (for debugging purposes).
     * @param grpId Group ID.
     * @param grpName Group name.
     * @param partId Partition ID.
     * @param pageMem Page memory.
     * @param lockLsnr Page lock listener.
     * @param defaultPageFlag Default flag value for allocated pages. One of {@link PageIdAllocator#FLAG_DATA} or {@link
     *      PageIdAllocator#FLAG_AUX}.
     */
    public DataStructure(
            String name,
            int grpId,
            @Nullable String grpName,
            int partId,
            PageMemory pageMem,
            PageLockListener lockLsnr,
            byte defaultPageFlag
    ) {
        assert !StringUtils.nullOrEmpty(name);
        assert pageMem != null;
        assert partId >= 0 && partId <= MAX_PARTITION_ID : partId;

        this.name = name;
        this.grpId = grpId;
        this.grpName = grpName;
        this.partId = partId;
        this.pageMem = pageMem;
        this.lockLsnr = lockLsnr;
        this.defaultPageFlag = defaultPageFlag;
    }

    /**
     * Returns structure name.
     */
    public final String name() {
        return name;
    }

    /**
     * Returns group id.
     */
    public final int groupId() {
        return grpId;
    }

    /**
     * Returns random value from {@code 0} (inclusive) to the given max value (exclusive).
     *
     * @param max Upper bound (exclusive). Must be positive.
     */
    public static int randomInt(int max) {
        Random rnd0 = rnd != null ? rnd : ThreadLocalRandom.current();

        return rnd0.nextInt(max);
    }

    /**
     * Allocates a new or reuses a free page.
     *
     * @param bag Reuse bag.
     * @return Allocated page id.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected final long allocatePage(@Nullable ReuseBag bag) throws IgniteInternalCheckedException {
        return allocatePage(bag, true);
    }

    /**
     * Allocates a new or reuses a free page.
     *
     * @param bag Reuse Bag.
     * @param useRecycled Use recycled page.
     * @return Allocated page id.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected final long allocatePage(@Nullable ReuseBag bag, boolean useRecycled) throws IgniteInternalCheckedException {
        return pageMem.allocatePage(reuseList, bag, useRecycled, grpId, partId, defaultPageFlag);
    }

    /**
     * Allocates a new page.
     *
     * @return Page ID of newly allocated page.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected long allocatePageNoReuse() throws IgniteInternalCheckedException {
        return pageMem.allocatePageNoReuse(grpId, partId, defaultPageFlag);
    }

    /**
     * Acquires the page by the given ID. This method will allocate a page with the given ID if it doesn't exist.
     *
     * <p>NOTE: Each page obtained with this method must be released by calling {@link #releasePage}.
     *
     * @param pageId Page ID.
     * @param statHolder Statistics holder to track IO operations.
     * @return Page pointer.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected final long acquirePage(long pageId, IoStatisticsHolder statHolder) throws IgniteInternalCheckedException {
        assert partitionId(pageId) >= 0 && partitionId(pageId) <= MAX_PARTITION_ID : toDetailString(pageId);

        return pageMem.acquirePage(grpId, pageId, statHolder);
    }

    /**
     * Releases pages acquired by {@link #acquirePage}.
     *
     * @param pageId Page ID to release.
     * @param page Page pointer returned by the corresponding acquirePage call.
     */
    protected final void releasePage(long pageId, long page) {
        pageMem.releasePage(grpId, pageId, page);
    }

    /**
     * Tries to acquire the write lock on the page.
     *
     * @param pageId Page ID
     * @param page Page pointer.
     * @return Page address or {@code 0} if failed to lock due to recycling.
     */
    protected final long tryWriteLock(long pageId, long page) {
        return PageHandler.writeLock(pageMem, grpId, pageId, page, lockLsnr, true);
    }

    /**
     * Acquires the write lock on the page.
     *
     * @param pageId Page ID
     * @param page Page pointer.
     * @return Page address.
     */
    protected final long writeLock(long pageId, long page) {
        return PageHandler.writeLock(pageMem, grpId, pageId, page, lockLsnr, false);
    }

    /**
     * Acquires the read lock on the page.
     *
     * @param pageId Page ID
     * @param page Page pointer.
     * @return Page address.
     */
    protected final long readLock(long pageId, long page) {
        return PageHandler.readLock(pageMem, grpId, pageId, page, lockLsnr);
    }

    /**
     * Releases acquired read lock.
     *
     * @param pageId Page ID
     * @param page Page pointer.
     * @param pageAddr Page address.
     */
    protected final void readUnlock(long pageId, long page, long pageAddr) {
        PageHandler.readUnlock(pageMem, grpId, pageId, page, pageAddr, lockLsnr);
    }

    /**
     * Releases acquired write lock.
     *
     * @param pageId Page ID
     * @param page Page pointer.
     * @param pageAddr Page address.
     * @param dirty Dirty flag.
     */
    protected final void writeUnlock(long pageId, long page, long pageAddr, boolean dirty) {
        PageHandler.writeUnlock(pageMem, grpId, pageId, page, pageAddr, lockLsnr, dirty);
    }

    /**
     * Executes handler under the write lock or returns lockFailed if lock failed.
     *
     * @param pageId Page ID.
     * @param h Handler.
     * @param intArg Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @param statHolder Statistics holder to track IO operations.
     * @return Handler result.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected final <R> R write(
            long pageId,
            PageHandler<?, R> h,
            int intArg,
            R lockFailed,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        return PageHandler.writePage(pageMem, grpId, pageId, lockLsnr, h, null, null, intArg, lockFailed, statHolder);
    }

    /**
     * Executes handler under the write lock or returns lockFailed if lock failed.
     *
     * @param pageId Page ID.
     * @param h Handler.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @param statHolder Statistics holder to track IO operations.
     * @return Handler result.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected final <X, R> R write(
            long pageId,
            PageHandler<X, R> h,
            X arg,
            int intArg,
            R lockFailed,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        return PageHandler.writePage(pageMem, grpId, pageId, lockLsnr, h, null, arg, intArg, lockFailed, statHolder);
    }

    /**
     * Executes handler under the write lock or returns lockFailed if lock failed.
     *
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param h Handler.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @param statHolder Statistics holder to track IO operations.
     * @return Handler result.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected final <X, R> R write(
            long pageId,
            long page,
            PageHandler<X, R> h,
            X arg,
            int intArg,
            R lockFailed,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        return PageHandler.writePage(pageMem, grpId, pageId, page, lockLsnr, h, null, arg, intArg, lockFailed, statHolder);
    }

    /**
     * Executes handler under the write lock or returns lockFailed if lock failed.
     *
     * @param pageId Page ID.
     * @param h Handler.
     * @param init IO for new page initialization or {@code null} if it is an existing page.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @param statHolder Statistics holder to track IO operations.
     * @return Handler result.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected final <X, R> R write(
            long pageId,
            PageHandler<X, R> h,
            PageIo init,
            X arg,
            int intArg,
            R lockFailed,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        return PageHandler.writePage(pageMem, grpId, pageId, lockLsnr, h, init, arg, intArg, lockFailed, statHolder);
    }

    /**
     * Executes handler under the read lock or returns lockFailed if lock failed.
     *
     * @param pageId Page ID.
     * @param h Handler.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @param statHolder Statistics holder to track IO operations.
     * @return Handler result.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected final <X, R> R read(
            long pageId,
            PageHandler<X, R> h,
            X arg,
            int intArg,
            R lockFailed,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        return PageHandler.readPage(pageMem, grpId, pageId, lockLsnr, h, arg, intArg, lockFailed, statHolder);
    }

    /**
     * Executes handler under the read lock or returns lockFailed if lock failed.
     *
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param h Handler.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @param statHolder Statistics holder to track IO operations.
     * @return Handler result.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected final <X, R> R read(
            long pageId,
            long page,
            PageHandler<X, R> h,
            X arg,
            int intArg,
            R lockFailed,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        return PageHandler.readPage(pageMem, grpId, pageId, page, lockLsnr, h, arg, intArg, lockFailed, statHolder);
    }

    /**
     * Initializes a new page.
     *
     * @param pageId Page ID.
     * @param init IO for new page initialization.
     * @throws IgniteInternalCheckedException if failed.
     */
    protected final void init(long pageId, PageIo init) throws IgniteInternalCheckedException {
        PageHandler.initPage(pageMem, grpId, pageId, init, lockLsnr, IoStatisticsHolderNoOp.INSTANCE);
    }

    /**
     * Increments the rotation ID of the page, marks it as reusable.
     *
     * @param pageId Page ID.
     * @param pageAddr Page address.
     * @return Page ID with the incremented rotation ID.
     * @see FullPageId
     */
    protected static long recyclePage(long pageId, long pageAddr) {
        long recycled = 0;

        if (flag(pageId) == FLAG_DATA) {
            int rotatedIdPart = PageIo.getRotatedIdPart(pageAddr);

            if (rotatedIdPart != 0) {
                recycled = PageIdUtils.link(pageId, rotatedIdPart);

                PageIo.setRotatedIdPart(pageAddr, 0);
            }
        }

        if (recycled == 0) {
            recycled = PageIdUtils.rotatePageId(pageId);
        }

        assert itemId(recycled) > 0 && itemId(recycled) <= MAX_ITEM_ID_NUM : StringUtils.hexLong(recycled);

        PageIo.setPageId(pageAddr, recycled);

        return recycled;
    }

    /**
     * Returns a page size without the encryption overhead, in bytes.
     */
    protected int pageSize() {
        return pageMem.realPageSize(grpId);
    }

    /**
     * Frees the resources allocated by this structure.
     */
    @Override
    public void close() {
        lockLsnr.close();
    }
}
