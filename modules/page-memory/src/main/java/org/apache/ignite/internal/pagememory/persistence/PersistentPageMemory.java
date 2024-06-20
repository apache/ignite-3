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

import static java.lang.System.lineSeparator;
import static org.apache.ignite.internal.pagememory.FullPageId.NULL_PAGE;
import static org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileConfigurationSchema.CLOCK_REPLACEMENT_MODE;
import static org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileConfigurationSchema.RANDOM_LRU_REPLACEMENT_MODE;
import static org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileConfigurationSchema.SEGMENTED_LRU_REPLACEMENT_MODE;
import static org.apache.ignite.internal.pagememory.io.PageIo.getCrc;
import static org.apache.ignite.internal.pagememory.io.PageIo.getPageId;
import static org.apache.ignite.internal.pagememory.io.PageIo.getType;
import static org.apache.ignite.internal.pagememory.io.PageIo.getVersion;
import static org.apache.ignite.internal.pagememory.io.PageIo.setPageId;
import static org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency.MUST_TRIGGER;
import static org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency.NOT_REQUIRED;
import static org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency.SHOULD_TRIGGER;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.dirty;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.fullPageId;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.isAcquired;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.readPageId;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.tempBufferPointer;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.writeTimestamp;
import static org.apache.ignite.internal.pagememory.persistence.PagePool.SEGMENT_INDEX_MASK;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.effectivePageId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.tag;
import static org.apache.ignite.internal.util.ArrayUtils.concat;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;
import static org.apache.ignite.internal.util.GridUnsafe.compareAndSwapInt;
import static org.apache.ignite.internal.util.GridUnsafe.compareAndSwapLong;
import static org.apache.ignite.internal.util.GridUnsafe.copyMemory;
import static org.apache.ignite.internal.util.GridUnsafe.getInt;
import static org.apache.ignite.internal.util.GridUnsafe.getLong;
import static org.apache.ignite.internal.util.GridUnsafe.putIntVolatile;
import static org.apache.ignite.internal.util.GridUnsafe.wrapPointer;
import static org.apache.ignite.internal.util.GridUnsafe.zeroMemory;
import static org.apache.ignite.internal.util.IgniteUtils.hash;
import static org.apache.ignite.internal.util.IgniteUtils.readableSize;
import static org.apache.ignite.internal.util.IgniteUtils.safeAbs;
import static org.apache.ignite.internal.util.OffheapReadWriteLock.TAG_LOCK_ALWAYS;
import static org.apache.ignite.internal.util.StringUtils.hexLong;
import static org.apache.ignite.internal.util.StringUtils.toHexString;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileView;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.mem.DirectMemoryProvider;
import org.apache.ignite.internal.pagememory.mem.DirectMemoryRegion;
import org.apache.ignite.internal.pagememory.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.pagememory.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointMetricsTracker;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointPages;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTimeoutLock;
import org.apache.ignite.internal.pagememory.persistence.replacement.ClockPageReplacementPolicyFactory;
import org.apache.ignite.internal.pagememory.persistence.replacement.DelayedPageReplacementTracker;
import org.apache.ignite.internal.pagememory.persistence.replacement.PageReplacementPolicy;
import org.apache.ignite.internal.pagememory.persistence.replacement.PageReplacementPolicyFactory;
import org.apache.ignite.internal.pagememory.persistence.replacement.RandomLruPageReplacementPolicyFactory;
import org.apache.ignite.internal.pagememory.persistence.replacement.SegmentedLruPageReplacementPolicyFactory;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Page memory with some persistence related additions.
 *
 * <p>Page header structure is described by the following diagram.
 *
 * <p>When page is not allocated (in a free list):
 * <pre>
 * +--------+------------------------------------------------------+
 * |8 bytes |         PAGE_SIZE + PAGE_OVERHEAD - 8 bytes          |
 * +--------+------------------------------------------------------+
 * |Next ptr|                      Page data                       |
 * +--------+------------------------------------------------------+
 * </pre>
 *
 * <p>When page is allocated and is in use:
 * <pre>
 * +------------------+--------+--------+----+----+--------+--------+----------------------+
 * |     8 bytes      |8 bytes |8 bytes |4 b |4 b |8 bytes |8 bytes |       PAGE_SIZE      |
 * +------------------+--------+--------+----+----+--------+--------+----------------------+
 * | Marker/Timestamp |Rel ptr |Page ID |C ID|PIN | LOCK   |TMP BUF |       Page data      |
 * +------------------+--------+--------+----+----+--------+--------+----------------------+
 * </pre>
 *
 * <p>Note that first 8 bytes of page header are used either for page marker or for next relative pointer depending on whether the page is
 * in use or not.
 */
@SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
public class PersistentPageMemory implements PageMemory {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(PersistentPageMemory.class);

    /** Full relative pointer mask. */
    public static final long RELATIVE_PTR_MASK = 0xFFFFFFFFFFFFFFL;

    /** Invalid relative pointer value. */
    public static final long INVALID_REL_PTR = RELATIVE_PTR_MASK;

    /** Pointer which means that this page is outdated (for example, group was destroyed, partition eviction'd happened. */
    public static final long OUTDATED_REL_PTR = INVALID_REL_PTR + 1;

    /** Page lock offset. */
    public static final int PAGE_LOCK_OFFSET = 32;

    /** 8b Marker/timestamp 8b Relative pointer 8b Page ID 4b Group ID 4b Pin count 8b Lock 8b Temporary buffer. */
    public static final int PAGE_OVERHEAD = 48;

    /** Try again tag. */
    public static final int TRY_AGAIN_TAG = -1;

    /** Data region configuration view. */
    private final PersistentPageMemoryProfileView storageProfileView;

    /** Page IO registry. */
    private final PageIoRegistry ioRegistry;

    /** Page manager. */
    private final PageReadWriteManager pageStoreManager;

    /** Page size. */
    private final int sysPageSize;

    /** Page replacement policy factory. */
    private final PageReplacementPolicyFactory pageReplacementPolicyFactory;

    /** Direct memory allocator. */
    private final DirectMemoryProvider directMemoryProvider;

    /** Segments array, {@code null} if not {@link #start() started}. */
    @Nullable
    private volatile Segment[] segments;

    /** Lock for segments changes. */
    private final Object segmentsLock = new Object();

    /** Offheap read write lock instance. */
    private final OffheapReadWriteLock rwLock;

    /** Callback invoked to track changes in pages. {@code Null} if page tracking functionality is disabled. */
    @Nullable
    private final PageChangeTracker changeTracker;

    /** Field updater. */
    private static final AtomicIntegerFieldUpdater<PersistentPageMemory> pageReplacementWarnedFieldUpdater =
            AtomicIntegerFieldUpdater.newUpdater(PersistentPageMemory.class, "pageReplacementWarned");

    /** Flag indicating page replacement started (rotation with disk), allocating new page requires freeing old one. */
    private volatile int pageReplacementWarned;

    /** Segments sizes, the last one being the {@link #checkpointPool checkpoint buffer} size. */
    // TODO: IGNITE-16350 Consider splitting into segments and the checkpoint buffer
    private final long[] sizes;

    /** {@code False} if memory was not started or already stopped and is not supposed for any usage. */
    private volatile boolean started;

    /** See {@link #checkpointUrgency()}. */
    private final AtomicReference<CheckpointUrgency> checkpointUrgency = new AtomicReference<>(NOT_REQUIRED);

    /** Checkpoint page pool, {@code null} if not {@link #start() started}. */
    @Nullable
    private volatile PagePool checkpointPool;

    /**
     * Delayed page replacement (rotation with disk) tracker. Because other thread may require exactly the same page to be loaded from
     * store, reads are protected by locking.
     */
    private final DelayedPageReplacementTracker delayedPageReplacementTracker;

    /** Checkpoint timeout lock. */
    private final CheckpointTimeoutLock checkpointTimeoutLock;

    /**
     * Constructor.
     *
     * @param storageProfileConfiguration Storage profile configuration.
     * @param ioRegistry IO registry.
     * @param segmentSizes Segments sizes in bytes.
     * @param checkpointBufferSize Checkpoint buffer size in bytes.
     * @param pageStoreManager Page store manager.
     * @param changeTracker Callback invoked to track changes in pages.
     * @param flushDirtyPageForReplacement Write callback invoked when a dirty page is removed for replacement.
     * @param checkpointTimeoutLock Checkpoint timeout lock.
     * @param pageSize Page size in bytes.
     */
    public PersistentPageMemory(
            PersistentPageMemoryProfileConfiguration storageProfileConfiguration,
            PageIoRegistry ioRegistry,
            long[] segmentSizes,
            long checkpointBufferSize,
            PageReadWriteManager pageStoreManager,
            @Nullable PageChangeTracker changeTracker,
            WriteDirtyPage flushDirtyPageForReplacement,
            CheckpointTimeoutLock checkpointTimeoutLock,
            // TODO: IGNITE-17017 Move to common config
            int pageSize
    ) {
        this.storageProfileView = (PersistentPageMemoryProfileView) storageProfileConfiguration.value();
        this.ioRegistry = ioRegistry;
        this.sizes = concat(segmentSizes, checkpointBufferSize);
        this.pageStoreManager = pageStoreManager;
        this.changeTracker = changeTracker;
        this.checkpointTimeoutLock = checkpointTimeoutLock;

        directMemoryProvider = new UnsafeMemoryProvider(null);

        sysPageSize = pageSize + PAGE_OVERHEAD;

        rwLock = new OffheapReadWriteLock(128);

        String replacementMode = storageProfileView.replacementMode();

        switch (replacementMode) {
            case RANDOM_LRU_REPLACEMENT_MODE:
                pageReplacementPolicyFactory = new RandomLruPageReplacementPolicyFactory();

                break;
            case SEGMENTED_LRU_REPLACEMENT_MODE:
                pageReplacementPolicyFactory = new SegmentedLruPageReplacementPolicyFactory();

                break;
            case CLOCK_REPLACEMENT_MODE:
                pageReplacementPolicyFactory = new ClockPageReplacementPolicyFactory();

                break;
            default:
                throw new IgniteInternalException("Unexpected page replacement mode: " + replacementMode);
        }

        delayedPageReplacementTracker = new DelayedPageReplacementTracker(pageSize, flushDirtyPageForReplacement, LOG, sizes.length - 1);
    }

    /** {@inheritDoc} */
    @Override
    public void start() throws IgniteInternalException {
        synchronized (segmentsLock) {
            if (started) {
                return;
            }

            started = true;

            directMemoryProvider.initialize(sizes);

            List<DirectMemoryRegion> regions = new ArrayList<>(sizes.length);

            while (true) {
                DirectMemoryRegion reg = directMemoryProvider.nextRegion();

                if (reg == null) {
                    break;
                }

                regions.add(reg);
            }

            int regs = regions.size();

            Segment[] segments = new Segment[regs - 1];

            DirectMemoryRegion checkpointRegion = regions.get(regs - 1);

            checkpointPool = new PagePool(regs - 1, checkpointRegion, sysPageSize, rwLock);

            long checkpointBufferSize = checkpointRegion.size();

            long totalAllocated = 0;
            int pages = 0;
            long totalTblSize = 0;
            long totalReplSize = 0;

            for (int i = 0; i < regs - 1; i++) {
                assert i < segments.length;

                DirectMemoryRegion reg = regions.get(i);

                totalAllocated += reg.size();

                segments[i] = new Segment(i, regions.get(i));

                pages += segments[i].pages();
                totalTblSize += segments[i].tableSize();
                totalReplSize += segments[i].replacementSize();
            }

            this.segments = segments;

            if (LOG.isInfoEnabled()) {
                LOG.info("Started page memory [memoryAllocated={}, pages={}, tableSize={}, replacementSize={}, checkpointBuffer={}]",
                        readableSize(totalAllocated, false), pages, readableSize(totalTblSize, false),
                        readableSize(totalReplSize, false), readableSize(checkpointBufferSize, false));
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop(boolean deallocate) throws IgniteInternalException {
        synchronized (segmentsLock) {
            if (!started) {
                return;
            }

            LOG.debug("Stopping page memory");

            if (segments != null) {
                for (Segment seg : segments) {
                    seg.close();
                }
            }

            started = false;

            directMemoryProvider.shutdown(deallocate);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void releasePage(int grpId, long pageId, long page) {
        assert started;

        Segment seg = segment(grpId, pageId);

        seg.readLock().lock();

        try {
            seg.releasePage(page);
        } finally {
            seg.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public long readLock(int grpId, long pageId, long page) {
        assert started;

        return readLock(page, pageId, false);
    }

    /**
     * Acquires a read lock associated with the given page.
     *
     * @param absPtr Absolute pointer to read lock.
     * @param pageId Page ID.
     * @param force Force flag.
     * @param touch Update page timestamp.
     * @return Pointer to the page read buffer.
     */
    public long readLock(long absPtr, long pageId, boolean force, boolean touch) {
        assert started;

        int tag = force ? -1 : tag(pageId);

        boolean locked = rwLock.readLock(absPtr + PAGE_LOCK_OFFSET, tag);

        if (!locked) {
            return 0;
        }

        if (touch) {
            writeTimestamp(absPtr, coarseCurrentTimeMillis());
        }

        assert getCrc(absPtr + PAGE_OVERHEAD) == 0; // TODO IGNITE-16612

        return absPtr + PAGE_OVERHEAD;
    }

    private long readLock(long absPtr, long pageId, boolean force) {
        return readLock(absPtr, pageId, force, true);
    }

    /** {@inheritDoc} */
    @Override
    public void readUnlock(int grpId, long pageId, long page) {
        assert started;

        readUnlockPage(page);
    }

    /** {@inheritDoc} */
    @Override
    public long writeLock(int grpId, long pageId, long page) {
        assert started;

        return writeLock(grpId, pageId, page, false);
    }

    /**
     * Acquired a write lock on the page.
     *
     * @param grpId Group ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param restore Determines if the page is locked for restore memory (crash recovery).
     * @return Pointer to the page read buffer.
     */
    public long writeLock(int grpId, long pageId, long page, boolean restore) {
        assert started;

        return writeLockPage(page, new FullPageId(pageId, grpId), !restore);
    }

    /** {@inheritDoc} */
    @Override
    public long tryWriteLock(int grpId, long pageId, long page) {
        assert started;

        return tryWriteLockPage(page, new FullPageId(pageId, grpId), true);
    }

    /** {@inheritDoc} */
    @Override
    public void writeUnlock(int grpId, long pageId, long page, boolean dirtyFlag) {
        assert started;

        writeUnlock(grpId, pageId, page, dirtyFlag, false);
    }

    /**
     * Releases locked page.
     *
     * @param grpId Group ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param dirtyFlag Determines whether the page was modified since the last checkpoint.
     * @param restore Determines if the page is locked for restore.
     */
    public void writeUnlock(int grpId, long pageId, long page, boolean dirtyFlag, boolean restore) {
        assert started;

        writeUnlockPage(page, new FullPageId(pageId, grpId), dirtyFlag, restore);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isDirty(int grpId, long pageId, long page) {
        assert started;

        return isDirty(page);
    }

    /**
     * Returns {@code true} if page is dirty.
     *
     * @param absPtr Absolute pointer.
     */
    boolean isDirty(long absPtr) {
        return dirty(absPtr);
    }

    /** {@inheritDoc} */
    @Override
    public long allocatePageNoReuse(int grpId, int partId, byte flags) throws IgniteInternalCheckedException {
        assert partId >= 0 && partId <= MAX_PARTITION_ID : partId;

        assert started;
        assert checkpointTimeoutLock.checkpointLockIsHeldByThread();

        long pageId = pageStoreManager.allocatePage(grpId, partId, flags);

        // We need to allocate page in memory for marking it dirty to save it in the next checkpoint.
        // Otherwise, it is possible that on file will be empty page which will be saved at snapshot and read with error
        // because there is no crc inside them.
        Segment seg = segment(grpId, pageId);

        seg.writeLock().lock();


        try {
            FullPageId fullId = new FullPageId(pageId, grpId);

            long relPtr = seg.loadedPages.get(
                    grpId,
                    effectivePageId(pageId),
                    seg.partGeneration(grpId, partId),
                    INVALID_REL_PTR,
                    OUTDATED_REL_PTR
            );

            if (relPtr == OUTDATED_REL_PTR) {
                relPtr = seg.refreshOutdatedPage(grpId, pageId, false);

                seg.pageReplacementPolicy.onRemove(relPtr);
            }

            if (relPtr == INVALID_REL_PTR) {
                relPtr = seg.borrowOrAllocateFreePage(pageId);
            }

            if (relPtr == INVALID_REL_PTR) {
                relPtr = seg.removePageForReplacement();
            }

            long absPtr = seg.absolute(relPtr);

            zeroMemory(absPtr + PAGE_OVERHEAD, pageSize());

            fullPageId(absPtr, fullId);
            writeTimestamp(absPtr, coarseCurrentTimeMillis());

            rwLock.init(absPtr + PAGE_LOCK_OFFSET, tag(pageId));

            assert getCrc(absPtr + PAGE_OVERHEAD) == 0; // TODO IGNITE-16612

            assert !isAcquired(absPtr) :
                    "Pin counter must be 0 for a new page [relPtr=" + hexLong(relPtr)
                            + ", absPtr=" + hexLong(absPtr) + ", pinCntr=" + PageHeader.pinCount(absPtr) + ']';

            setDirty(fullId, absPtr, true, true);

            seg.pageReplacementPolicy.onMiss(relPtr);

            seg.loadedPages.put(grpId, effectivePageId(pageId), relPtr, seg.partGeneration(grpId, partId));
        } catch (IgniteOutOfMemoryException oom) {
            IgniteOutOfMemoryException e = new IgniteOutOfMemoryException("Out of memory in data region ["
                    + "name=" + storageProfileView.name()
                    + ", size=" + readableSize(storageProfileView.size(), false)
                    + ", persistence=true] Try the following:" + lineSeparator()
                    + "  ^-- Increase maximum off-heap memory size (PersistentPageMemoryProfileConfigurationSchema.size)"
                    + lineSeparator()
                    + "  ^-- Enable eviction or expiration policies"
            );

            e.initCause(oom);

            throw e;
        } finally {
            seg.writeLock().unlock();
        }

        // Finish replacement only when an exception wasn't thrown otherwise it possible to corrupt B+Tree.
        delayedPageReplacementTracker.delayedPageWrite().finishReplacement();

        return pageId;
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer pageBuffer(long pageAddr) {
        return wrapPointer(pageAddr, pageSize());
    }

    /** {@inheritDoc} */
    @Override
    public boolean freePage(int grpId, long pageId) {
        assert false : "Free page should be never called directly when persistence is enabled.";

        return false;
    }

    /** {@inheritDoc} */
    @Override
    public long acquirePage(int grpId, long pageId) throws IgniteInternalCheckedException {
        return acquirePage(grpId, pageId, IoStatisticsHolderNoOp.INSTANCE, false);
    }

    /** {@inheritDoc} */
    @Override
    public long acquirePage(int grpId, long pageId, IoStatisticsHolder statHolder) throws IgniteInternalCheckedException {
        return acquirePage(grpId, pageId, statHolder, false);
    }

    /**
     * Returns an absolute pointer to a page, associated with the given page ID.
     *
     * @param grpId Group ID.
     * @param pageId Page ID.
     * @param pageAllocated Flag is set if new page was allocated in offheap memory.
     * @return Page.
     * @throws IgniteInternalCheckedException If failed.
     * @see #acquirePage(int, long) Sets additional flag indicating that page was not found in memory and had to be allocated.
     */
    public long acquirePage(int grpId, long pageId, AtomicBoolean pageAllocated) throws IgniteInternalCheckedException {
        return acquirePage(grpId, pageId, IoStatisticsHolderNoOp.INSTANCE, false, pageAllocated);
    }

    /**
     * Returns an absolute pointer to a page, associated with the given page ID.
     *
     * @param grpId Group ID.
     * @param pageId Page id.
     * @param restore Get page for restore
     * @return Page.
     * @throws IgniteInternalCheckedException If failed.
     * @see #acquirePage(int, long) Will read page from file if it is not present in memory.
     */
    public long acquirePage(int grpId, long pageId, IoStatisticsHolder statHolder, boolean restore) throws IgniteInternalCheckedException {
        return acquirePage(grpId, pageId, statHolder, restore, null);
    }

    private long acquirePage(
            int grpId,
            long pageId,
            IoStatisticsHolder statHolder,
            boolean restore,
            @Nullable AtomicBoolean pageAllocated
    ) throws IgniteInternalCheckedException {
        assert started;
        assert pageIndex(pageId) != 0 : "Partition meta should should not be read through PageMemory so as not to occupy memory.";

        int partId = partitionId(pageId);

        Segment seg = segment(grpId, pageId);

        seg.readLock().lock();

        try {
            long relPtr = seg.loadedPages.get(
                    grpId,
                    effectivePageId(pageId),
                    seg.partGeneration(grpId, partId),
                    INVALID_REL_PTR,
                    INVALID_REL_PTR
            );

            // The page is loaded to the memory.
            if (relPtr != INVALID_REL_PTR) {
                long absPtr = seg.absolute(relPtr);

                seg.acquirePage(absPtr);

                seg.pageReplacementPolicy.onHit(relPtr);

                statHolder.trackLogicalRead(absPtr + PAGE_OVERHEAD);

                return absPtr;
            }
        } finally {
            seg.readLock().unlock();
        }

        FullPageId fullId = new FullPageId(pageId, grpId);

        seg.writeLock().lock();

        long lockedPageAbsPtr = -1;
        boolean readPageFromStore = false;

        try {
            // Double-check.
            long relPtr = seg.loadedPages.get(
                    grpId,
                    fullId.effectivePageId(),
                    seg.partGeneration(grpId, partId),
                    INVALID_REL_PTR,
                    OUTDATED_REL_PTR
            );

            long absPtr;

            if (relPtr == INVALID_REL_PTR) {
                relPtr = seg.borrowOrAllocateFreePage(pageId);

                if (pageAllocated != null) {
                    pageAllocated.set(true);
                }

                if (relPtr == INVALID_REL_PTR) {
                    relPtr = seg.removePageForReplacement();
                }

                absPtr = seg.absolute(relPtr);

                fullPageId(absPtr, fullId);
                writeTimestamp(absPtr, coarseCurrentTimeMillis());

                assert !isAcquired(absPtr) :
                        "Pin counter must be 0 for a new page [relPtr=" + hexLong(relPtr) + ", absPtr=" + hexLong(absPtr) + ']';

                // We can clear dirty flag after the page has been allocated.
                setDirty(fullId, absPtr, false, false);

                seg.pageReplacementPolicy.onMiss(relPtr);

                seg.loadedPages.put(
                        grpId,
                        fullId.effectivePageId(),
                        relPtr,
                        seg.partGeneration(grpId, partId)
                );

                long pageAddr = absPtr + PAGE_OVERHEAD;

                if (!restore) {
                    delayedPageReplacementTracker.waitUnlock(fullId);

                    readPageFromStore = true;
                } else {
                    zeroMemory(absPtr + PAGE_OVERHEAD, pageSize());

                    // Must init page ID in order to ensure RWLock tag consistency.
                    setPageId(pageAddr, pageId);
                }

                rwLock.init(absPtr + PAGE_LOCK_OFFSET, tag(pageId));

                if (readPageFromStore) {
                    boolean locked = rwLock.writeLock(absPtr + PAGE_LOCK_OFFSET, TAG_LOCK_ALWAYS);

                    assert locked : "Page ID " + fullId + " expected to be locked";

                    lockedPageAbsPtr = absPtr;
                }
            } else if (relPtr == OUTDATED_REL_PTR) {
                assert pageIndex(pageId) == 0 : fullId;

                relPtr = seg.refreshOutdatedPage(grpId, pageId, false);

                absPtr = seg.absolute(relPtr);

                long pageAddr = absPtr + PAGE_OVERHEAD;

                zeroMemory(pageAddr, pageSize());

                fullPageId(absPtr, fullId);
                writeTimestamp(absPtr, coarseCurrentTimeMillis());
                setPageId(pageAddr, pageId);

                assert !isAcquired(absPtr) :
                        "Pin counter must be 0 for a new page [relPtr=" + hexLong(relPtr) + ", absPtr=" + hexLong(absPtr) + ']';

                rwLock.init(absPtr + PAGE_LOCK_OFFSET, tag(pageId));

                seg.pageReplacementPolicy.onRemove(relPtr);
                seg.pageReplacementPolicy.onMiss(relPtr);
            } else {
                absPtr = seg.absolute(relPtr);

                seg.pageReplacementPolicy.onHit(relPtr);
            }

            seg.acquirePage(absPtr);

            if (!readPageFromStore) {
                statHolder.trackLogicalRead(absPtr + PAGE_OVERHEAD);
            }

            return absPtr;
        } finally {
            seg.writeLock().unlock();

            delayedPageReplacementTracker.delayedPageWrite().finishReplacement();

            if (readPageFromStore) {
                assert lockedPageAbsPtr != -1 : "Page is expected to have a valid address [pageId=" + fullId
                        + ", lockedPageAbsPtr=" + hexLong(lockedPageAbsPtr) + ']';

                assert isPageWriteLocked(lockedPageAbsPtr) : "Page is expected to be locked: [pageId=" + fullId + "]";

                long pageAddr = lockedPageAbsPtr + PAGE_OVERHEAD;

                ByteBuffer buf = wrapPointer(pageAddr, pageSize());

                long actualPageId = 0;

                try {
                    pageStoreManager.read(grpId, pageId, buf, false);

                    statHolder.trackPhysicalAndLogicalRead(pageAddr);

                    actualPageId = getPageId(buf);
                } finally {
                    rwLock.writeUnlock(lockedPageAbsPtr + PAGE_LOCK_OFFSET, actualPageId == 0 ? TAG_LOCK_ALWAYS : tag(actualPageId));
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public int pageSize() {
        return sysPageSize - PAGE_OVERHEAD;
    }

    /** {@inheritDoc} */
    @Override
    public int systemPageSize() {
        return sysPageSize;
    }

    /** {@inheritDoc} */
    @Override
    public int realPageSize(int grpId) {
        return pageSize();
    }

    /**
     * Returns total pages can be placed in all segments.
     */
    public long totalPages() {
        if (segments == null) {
            return 0;
        }

        long res = 0;

        for (Segment segment : segments) {
            res += segment.pages();
        }

        return res;
    }

    private void copyInBuffer(long absPtr, ByteBuffer tmpBuf) {
        if (tmpBuf.isDirect()) {
            long tmpPtr = bufferAddress(tmpBuf);

            copyMemory(absPtr + PAGE_OVERHEAD, tmpPtr, pageSize());

            assert getCrc(absPtr + PAGE_OVERHEAD) == 0; // TODO IGNITE-16612
            assert getCrc(tmpPtr) == 0; // TODO IGNITE-16612
        } else {
            byte[] arr = tmpBuf.array();

            assert arr.length == pageSize();

            copyMemory(null, absPtr + PAGE_OVERHEAD, arr, BYTE_ARR_OFF, pageSize());
        }
    }

    /**
     * Get current partition generation tag.
     *
     * @param seg Segment.
     * @param fullPageId Full page id.
     * @return Current partition generation tag.
     */
    private int generationTag(Segment seg, FullPageId fullPageId) {
        return seg.partGeneration(
                fullPageId.groupId(),
                partitionId(fullPageId.pageId())
        );
    }

    /**
     * Resolver relative pointer via {@link LoadedPagesMap}.
     *
     * @param seg Segment.
     * @param fullId Full page id.
     * @param reqVer Required version.
     * @return Relative pointer.
     */
    private long resolveRelativePointer(Segment seg, FullPageId fullId, int reqVer) {
        return seg.loadedPages.get(
                fullId.groupId(),
                fullId.effectivePageId(),
                reqVer,
                INVALID_REL_PTR,
                OUTDATED_REL_PTR
        );
    }

    /**
     * Marks partition as invalid / outdated.
     *
     * @param grpId Group ID.
     * @param partId Partition ID.
     * @return New partition generation (growing 1-based partition file version).
     */
    public int invalidate(int grpId, int partId) {
        synchronized (segmentsLock) {
            if (!started) {
                return 0;
            }

            int tag = 0;

            for (Segment segment : segments) {
                segment.writeLock().lock();

                try {
                    int newTag = segment.incrementPartGeneration(grpId, partId);

                    if (tag == 0) {
                        tag = newTag;
                    }

                    assert tag == newTag;
                } finally {
                    segment.writeLock().unlock();
                }
            }

            return tag;
        }
    }

    /**
     * Clears internal metadata of destroyed group.
     *
     * @param grpId Group ID.
     */
    public void onGroupDestroyed(int grpId) {
        for (Segment seg : segments) {
            seg.writeLock().lock();

            try {
                seg.resetGroupPartitionsGeneration(grpId);
            } finally {
                seg.writeLock().unlock();
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public long loadedPages() {
        long total = 0;

        Segment[] segments = this.segments;

        if (segments != null) {
            for (Segment seg : segments) {
                if (seg == null) {
                    break;
                }

                seg.readLock().lock();

                try {
                    if (seg.closed) {
                        continue;
                    }

                    total += seg.loadedPages.size();
                } finally {
                    seg.readLock().unlock();
                }
            }
        }

        return total;
    }

    /**
     * Returns total number of acquired pages.
     */
    public long acquiredPages() {
        if (segments == null) {
            return 0L;
        }

        long total = 0;

        for (Segment seg : segments) {
            seg.readLock().lock();

            try {
                if (seg.closed) {
                    continue;
                }

                total += seg.acquiredPages();
            } finally {
                seg.readLock().unlock();
            }
        }

        return total;
    }

    /**
     * Returns {@code true} if the page is contained in the loaded pages table, {@code false} otherwise.
     *
     * @param fullPageId Full page ID to check.
     */
    public boolean hasLoadedPage(FullPageId fullPageId) {
        int grpId = fullPageId.groupId();
        long pageId = fullPageId.effectivePageId();
        int partId = partitionId(pageId);

        Segment seg = segment(grpId, pageId);

        seg.readLock().lock();

        try {
            long res = seg.loadedPages.get(grpId, pageId, seg.partGeneration(grpId, partId), INVALID_REL_PTR, INVALID_REL_PTR);

            return res != INVALID_REL_PTR;
        } finally {
            seg.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public long readLockForce(int grpId, long pageId, long page) {
        assert started;

        return readLock(page, pageId, true);
    }

    /**
     * Releases read lock.
     *
     * @param absPtr Absolute pointer to unlock.
     */
    void readUnlockPage(long absPtr) {
        rwLock.readUnlock(absPtr + PAGE_LOCK_OFFSET);
    }

    /**
     * Checks if a page has temp copy buffer.
     *
     * @param absPtr Absolute pointer.
     * @return {@code True} if a page has temp buffer.
     */
    public boolean hasTempCopy(long absPtr) {
        return tempBufferPointer(absPtr) != INVALID_REL_PTR;
    }

    /**
     * Tries to acquire a write lock.
     *
     * @param absPtr Absolute pointer.
     * @return Pointer to the page write buffer or {@code 0} if page was not locked.
     */
    private long tryWriteLockPage(long absPtr, FullPageId fullId, boolean checkTag) {
        int tag = checkTag ? tag(fullId.pageId()) : TAG_LOCK_ALWAYS;

        return !rwLock.tryWriteLock(absPtr + PAGE_LOCK_OFFSET, tag) ? 0 : postWriteLockPage(absPtr, fullId);
    }

    /**
     * Acquires a write lock.
     *
     * @param absPtr Absolute pointer.
     * @return Pointer to the page write buffer or {@code 0} if page was not locked.
     */
    private long writeLockPage(long absPtr, FullPageId fullId, boolean checkTag) {
        int tag = checkTag ? tag(fullId.pageId()) : TAG_LOCK_ALWAYS;

        boolean locked = rwLock.writeLock(absPtr + PAGE_LOCK_OFFSET, tag);

        return locked ? postWriteLockPage(absPtr, fullId) : 0;
    }

    private long postWriteLockPage(long absPtr, FullPageId fullId) {
        writeTimestamp(absPtr, coarseCurrentTimeMillis());

        // Create a buffer copy if the page is scheduled for a checkpoint.
        if (isInCheckpoint(fullId) && tempBufferPointer(absPtr) == INVALID_REL_PTR) {
            long tmpRelPtr = checkpointPool.borrowOrAllocateFreePage(tag(fullId.pageId()));

            if (tmpRelPtr == INVALID_REL_PTR) {
                rwLock.writeUnlock(absPtr + PAGE_LOCK_OFFSET, TAG_LOCK_ALWAYS);

                throw new IgniteInternalException(
                        "Failed to allocate temporary buffer for checkpoint (increase checkpointPageBufferSize configuration property): "
                                + storageProfileView.name());
            }

            // Pin the page until checkpoint is not finished.
            PageHeader.acquirePage(absPtr);

            long tmpAbsPtr = checkpointPool.absolute(tmpRelPtr);

            copyMemory(
                    null,
                    absPtr + PAGE_OVERHEAD,
                    null,
                    tmpAbsPtr + PAGE_OVERHEAD,
                    pageSize()
            );

            assert getType(tmpAbsPtr + PAGE_OVERHEAD) != 0 : "Invalid state. Type is 0! pageId = " + hexLong(fullId.pageId());
            assert getVersion(tmpAbsPtr + PAGE_OVERHEAD) != 0 :
                    "Invalid state. Version is 0! pageId = " + hexLong(fullId.pageId());

            dirty(absPtr, false);
            tempBufferPointer(absPtr, tmpRelPtr);
            // info for checkpoint buffer cleaner.
            fullPageId(tmpAbsPtr, fullId);

            assert getCrc(absPtr + PAGE_OVERHEAD) == 0; // TODO GG-11480
            assert getCrc(tmpAbsPtr + PAGE_OVERHEAD) == 0; // TODO GG-11480
        }

        assert getCrc(absPtr + PAGE_OVERHEAD) == 0; // TODO IGNITE-16612

        return absPtr + PAGE_OVERHEAD;
    }

    private void writeUnlockPage(
            long page,
            FullPageId fullId,
            boolean markDirty,
            boolean restore
    ) {
        boolean wasDirty = isDirty(page);

        try {
            // If page is for restore, we shouldn't mark it as changed.
            if (!restore && markDirty && !wasDirty && changeTracker != null) {
                changeTracker.apply(page, fullId, this);
            }

            assert getCrc(page + PAGE_OVERHEAD) == 0; // TODO IGNITE-16612

            if (markDirty) {
                setDirty(fullId, page, true, false);
            }
        } finally { // Always release the lock.
            long pageId = getPageId(page + PAGE_OVERHEAD);

            try {
                assert pageId != 0 : hexLong(readPageId(page));

                rwLock.writeUnlock(page + PAGE_LOCK_OFFSET, tag(pageId));

                assert getVersion(page + PAGE_OVERHEAD) != 0 : dumpPage(pageId, fullId.groupId());
                assert getType(page + PAGE_OVERHEAD) != 0 : hexLong(pageId);
            } catch (AssertionError ex) {
                LOG.debug("Failed to unlock page [fullPageId={}, binPage={}]", fullId, toHexString(page, systemPageSize()));

                throw ex;
            }
        }
    }

    /**
     * Prepares page details for assertion.
     *
     * @param pageId Page id.
     * @param grpId Group id.
     */
    private String dumpPage(long pageId, int grpId) {
        int pageIdx = pageIndex(pageId);
        int partId = partitionId(pageId);
        long off = (long) (pageIdx + 1) * pageSize();

        return hexLong(pageId) + " (grpId=" + grpId + ", pageIdx=" + pageIdx + ", partId=" + partId + ", offH="
                + Long.toHexString(off) + ")";
    }

    /**
     * Returns {@code true} if write lock acquired for the page.
     *
     * @param absPtr Absolute pointer to the page.
     */
    boolean isPageWriteLocked(long absPtr) {
        return rwLock.isWriteLocked(absPtr + PAGE_LOCK_OFFSET);
    }

    /**
     * Returns {@code true} if read lock acquired for the page.
     *
     * @param absPtr Absolute pointer to the page.
     */
    boolean isPageReadLocked(long absPtr) {
        return rwLock.isReadLocked(absPtr + PAGE_LOCK_OFFSET);
    }

    /**
     * Returns the number of active pages across all segments. Used for test purposes only.
     */
    public int activePagesCount() {
        if (segments == null) {
            return 0;
        }

        int total = 0;

        for (Segment seg : segments) {
            total += seg.acquiredPages();
        }

        return total;
    }

    /**
     * This method must be called in synchronized context.
     *
     * @param pageId full page ID.
     * @param absPtr Absolute pointer.
     * @param dirty {@code True} dirty flag.
     * @param forceAdd If this flag is {@code true}, then the page will be added to the dirty set regardless whether the old flag was dirty
     *      or not.
     */
    private void setDirty(FullPageId pageId, long absPtr, boolean dirty, boolean forceAdd) {
        boolean wasDirty = dirty(absPtr, dirty);

        if (dirty) {
            assert checkpointTimeoutLock.checkpointLockIsHeldByThread();
            assert pageIndex(pageId.pageId()) != 0 : "Partition meta should only be updated via the instance of PartitionMeta.";

            if (!wasDirty || forceAdd) {
                Segment seg = segment(pageId.groupId(), pageId.pageId());

                if (seg.dirtyPages.add(pageId)) {
                    long dirtyPagesCnt = seg.dirtyPagesCntr.incrementAndGet();

                    if (dirtyPagesCnt >= seg.dirtyPagesSoftThreshold) {
                        CheckpointUrgency urgency = checkpointUrgency.get();

                        if (urgency != MUST_TRIGGER) {
                            if (dirtyPagesCnt >= seg.dirtyPagesHardThreshold) {
                                checkpointUrgency.set(MUST_TRIGGER);
                            } else if (urgency != SHOULD_TRIGGER) {
                                checkpointUrgency.compareAndSet(NOT_REQUIRED, SHOULD_TRIGGER);
                            }
                        }
                    }
                }
            }
        } else {
            Segment seg = segment(pageId.groupId(), pageId.pageId());

            if (seg.dirtyPages.remove(pageId)) {
                seg.dirtyPagesCntr.decrementAndGet();
            }
        }
    }

    private Segment segment(int grpId, long pageId) {
        int idx = segmentIndex(grpId, pageId, segments.length);

        return segments[idx];
    }

    /**
     * Returns segment index.
     *
     * @param grpId Group ID.
     * @param pageId Page ID.
     * @param segments Number of segments.
     */
    public static int segmentIndex(int grpId, long pageId, int segments) {
        pageId = effectivePageId(pageId);

        // Take a prime number larger than total number of partitions.
        int hash = hash(pageId * 65537 + grpId);

        return safeAbs(hash) % segments;
    }

    /**
     * Returns a collection of all pages currently marked as dirty. Will create a collection copy.
     */
    @TestOnly
    public Set<FullPageId> dirtyPages() {
        if (segments == null) {
            return Set.of();
        }

        Set<FullPageId> res = new HashSet<>((int) loadedPages());

        for (Segment seg : segments) {
            res.addAll(seg.dirtyPages);
        }

        return res;
    }

    /**
     * Page segment.
     */
    public class Segment extends ReentrantReadWriteLock {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Pointer to acquired pages integer counter. */
        private static final int ACQUIRED_PAGES_SIZEOF = 4;

        /** Padding to read from word beginning. */
        private static final int ACQUIRED_PAGES_PADDING = 4;

        /** Page ID to relative pointer map. */
        private final LoadedPagesMap loadedPages;

        /** Pointer to acquired pages integer counter. */
        private final long acquiredPagesPtr;

        /** Page pool. */
        private final PagePool pool;

        /** Page replacement policy. */
        private final PageReplacementPolicy pageReplacementPolicy;

        /** Bytes required to store {@link #loadedPages}. */
        private final long memPerTbl;

        /** Bytes required to store {@link #pageReplacementPolicy} service data. */
        private long memPerRepl;

        /** Pages marked as dirty since the last checkpoint. */
        private volatile Set<FullPageId> dirtyPages = ConcurrentHashMap.newKeySet();

        /** Atomic size counter for {@link #dirtyPages}. */
        private final AtomicLong dirtyPagesCntr = new AtomicLong();

        /** Wrapper of pages of current checkpoint. */
        @Nullable
        private volatile CheckpointPages checkpointPages;

        /** Maximum number of dirty pages for {@link CheckpointUrgency#NOT_REQUIRED}. */
        private final long dirtyPagesSoftThreshold;

        /** Maximum number of dirty pages for {@link CheckpointUrgency#SHOULD_TRIGGER}. */
        private final long dirtyPagesHardThreshold;

        /** Initial partition generation. */
        private static final int INIT_PART_GENERATION = 1;

        /** Maps partition (grpId, partId) to its generation. Generation is 1-based incrementing partition counter. */
        private final Map<GroupPartitionId, Integer> partGenerationMap = new HashMap<>();

        /** Segment closed flag. */
        private boolean closed;

        /**
         * Constructor.
         *
         * @param idx Segment index.
         * @param region Memory region.
         */
        private Segment(int idx, DirectMemoryRegion region) {
            long totalMemory = region.size();

            int pages = (int) (totalMemory / sysPageSize);

            acquiredPagesPtr = region.address();

            putIntVolatile(null, acquiredPagesPtr, 0);

            int ldPagesMapOffInRegion = ACQUIRED_PAGES_SIZEOF + ACQUIRED_PAGES_PADDING;

            long ldPagesAddr = region.address() + ldPagesMapOffInRegion;

            memPerTbl = RobinHoodBackwardShiftHashMap.requiredMemory(pages);

            loadedPages = new RobinHoodBackwardShiftHashMap(ldPagesAddr, memPerTbl);

            pages = (int) ((totalMemory - memPerTbl - ldPagesMapOffInRegion) / sysPageSize);

            memPerRepl = pageReplacementPolicyFactory.requiredMemory(pages);

            DirectMemoryRegion poolRegion = region.slice(memPerTbl + memPerRepl + ldPagesMapOffInRegion);

            pool = new PagePool(idx, poolRegion, sysPageSize, rwLock);

            pageReplacementPolicy = pageReplacementPolicyFactory.create(
                    this,
                    region.address() + memPerTbl + ldPagesMapOffInRegion,
                    pool.pages()
            );

            dirtyPagesSoftThreshold = pool.pages() * 3L / 4;
            dirtyPagesHardThreshold = pool.pages() * 9L / 10;
        }

        /**
         * Closes the segment.
         */
        private void close() {
            writeLock().lock();

            try {
                closed = true;
            } finally {
                writeLock().unlock();
            }
        }

        /**
         * Returns max number of pages this segment can allocate.
         */
        private int pages() {
            return pool.pages();
        }

        /**
         * Returns memory allocated for pages table.
         */
        private long tableSize() {
            return memPerTbl;
        }

        /**
         * Returns memory allocated for page replacement service data.
         */
        private long replacementSize() {
            return memPerRepl;
        }

        private void acquirePage(long absPtr) {
            PageHeader.acquirePage(absPtr);

            updateAtomicInt(acquiredPagesPtr, 1);
        }

        private void releasePage(long absPtr) {
            PageHeader.releasePage(absPtr);

            updateAtomicInt(acquiredPagesPtr, -1);
        }

        /**
         * Returns total number of acquired pages.
         */
        private int acquiredPages() {
            return getInt(acquiredPagesPtr);
        }

        /**
         * Allocates a new free page.
         *
         * @param pageId Page ID.
         * @return Page relative pointer.
         */
        private long borrowOrAllocateFreePage(long pageId) {
            return pool.borrowOrAllocateFreePage(tag(pageId));
        }

        /**
         * Clear dirty pages collection and reset counter.
         */
        private void resetDirtyPages() {
            dirtyPages = ConcurrentHashMap.newKeySet();

            dirtyPagesCntr.set(0);
        }

        /**
         * Prepares a page removal for page replacement, if needed.
         *
         * @param fullPageId Candidate page full ID.
         * @param absPtr Absolute pointer of the page to evict.
         * @return {@code True} if it is ok to replace this page, {@code false} if another page should be selected.
         * @throws IgniteInternalCheckedException If failed to write page to the underlying store during eviction.
         */
        public boolean tryToRemovePage(FullPageId fullPageId, long absPtr) throws IgniteInternalCheckedException {
            assert writeLock().isHeldByCurrentThread();

            if (isAcquired(absPtr)) {
                return false;
            }

            if (isDirty(absPtr)) {
                CheckpointPages checkpointPages = this.checkpointPages;
                // Can evict a dirty page only if should be written by a checkpoint.
                // These pages does not have tmp buffer.
                if (checkpointPages != null && checkpointPages.allowToSave(fullPageId)) {
                    WriteDirtyPage writeDirtyPage = delayedPageReplacementTracker.delayedPageWrite();

                    writeDirtyPage.write(PersistentPageMemory.this, fullPageId, wrapPointer(absPtr + PAGE_OVERHEAD, pageSize()));

                    setDirty(fullPageId, absPtr, false, true);

                    checkpointPages.markAsSaved(fullPageId);

                    loadedPages.remove(fullPageId.groupId(), fullPageId.effectivePageId());

                    return true;
                }

                return false;
            } else {
                loadedPages.remove(fullPageId.groupId(), fullPageId.effectivePageId());

                return true;
            }
        }

        /**
         * Refresh outdated value.
         *
         * @param grpId Group ID.
         * @param pageId Page ID.
         * @param rmv {@code True} if page should be removed.
         * @return Relative pointer to refreshed page.
         */
        public long refreshOutdatedPage(int grpId, long pageId, boolean rmv) {
            assert writeLock().isHeldByCurrentThread();

            int tag = partGeneration(grpId, partitionId(pageId));

            long relPtr = loadedPages.refresh(grpId, effectivePageId(pageId), tag);

            long absPtr = absolute(relPtr);

            zeroMemory(absPtr + PAGE_OVERHEAD, pageSize());

            dirty(absPtr, false);

            long tmpBufPtr = tempBufferPointer(absPtr);

            if (tmpBufPtr != INVALID_REL_PTR) {
                zeroMemory(checkpointPool.absolute(tmpBufPtr) + PAGE_OVERHEAD, pageSize());

                tempBufferPointer(absPtr, INVALID_REL_PTR);

                // We pinned the page when allocated the temp buffer, release it now.
                PageHeader.releasePage(absPtr);

                releaseCheckpointBufferPage(tmpBufPtr);
            }

            if (rmv) {
                loadedPages.remove(grpId, effectivePageId(pageId));
            }

            return relPtr;
        }

        /**
         * Removes random oldest page for page replacement from memory to storage.
         *
         * @return Relative address for removed page, now it can be replaced by allocated or reloaded page.
         * @throws IgniteInternalCheckedException If failed to evict page.
         */
        private long removePageForReplacement() throws IgniteInternalCheckedException {
            assert getWriteHoldCount() > 0;

            if (pageReplacementWarned == 0) {
                if (pageReplacementWarnedFieldUpdater.compareAndSet(PersistentPageMemory.this, 0, 1)) {
                    LOG.warn("Page replacements started, pages will be rotated with disk, this will affect "
                            + "storage performance (consider increasing PageMemoryDataRegionConfiguration#setMaxSize for "
                            + "data region) [region={}]", storageProfileView.name());
                }
            }

            if (acquiredPages() >= loadedPages.size()) {
                throw oomException("all pages are acquired");
            }

            return pageReplacementPolicy.replace();
        }

        /**
         * Creates out of memory exception with additional information.
         *
         * @param reason Reason.
         */
        public IgniteOutOfMemoryException oomException(String reason) {
            return new IgniteOutOfMemoryException("Failed to find a page for eviction (" + reason + ") ["
                    + "segmentCapacity=" + loadedPages.capacity()
                    + ", loaded=" + loadedPages.size()
                    + ", dirtyPagesSoftThreshold=" + dirtyPagesSoftThreshold
                    + ", dirtyPagesHardThreshold=" + dirtyPagesHardThreshold
                    + ", dirtyPages=" + dirtyPagesCntr
                    + ", pinned=" + acquiredPages()
                    + ']' + lineSeparator() + "Out of memory in data region ["
                    + "name=" + storageProfileView.name()
                    + ", size=" + readableSize(storageProfileView.size(), false)
                    + ", persistence=true] Try the following:" + lineSeparator()
                    + "  ^-- Increase off-heap memory size (PersistentPageMemoryProfileConfigurationSchema.size)" + lineSeparator()
            );
        }

        /**
         * Delegate to the corresponding page pool.
         *
         * @param relPtr Relative pointer.
         * @return Absolute pointer.
         */
        public long absolute(long relPtr) {
            return pool.absolute(relPtr);
        }

        /**
         * Delegate to the corresponding page pool.
         *
         * @param pageIdx Page index.
         * @return Relative pointer.
         */
        public long relative(long pageIdx) {
            return pool.relative(pageIdx);
        }

        /**
         * Delegate to the corresponding page pool.
         *
         * @param relPtr Relative pointer.
         * @return Page index in the pool.
         */
        public long pageIndex(long relPtr) {
            return pool.pageIndex(relPtr);
        }

        /**
         * Returns partition generation. Growing, 1-based partition version.
         *
         * @param grpId Group ID.
         * @param partId Partition ID.
         */
        public int partGeneration(int grpId, int partId) {
            assert getReadHoldCount() > 0 || getWriteHoldCount() > 0;

            Integer tag = partGenerationMap.get(new GroupPartitionId(grpId, partId));

            assert tag == null || tag >= 0 : "Negative tag=" + tag;

            return tag == null ? INIT_PART_GENERATION : tag;
        }

        /**
         * Gets loaded pages map.
         */
        public LoadedPagesMap loadedPages() {
            return loadedPages;
        }

        /**
         * Gets page pool.
         */
        public PagePool pool() {
            return pool;
        }

        /**
         * Increments partition generation due to partition invalidation (e.g. partition was rebalanced to other node and evicted).
         *
         * @param grpId Group ID.
         * @param partId Partition ID.
         * @return New partition generation.
         */
        private int incrementPartGeneration(int grpId, int partId) {
            assert getWriteHoldCount() > 0;

            GroupPartitionId grpPart = new GroupPartitionId(grpId, partId);

            Integer gen = partGenerationMap.get(grpPart);

            if (gen == null) {
                gen = INIT_PART_GENERATION;
            }

            if (gen == Integer.MAX_VALUE) {
                LOG.info("Partition tag overflow [grpId={}, partId={}]", grpId, partId);

                partGenerationMap.put(grpPart, 0);

                return 0;
            } else {
                partGenerationMap.put(grpPart, gen + 1);

                return gen + 1;
            }
        }

        private void resetGroupPartitionsGeneration(int grpId) {
            assert getWriteHoldCount() > 0;

            partGenerationMap.keySet().removeIf(grpPart -> grpPart.getGroupId() == grpId);
        }

        /**
         * Returns IO registry.
         */
        public PageIoRegistry ioRegistry() {
            return ioRegistry;
        }

        /**
         * Gets checkpoint pages.
         */
        public CheckpointPages checkpointPages() {
            return checkpointPages;
        }
    }

    private static int updateAtomicInt(long ptr, int delta) {
        while (true) {
            int old = getInt(ptr);

            int updated = old + delta;

            if (compareAndSwapInt(null, ptr, old, updated)) {
                return updated;
            }
        }
    }

    private static long updateAtomicLong(long ptr, long delta) {
        while (true) {
            long old = getLong(ptr);

            long updated = old + delta;

            if (compareAndSwapLong(null, ptr, old, updated)) {
                return updated;
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public PageIoRegistry ioRegistry() {
        return ioRegistry;
    }

    /**
     * Callback invoked to track changes in pages.
     */
    @FunctionalInterface
    public interface PageChangeTracker {
        /**
         * Callback body.
         *
         * @param page – Page pointer.
         * @param fullPageId Full page ID.
         * @param pageMemoryImpl Page memory.
         */
        void apply(long page, FullPageId fullPageId, PersistentPageMemory pageMemoryImpl);
    }

    /**
     * Heuristic method which allows a thread to check if it is safe to start memory structure modifications in regard with checkpointing.
     * May return false-negative result during or after partition eviction.
     *
     * @see CheckpointUrgency
     */
    public CheckpointUrgency checkpointUrgency() {
        return checkpointUrgency.get();
    }

    /**
     * Returns number of pages used in checkpoint buffer.
     */
    public int usedCheckpointBufferPages() {
        PagePool checkpointPool = this.checkpointPool;

        return checkpointPool == null ? 0 : checkpointPool.size();
    }

    /**
     * Returns max number of pages in checkpoint buffer.
     */
    public int maxCheckpointBufferPages() {
        PagePool checkpointPool = this.checkpointPool;

        return checkpointPool == null ? 0 : checkpointPool.pages();
    }

    private void releaseCheckpointBufferPage(long tmpBufPtr) {
        checkpointPool.releaseFreePage(tmpBufPtr);
    }

    /**
     * Returns {@code true} if it was added to the checkpoint list.
     *
     * @param pageId Page ID to check if it was added to the checkpoint list.
     */
    boolean isInCheckpoint(FullPageId pageId) {
        Segment seg = segment(pageId.groupId(), pageId.pageId());

        CheckpointPages pages0 = seg.checkpointPages;

        return pages0 != null && pages0.contains(pageId);
    }

    /**
     * Returns {@code true} if remove successfully.
     *
     * @param fullPageId Page ID to clear.
     */
    boolean clearCheckpoint(FullPageId fullPageId) {
        Segment seg = segment(fullPageId.groupId(), fullPageId.pageId());

        CheckpointPages pages0 = seg.checkpointPages;

        assert pages0 != null;

        return pages0.markAsSaved(fullPageId);
    }

    /**
     * Makes a full copy of the dirty page for checkpointing, then marks the page as not dirty.
     *
     * @param absPtr Absolute page pointer.
     * @param fullId Full page id.
     * @param buf Buffer for copy page content for future write via {@link PageStoreWriter}.
     * @param pageSingleAcquire Page is acquired only once. We don't pin the page second time (until page will not be copied) in case
     *      checkpoint temporary buffer is used.
     * @param pageStoreWriter Checkpoint page writer.
     * @param tracker Checkpoint metrics tracker.
     */
    private void copyPageForCheckpoint(
            long absPtr,
            FullPageId fullId,
            ByteBuffer buf,
            int tag,
            boolean pageSingleAcquire,
            PageStoreWriter pageStoreWriter,
            CheckpointMetricsTracker tracker
    ) throws IgniteInternalCheckedException {
        assert absPtr != 0;
        assert isAcquired(absPtr) || !isInCheckpoint(fullId);

        // Exception protection flag.
        // No need to write if exception occurred.
        boolean canWrite = false;

        boolean locked = rwLock.tryWriteLock(absPtr + PAGE_LOCK_OFFSET, TAG_LOCK_ALWAYS);

        if (!locked) {
            // We release the page only once here because this page will be copied sometime later and
            // will be released properly then.
            if (!pageSingleAcquire) {
                PageHeader.releasePage(absPtr);
            }

            buf.clear();

            if (isInCheckpoint(fullId)) {
                pageStoreWriter.writePage(fullId, buf, TRY_AGAIN_TAG);
            }

            return;
        }

        if (!clearCheckpoint(fullId)) {
            rwLock.writeUnlock(absPtr + PAGE_LOCK_OFFSET, TAG_LOCK_ALWAYS);

            if (!pageSingleAcquire) {
                PageHeader.releasePage(absPtr);
            }

            return;
        }

        try {
            long tmpRelPtr = tempBufferPointer(absPtr);

            if (tmpRelPtr != INVALID_REL_PTR) {
                tempBufferPointer(absPtr, INVALID_REL_PTR);

                long tmpAbsPtr = checkpointPool.absolute(tmpRelPtr);

                copyInBuffer(tmpAbsPtr, buf);

                fullPageId(tmpAbsPtr, NULL_PAGE);

                zeroMemory(tmpAbsPtr + PAGE_OVERHEAD, pageSize());

                tracker.onCopyOnWritePageWritten();

                releaseCheckpointBufferPage(tmpRelPtr);

                // Need release again because we pin page when resolve abs pointer,
                // and page did not have tmp buffer page.
                if (!pageSingleAcquire) {
                    PageHeader.releasePage(absPtr);
                }
            } else {
                copyInBuffer(absPtr, buf);

                dirty(absPtr, false);
            }

            assert getType(buf) != 0 : "Invalid state. Type is 0! pageId = " + hexLong(fullId.pageId());
            assert getVersion(buf) != 0 : "Invalid state. Version is 0! pageId = " + hexLong(fullId.pageId());

            canWrite = true;
        } finally {
            rwLock.writeUnlock(absPtr + PAGE_LOCK_OFFSET, TAG_LOCK_ALWAYS);

            if (canWrite) {
                buf.rewind();

                pageStoreWriter.writePage(fullId, buf, tag);

                buf.rewind();
            }

            // We pinned the page either when allocated the temp buffer, or when resolved abs pointer.
            // Must release the page only after write unlock.
            PageHeader.releasePage(absPtr);
        }
    }

    /**
     * Prepare page for write during checkpoint. {@link PageStoreWriter} will be called when the page will be ready to write.
     *
     * @param fullId Page ID to get byte buffer for. The page ID must be present in the collection returned by the {@link
     * #beginCheckpoint(CompletableFuture)} method call.
     * @param buf Temporary buffer to write changes into.
     * @param pageStoreWriter Checkpoint page write context.
     * @param tracker Checkpoint metrics tracker.
     * @throws IgniteInternalCheckedException If failed to obtain page data.
     */
    public void checkpointWritePage(
            FullPageId fullId,
            ByteBuffer buf,
            PageStoreWriter pageStoreWriter,
            CheckpointMetricsTracker tracker
    ) throws IgniteInternalCheckedException {
        assert buf.remaining() == pageSize() : buf.remaining();

        Segment seg = segment(fullId.groupId(), fullId.pageId());

        long absPtr = 0;

        long relPtr;

        int tag;

        boolean pageSingleAcquire = false;

        seg.readLock().lock();

        try {
            if (!isInCheckpoint(fullId)) {
                return;
            }

            relPtr = resolveRelativePointer(seg, fullId, tag = generationTag(seg, fullId));

            // Page may have been cleared during eviction. We have nothing to do in this case.
            if (relPtr == INVALID_REL_PTR) {
                return;
            }

            if (relPtr != OUTDATED_REL_PTR) {
                absPtr = seg.absolute(relPtr);

                // Pin the page until page will not be copied. This helpful to prevent page replacement of this page.
                if (tempBufferPointer(absPtr) == INVALID_REL_PTR) {
                    PageHeader.acquirePage(absPtr);
                } else {
                    pageSingleAcquire = true;
                }
            }
        } finally {
            seg.readLock().unlock();
        }

        if (relPtr == OUTDATED_REL_PTR) {
            seg.writeLock().lock();

            try {
                // Double-check.
                relPtr = resolveRelativePointer(seg, fullId, generationTag(seg, fullId));

                if (relPtr == INVALID_REL_PTR) {
                    return;
                }

                if (relPtr == OUTDATED_REL_PTR) {
                    relPtr = seg.refreshOutdatedPage(
                            fullId.groupId(),
                            fullId.effectivePageId(),
                            true
                    );

                    seg.pageReplacementPolicy.onRemove(relPtr);

                    seg.pool.releaseFreePage(relPtr);
                }

                return;
            } finally {
                seg.writeLock().unlock();
            }
        }

        copyPageForCheckpoint(absPtr, fullId, buf, tag, pageSingleAcquire, pageStoreWriter, tracker);
    }

    /**
     * Get arbitrary page from cp buffer.
     */
    public FullPageId pullPageFromCpBuffer() {
        long idx = getLong(checkpointPool.lastAllocatedIdxPtr);

        long lastIdx = ThreadLocalRandom.current().nextLong(idx / 2, idx);

        while (--lastIdx > 1) {
            assert (lastIdx & SEGMENT_INDEX_MASK) == 0L;

            long relative = checkpointPool.relative(lastIdx);

            long freePageAbsPtr = checkpointPool.absolute(relative);

            FullPageId fullPageId = fullPageId(freePageAbsPtr);

            if (fullPageId.pageId() == NULL_PAGE.pageId() || fullPageId.groupId() == NULL_PAGE.groupId()) {
                continue;
            }

            if (!isInCheckpoint(fullPageId)) {
                continue;
            }

            return fullPageId;
        }

        return NULL_PAGE;
    }

    /**
     * Gets a collection of dirty page IDs since the last checkpoint. If a dirty page is being written after the checkpointing operation
     * begun, the modifications will be written to a temporary buffer which will be flushed to the main memory after the checkpointing
     * finished. This method must be called when no concurrent operations on pages are performed.
     *
     * @param allowToReplace The sign which allows replacing pages from a checkpoint by page replacer.
     * @return Collection view of dirty page IDs.
     * @throws IgniteInternalException If checkpoint has been already started and was not finished.
     */
    public Collection<FullPageId> beginCheckpoint(CompletableFuture<?> allowToReplace) throws IgniteInternalException {
        if (segments == null) {
            return List.of();
        }

        Set<FullPageId>[] dirtyPageIds = new Set[segments.length];

        for (int i = 0; i < segments.length; i++) {
            Segment segment = segments[i];

            assert segment.checkpointPages == null : "Failed to begin checkpoint (it is already in progress)";

            Set<FullPageId> segmentDirtyPages = (dirtyPageIds[i] = segment.dirtyPages);

            segment.checkpointPages = new CheckpointPages(segmentDirtyPages, allowToReplace);

            segment.resetDirtyPages();
        }

        checkpointUrgency.set(NOT_REQUIRED);

        return CollectionUtils.concat(dirtyPageIds);
    }

    /**
     * Finishes checkpoint operation.
     */
    public void finishCheckpoint() {
        if (segments == null) {
            return;
        }

        synchronized (segmentsLock) {
            for (Segment seg : segments) {
                seg.checkpointPages = null;
            }
        }
    }
}
