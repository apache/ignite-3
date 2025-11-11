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
import static org.apache.ignite.internal.pagememory.io.PageIo.getCrc;
import static org.apache.ignite.internal.pagememory.io.PageIo.getPageId;
import static org.apache.ignite.internal.pagememory.io.PageIo.getType;
import static org.apache.ignite.internal.pagememory.io.PageIo.getVersion;
import static org.apache.ignite.internal.pagememory.io.PageIo.setPageId;
import static org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency.MUST_TRIGGER;
import static org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency.NOT_REQUIRED;
import static org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency.SHOULD_TRIGGER;
import static org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId.NULL_PAGE;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.PAGE_LOCK_OFFSET;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.PAGE_OVERHEAD;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.UNKNOWN_PARTITION_GENERATION;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.dirty;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.dirtyFullPageId;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.fullPageId;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.isAcquired;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.partitionGeneration;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.pinCount;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.tempBufferPointer;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.timestamp;
import static org.apache.ignite.internal.pagememory.persistence.PagePool.SEGMENT_INDEX_MASK;
import static org.apache.ignite.internal.pagememory.persistence.throttling.PagesWriteThrottlePolicy.CP_BUF_FILL_THRESHOLD;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.effectivePageId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.tag;
import static org.apache.ignite.internal.util.ArrayUtils.concat;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;
import static org.apache.ignite.internal.util.GridUnsafe.copyMemory;
import static org.apache.ignite.internal.util.GridUnsafe.decrementAndGetInt;
import static org.apache.ignite.internal.util.GridUnsafe.getInt;
import static org.apache.ignite.internal.util.GridUnsafe.getLong;
import static org.apache.ignite.internal.util.GridUnsafe.incrementAndGetInt;
import static org.apache.ignite.internal.util.GridUnsafe.putIntVolatile;
import static org.apache.ignite.internal.util.GridUnsafe.wrapPointer;
import static org.apache.ignite.internal.util.GridUnsafe.zeroMemory;
import static org.apache.ignite.internal.util.IgniteUtils.hash;
import static org.apache.ignite.internal.util.IgniteUtils.readableSize;
import static org.apache.ignite.internal.util.IgniteUtils.safeAbs;
import static org.apache.ignite.internal.util.OffheapReadWriteLock.TAG_LOCK_ALWAYS;
import static org.apache.ignite.internal.util.StringUtils.hexLong;
import static org.apache.ignite.internal.util.StringUtils.toHexString;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
import org.apache.ignite.internal.pagememory.configuration.PersistentDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.configuration.ReplacementMode;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.mem.DirectMemoryProvider;
import org.apache.ignite.internal.pagememory.mem.DirectMemoryRegion;
import org.apache.ignite.internal.pagememory.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.pagememory.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointMetricsTracker;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointPages;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTimeoutLock;
import org.apache.ignite.internal.pagememory.persistence.replacement.ClockPageReplacementPolicyFactory;
import org.apache.ignite.internal.pagememory.persistence.replacement.DelayedDirtyPageWrite;
import org.apache.ignite.internal.pagememory.persistence.replacement.DelayedPageReplacementTracker;
import org.apache.ignite.internal.pagememory.persistence.replacement.PageReplacementPolicy;
import org.apache.ignite.internal.pagememory.persistence.replacement.PageReplacementPolicyFactory;
import org.apache.ignite.internal.pagememory.persistence.replacement.RandomLruPageReplacementPolicyFactory;
import org.apache.ignite.internal.pagememory.persistence.replacement.SegmentedLruPageReplacementPolicyFactory;
import org.apache.ignite.internal.pagememory.persistence.throttling.PagesWriteThrottlePolicy;
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
 * +--------------+----------+
 * |PAGE_OVERHEAD |PAGE_SIZE |
 * +--------------+----------+
 * |Page header   |Page data |
 * +--------------+----------+
 * </pre>
 *
 * <p>For the structure of the page header, see {@link PageHeader}.</p>
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

    /** Try again tag. */
    public static final int TRY_AGAIN_TAG = -1;

    /** Data region configuration. */
    private final PersistentDataRegionConfiguration dataRegionConfiguration;

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
    private volatile Segment @Nullable [] segments;

    /** Lock for segments changes. */
    private final Object segmentsLock = new Object();

    /** Offheap read write lock instance. */
    private final OffheapReadWriteLock rwLock;

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
    private volatile @Nullable PagePool checkpointPool;

    /** Pages write throttle. */
    private volatile @Nullable PagesWriteThrottlePolicy writeThrottle;

    /**
     * Delayed page replacement (rotation with disk) tracker. Because other thread may require exactly the same page to be loaded from
     * store, reads are protected by locking.
     */
    private final DelayedPageReplacementTracker delayedPageReplacementTracker;

    /** Checkpoint timeout lock. */
    private final CheckpointTimeoutLock checkpointTimeoutLock;

    private final PersistentPageMemoryMetrics metrics;

    /**
     * Constructor.
     *
     * @param dataRegionConfiguration Data region configuration.
     * @param metricSource Metric source.
     * @param ioRegistry IO registry.
     * @param segmentSizes Segments sizes in bytes.
     * @param checkpointBufferSize Checkpoint buffer size in bytes.
     * @param pageStoreManager Page store manager.
     * @param flushDirtyPageForReplacement Write callback invoked when a dirty page is removed for replacement.
     * @param checkpointTimeoutLock Checkpoint timeout lock.
     * @param rwLock Read-write lock for pages.
     * @param partitionDestructionLockManager Partition Destruction Lock Manager.
     */
    public PersistentPageMemory(
            PersistentDataRegionConfiguration dataRegionConfiguration,
            PersistentPageMemoryMetricSource metricSource,
            PageIoRegistry ioRegistry,
            long[] segmentSizes,
            long checkpointBufferSize,
            PageReadWriteManager pageStoreManager,
            WriteDirtyPage flushDirtyPageForReplacement,
            CheckpointTimeoutLock checkpointTimeoutLock,
            OffheapReadWriteLock rwLock,
            PartitionDestructionLockManager partitionDestructionLockManager
    ) {
        this.dataRegionConfiguration = dataRegionConfiguration;

        this.ioRegistry = ioRegistry;
        this.sizes = concat(segmentSizes, checkpointBufferSize);
        this.pageStoreManager = pageStoreManager;
        this.checkpointTimeoutLock = checkpointTimeoutLock;

        directMemoryProvider = new UnsafeMemoryProvider(null);

        int pageSize = dataRegionConfiguration.pageSize();
        sysPageSize = pageSize + PAGE_OVERHEAD;

        this.rwLock = rwLock;

        ReplacementMode replacementMode = this.dataRegionConfiguration.replacementMode();

        switch (replacementMode) {
            case RANDOM_LRU:
                pageReplacementPolicyFactory = new RandomLruPageReplacementPolicyFactory();

                break;
            case SEGMENTED_LRU:
                pageReplacementPolicyFactory = new SegmentedLruPageReplacementPolicyFactory();

                break;
            case CLOCK:
                pageReplacementPolicyFactory = new ClockPageReplacementPolicyFactory();

                break;
            default:
                throw new IgniteInternalException("Unexpected page replacement mode: " + replacementMode);
        }

        metrics = new PersistentPageMemoryMetrics(metricSource, this, dataRegionConfiguration);

        delayedPageReplacementTracker = new DelayedPageReplacementTracker(
                pageSize,
                (pageMemory, fullPageId, buffer) -> {
                    metrics.incrementWriteToDiskMetric();

                    flushDirtyPageForReplacement.write(pageMemory, fullPageId, buffer);
                },
                LOG,
                sizes.length - 1,
                partitionDestructionLockManager
        );

        this.writeThrottle = null;
    }

    /**
     * Temporary method to enable throttling in tests.
     *
     * @param writeThrottle Page write throttling instance.
     */
    // TODO IGNITE-24933 Remove this method.
    public void initThrottling(PagesWriteThrottlePolicy writeThrottle) {
        this.writeThrottle = writeThrottle;
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
                LOG.info(
                        "Started page memory [name='{}', memoryAllocated={}, pages={}, tableSize={}, replacementSize={},"
                                + " checkpointBuffer={}]",
                        dataRegionConfiguration.name(),
                        readableSize(totalAllocated, false),
                        pages,
                        readableSize(totalTblSize, false),
                        readableSize(totalReplSize, false),
                        readableSize(checkpointBufferSize, false)
                );
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

            Segment[] segments = this.segments;
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
            timestamp(absPtr, coarseCurrentTimeMillis());
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

    @Override
    public long allocatePageNoReuse(int grpId, int partId, byte flags) throws IgniteInternalCheckedException {
        assert partId >= 0 && partId <= MAX_PARTITION_ID : "grpId=" + grpId + ", partId=" + partId;

        assert started : "grpId=" + grpId + ", partId=" + partId;
        assert checkpointTimeoutLock.checkpointLockIsHeldByThread() : "grpId=" + grpId + ", partId=" + partId;

        PagesWriteThrottlePolicy writeThrottle = this.writeThrottle;
        if (writeThrottle != null) {
            writeThrottle.onMarkDirty(false);
        }

        long pageId = pageStoreManager.allocatePage(grpId, partId, flags);

        // We need to allocate page in memory for marking it dirty to save it in the next checkpoint.
        // Otherwise, it is possible that on file will be empty page which will be saved at snapshot and read with error
        // because there is no crc inside them.
        Segment seg = segment(grpId, pageId);

        seg.writeLock().lock();

        try {
            FullPageId fullId = new FullPageId(pageId, grpId);

            int partGen = partGeneration(seg, fullId);

            long relPtr = seg.loadedPages.get(
                    grpId,
                    effectivePageId(pageId),
                    partGen,
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
            timestamp(absPtr, coarseCurrentTimeMillis());
            partitionGeneration(absPtr, partGen);

            rwLock.init(absPtr + PAGE_LOCK_OFFSET, tag(pageId));

            assert getCrc(absPtr + PAGE_OVERHEAD) == 0 : fullId; // TODO IGNITE-16612

            assert !isAcquired(absPtr) : String.format(
                    "Pin counter must be 0 for a new page [relPtr=%s, absPtr=%s, pinCntr=%s, fullId=%s]",
                    hexLong(relPtr), hexLong(absPtr), pinCount(absPtr), fullId
            );

            setDirty(fullId, absPtr, true, true);

            seg.pageReplacementPolicy.onMiss(relPtr);

            seg.loadedPages.put(grpId, effectivePageId(pageId), relPtr, partGen);
        } catch (IgniteOutOfMemoryException oom) {
            IgniteOutOfMemoryException e = new IgniteOutOfMemoryException("Out of memory in data region ["
                    + "name=" + dataRegionConfiguration.name()
                    + ", size=" + readableSize(dataRegionConfiguration.sizeBytes(), false)
                    + ", persistence=true] Try the following:" + lineSeparator()
                    + "  ^-- Increase maximum off-heap memory size"
                    + lineSeparator()
                    + "  ^-- Enable eviction or expiration policies"
            );

            e.initCause(oom);

            throw e;
        } finally {
            seg.writeLock().unlock();

            delayedPageReplacementTracker.delayedPageWrite().flushCopiedPageIfExists();
        }

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
        return acquirePage(grpId, pageId, false);
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
        return acquirePage(grpId, pageId, false, pageAllocated);
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
    public long acquirePage(int grpId, long pageId, boolean restore) throws IgniteInternalCheckedException {
        return acquirePage(grpId, pageId, restore, null);
    }

    private long acquirePage(
            int grpId,
            long pageId,
            boolean restore,
            @Nullable AtomicBoolean pageAllocated
    ) throws IgniteInternalCheckedException {
        assert started : "grpId=" + grpId + ", pageId=" + hexLong(pageId);
        assert pageIndex(pageId) != 0 : String.format(
                "Partition meta should should not be read through PageMemory so as not to occupy memory: [grpId=%s, pageId=%s]",
                grpId, hexLong(pageId)
        );

        FullPageId fullId = new FullPageId(pageId, grpId);

        Segment seg = segment(grpId, pageId);

        seg.readLock().lock();

        try {
            long relPtr = seg.loadedPages.get(
                    grpId,
                    effectivePageId(pageId),
                    partGeneration(seg, fullId),
                    INVALID_REL_PTR,
                    INVALID_REL_PTR
            );

            // The page is loaded to the memory.
            if (relPtr != INVALID_REL_PTR) {
                long absPtr = seg.absolute(relPtr);

                seg.acquirePage(absPtr);

                seg.pageReplacementPolicy.onHit(relPtr);

                return absPtr;
            }
        } finally {
            seg.readLock().unlock();
        }

        seg.writeLock().lock();

        long lockedPageAbsPtr = -1;
        boolean readPageFromStore = false;

        try {
            int partGen = partGeneration(seg, fullId);

            // Double-check.
            long relPtr = seg.loadedPages.get(
                    grpId,
                    fullId.effectivePageId(),
                    partGen,
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
                timestamp(absPtr, coarseCurrentTimeMillis());
                partitionGeneration(absPtr, partGen);

                assert !isAcquired(absPtr) : String.format(
                        "Pin counter must be 0 for a new page [relPtr=%s, absPtr=%s, pinCntr=%s, fullId=%s]",
                        hexLong(relPtr), hexLong(absPtr), pinCount(absPtr), fullId
                );

                // We can clear dirty flag after the page has been allocated.
                setDirty(fullId, absPtr, false, false);

                seg.pageReplacementPolicy.onMiss(relPtr);

                seg.loadedPages.put(
                        grpId,
                        fullId.effectivePageId(),
                        relPtr,
                        partGen
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
                // assert pageIndex(pageId) == 0 : fullId;

                relPtr = seg.refreshOutdatedPage(grpId, pageId, false);

                absPtr = seg.absolute(relPtr);

                long pageAddr = absPtr + PAGE_OVERHEAD;

                zeroMemory(pageAddr, pageSize());

                fullPageId(absPtr, fullId);
                timestamp(absPtr, coarseCurrentTimeMillis());
                partitionGeneration(absPtr, partGen);
                setPageId(pageAddr, pageId);

                assert !isAcquired(absPtr) : String.format(
                        "Pin counter must be 0 for a new page [relPtr=%s, absPtr=%s, pinCntr=%s, fullId=%s]",
                        hexLong(relPtr), hexLong(absPtr), pinCount(absPtr), fullId
                );

                rwLock.init(absPtr + PAGE_LOCK_OFFSET, tag(pageId));

                seg.pageReplacementPolicy.onRemove(relPtr);
                seg.pageReplacementPolicy.onMiss(relPtr);
            } else {
                absPtr = seg.absolute(relPtr);

                seg.pageReplacementPolicy.onHit(relPtr);
            }

            seg.acquirePage(absPtr);

            return absPtr;
        } finally {
            seg.writeLock().unlock();

            delayedPageReplacementTracker.delayedPageWrite().flushCopiedPageIfExists();

            if (readPageFromStore) {
                assert lockedPageAbsPtr != -1 : "Page is expected to have a valid address [pageId=" + fullId
                        + ", lockedPageAbsPtr=" + hexLong(lockedPageAbsPtr) + ']';

                assert isPageWriteLocked(lockedPageAbsPtr) : "Page is expected to be locked: [pageId=" + fullId + "]";

                long pageAddr = lockedPageAbsPtr + PAGE_OVERHEAD;

                ByteBuffer buf = wrapPointer(pageAddr, pageSize());

                long actualPageId = 0;

                try {
                    pageStoreManager.read(grpId, pageId, buf, false);

                    actualPageId = getPageId(buf);

                    metrics.incrementReadFromDiskMetric();
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
        Segment[] segments = this.segments;
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
     * Get current partition generation.
     *
     * @param seg Segment.
     * @param fullPageId Full page ID.
     */
    private int partGeneration(Segment seg, FullPageId fullPageId) {
        return seg.partGeneration(fullPageId.groupId(), fullPageId.partitionId());
    }

    /**
     * Get current partition generation.
     *
     * @param grpId Group ID.
     * @param partId Partition ID.
     */
    public int partGeneration(int grpId, int partId) {
        Segment seg = segment(grpId, partId);

        seg.readLock().lock();

        try {
            return seg.partGeneration(grpId, partId);
        } finally {
            seg.readLock().unlock();
        }
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
     * <p>Used when destroying a partition, so that when reading or writing pages, they are considered outdated. And, for example, they
     * were not written to disk, but were freed in memory at a checkpoint.</p>
     *
     * @param grpId Group ID.
     * @param partId Partition ID.
     * @return New partition generation (growing 1-based partition file version), {@code 0} if the {@link PersistentPageMemory} did not
     *      start or stopped.
     */
    public int invalidate(int grpId, int partId) {
        synchronized (segmentsLock) {
            if (!started) {
                return 0;
            }

            int resultPartitionGeneration = 0;

            for (Segment segment : segments) {
                segment.writeLock().lock();

                try {
                    int newPartitionGeneration = segment.incrementPartGeneration(grpId, partId);

                    if (resultPartitionGeneration == 0) {
                        resultPartitionGeneration = newPartitionGeneration;
                    }

                    assert resultPartitionGeneration == newPartitionGeneration : String.format(
                            "grpId=%s, partId=%s, resultPartitionGeneration=%s, newPartitionGeneration=%s",
                            grpId, partId, resultPartitionGeneration, newPartitionGeneration
                    );
                } finally {
                    segment.writeLock().unlock();
                }
            }

            return resultPartitionGeneration;
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
        Segment[] segments = this.segments;
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

        Segment seg = segment(grpId, pageId);

        seg.readLock().lock();

        try {
            long res = seg.loadedPages.get(grpId, pageId, partGeneration(seg, fullPageId), INVALID_REL_PTR, INVALID_REL_PTR);

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
        timestamp(absPtr, coarseCurrentTimeMillis());

        DirtyFullPageId dirtyFullId = dirtyFullPageId(absPtr);

        // Create a buffer copy if the page is scheduled for a checkpoint.
        if (isInCheckpoint(dirtyFullId) && tempBufferPointer(absPtr) == INVALID_REL_PTR) {
            long tmpRelPtr;

            PagePool checkpointPool = this.checkpointPool;

            while (true) {
                tmpRelPtr = checkpointPool.borrowOrAllocateFreePage(tag(fullId.pageId()));

                if (tmpRelPtr != INVALID_REL_PTR) {
                    break;
                }

                // TODO https://issues.apache.org/jira/browse/IGNITE-23106 Replace spin-wait with a proper wait.
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignore) {
                    // No-op.
                }
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
            dirtyFullPageId(tmpAbsPtr, dirtyFullId);

            assert getCrc(absPtr + PAGE_OVERHEAD) == 0; // TODO IGNITE-16612
            assert getCrc(tmpAbsPtr + PAGE_OVERHEAD) == 0; // TODO IGNITE-16612
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
            assert getCrc(page + PAGE_OVERHEAD) == 0; // TODO IGNITE-16612

            if (markDirty) {
                setDirty(fullId, page, true, false);
            }
        } finally { // Always release the lock.
            long pageId = getPageId(page + PAGE_OVERHEAD);

            try {
                assert pageId != 0 : hexLong(PageHeader.pageId(page));

                rwLock.writeUnlock(page + PAGE_LOCK_OFFSET, tag(pageId));

                assert getVersion(page + PAGE_OVERHEAD) != 0 : dumpPage(pageId, fullId.groupId());
                assert getType(page + PAGE_OVERHEAD) != 0 : hexLong(pageId);

                PagesWriteThrottlePolicy writeThrottle = this.writeThrottle;
                if (writeThrottle != null && !restore && !wasDirty && markDirty) {
                    writeThrottle.onMarkDirty(isInCheckpoint(dirtyFullPageId(page)));
                }
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
        Segment[] segments = this.segments;
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

        int partGen = partitionGeneration(absPtr);

        assert partGen != UNKNOWN_PARTITION_GENERATION : pageId;

        if (dirty) {
            assert checkpointTimeoutLock.checkpointLockIsHeldByThread() : pageId;
            assert pageIndex(pageId.pageId()) != 0 : "Partition meta should only be updated via the instance of PartitionMeta: " + pageId;

            if (!wasDirty || forceAdd) {
                Segment seg = segment(pageId.groupId(), pageId.pageId());

                if (seg.dirtyPages.add(new DirtyFullPageId(pageId, partGen))) {
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

            if (seg.dirtyPages.remove(new DirtyFullPageId(pageId, partGen))) {
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
    public Set<DirtyFullPageId> dirtyPages() {
        Segment[] segments = this.segments;
        if (segments == null) {
            return Set.of();
        }

        var res = new HashSet<DirtyFullPageId>();

        for (Segment seg : segments) {
            res.addAll(seg.dirtyPages);
        }

        return res;
    }

    /**
     * Returns max dirty pages ratio among all segments.
     */
    public double dirtyPagesRatio() {
        Segment[] segments = this.segments;
        if (segments == null) {
            return 0;
        }

        long res = 0;

        for (Segment segment : segments) {
            res = Math.max(res, segment.dirtyPagesRatio());
        }

        return res * 1.0e-4d;
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
        private volatile Set<DirtyFullPageId> dirtyPages = ConcurrentHashMap.newKeySet();

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

        /**
         * Maps partition (grpId, partId) to its generation. Generation is 1-based incrementing partition counter.
         *
         * <p>Guarded by segment read/write lock.</p>
         */
        private final Object2IntMap<GroupPartitionId> partGenerationMap = new Object2IntOpenHashMap<>();

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

        /** Times a long value of dirty pages ratio multiplied by 10k. Here we avoid double division, while still having some precision. */
        private long dirtyPagesRatio() {
            return dirtyPagesCntr.longValue() * 10_000L / pages();
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

            incrementAndGetInt(acquiredPagesPtr);
        }

        private void releasePage(long absPtr) {
            PageHeader.releasePage(absPtr);

            decrementAndGetInt(acquiredPagesPtr);
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
         * Tries to replace the page.
         *
         * <p>The replacement will be successful if the following conditions are met:</p>
         * <ul>
         *     <li>Page is pinned by another thread, such as a checkpoint dirty page writer or in the process of being modified - nothing
         *     needs to be done.</li>
         *     <li>Page is not dirty - just remove it from the loaded pages.</li>
         *     <li>Page is dirty, there is a checkpoint in the process and the following sub-conditions are met:</li>
         *     <ul>
         *         <li>Page belongs to current checkpoint.</li>
         *         <li>If the dirty page sorting phase is complete, otherwise we wait for it. This is necessary so that we can safely
         *         create partition delta files in which the dirty page order must be preserved.</li>
         *         <li>If the checkpoint dirty page writer has not started writing the page or has already written it.</li>
         *     </ul>
         * </ul>
         *
         * <p>It is expected that if the method returns {@code true}, it will not be invoked again for the same page ID.</p>
         *
         * <p>If we intend to replace a page, it is important for us to block the delta file fsync phase of the checkpoint to preserve data
         * consistency. The phase should not start until all dirty pages are written by the checkpoint writer, but for page replacement we
         * must block it ourselves.</p>
         *
         * @param fullPageId Candidate page ID.
         * @param absPtr Absolute pointer to the candidate page.
         * @return {@code True} if the page replacement was successful, otherwise need to try another one.
         * @throws IgniteInternalCheckedException If any error occurred while waiting for the dirty page sorting phase to complete at a
         *      checkpoint.
         */
        public boolean tryToRemovePage(FullPageId fullPageId, long absPtr) throws IgniteInternalCheckedException {
            assert writeLock().isHeldByCurrentThread() : fullPageId;

            if (isAcquired(absPtr)) {
                // Page is pinned by another thread, such as a checkpoint dirty page writer or in the process of being modified - nothing
                // needs to be done.
                return false;
            }

            if (isDirty(absPtr)) {
                DirtyFullPageId dirtyFullPageId = dirtyFullPageId(absPtr);

                CheckpointPages checkpointPages = this.checkpointPages;
                // Can replace a dirty page only if it should be written by a checkpoint.
                // Safe to invoke because we keep segment write lock and the checkpoint writer must remove pages on the segment read lock.
                if (checkpointPages != null && checkpointPages.removeOnPageReplacement(dirtyFullPageId)) {
                    checkpointPages.blockFsyncOnPageReplacement(dirtyFullPageId);

                    DelayedDirtyPageWrite delayedDirtyPageWrite = delayedPageReplacementTracker.delayedPageWrite();

                    delayedDirtyPageWrite.copyPageToTemporaryBuffer(
                            PersistentPageMemory.this,
                            dirtyFullPageId,
                            wrapPointer(absPtr + PAGE_OVERHEAD, pageSize()),
                            checkpointPages
                    );

                    setDirty(fullPageId, absPtr, false, true);

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
            assert writeLock().isHeldByCurrentThread() : "grpId=" + grpId + ", pageId=" + pageId;

            int partGen = partGeneration(grpId, partitionId(pageId));

            long relPtr = loadedPages.refresh(grpId, effectivePageId(pageId), partGen);

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
                            + "data region) [region={}]", dataRegionConfiguration.name());
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
                    + "name=" + dataRegionConfiguration.name()
                    + ", size=" + readableSize(dataRegionConfiguration.sizeBytes(), false)
                    + ", persistence=true] Try the following:" + lineSeparator()
                    + "  ^-- Increase off-heap memory size" + lineSeparator()
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
            assert getReadHoldCount() > 0 || getWriteHoldCount() > 0 : "grpId=" + grpId + ", partId=" + partId;

            var groupPartitionId = new GroupPartitionId(grpId, partId);

            int partitionGeneration = partGenerationMap.getOrDefault(groupPartitionId, INIT_PART_GENERATION);

            assert partitionGeneration > 0 : "groupPartitionId=" + groupPartitionId + ", partitionGeneration=" + partitionGeneration;

            return partitionGeneration;
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
            assert getWriteHoldCount() > 0 : "grpId=" + grpId + ", partId=" + partId;

            var groupPartitionId = new GroupPartitionId(grpId, partId);

            int partitionGeneration = partGenerationMap.getOrDefault(groupPartitionId, INIT_PART_GENERATION);

            if (partitionGeneration == Integer.MAX_VALUE) {
                LOG.info("Partition generation overflow [grpId={}, partId={}]", grpId, partId);

                partGenerationMap.put(groupPartitionId, INIT_PART_GENERATION);

                return INIT_PART_GENERATION;
            } else {
                partGenerationMap.put(groupPartitionId, partitionGeneration + 1);

                return partitionGeneration + 1;
            }
        }

        private void resetGroupPartitionsGeneration(int grpId) {
            assert getWriteHoldCount() > 0 : "grpId=" + grpId;

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

        /**
         * Checks if segment is sufficiently full.
         *
         * @param dirtyRatioThreshold Max allowed dirty pages ration.
         */
        boolean shouldThrottle(double dirtyRatioThreshold) {
            // Comparison using multiplication is faster than comparison using division.
            return dirtyPagesCntr.doubleValue() > dirtyRatioThreshold * pages();
        }
    }

    /** {@inheritDoc} */
    @Override
    public PageIoRegistry ioRegistry() {
        return ioRegistry;
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
     * Checks if region is sufficiently full.
     *
     * @param dirtyRatioThreshold Max allowed dirty pages ration.
     */
    public boolean shouldThrottle(double dirtyRatioThreshold) {
        Segment[] segments = this.segments;

        if (segments == null) {
            return false;
        }

        for (Segment segment : segments) {
            if (segment.shouldThrottle(dirtyRatioThreshold)) {
                return true;
            }
        }

        return false;
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
        PagePool checkpointPool = this.checkpointPool;
        assert checkpointPool != null;

        int resultCounter = checkpointPool.releaseFreePage(tmpBufPtr);

        PagesWriteThrottlePolicy writeThrottle = this.writeThrottle;
        if (writeThrottle != null && resultCounter == checkpointPool.pages() / 2) {
            writeThrottle.wakeupThrottledThreads();
        }
    }

    /**
     * Returns {@code true} if it was added to the checkpoint list.
     *
     * @param pageId Page ID to check if it was added to the checkpoint list.
     */
    private boolean isInCheckpoint(DirtyFullPageId pageId) {
        Segment seg = segment(pageId.groupId(), pageId.pageId());

        CheckpointPages pages0 = seg.checkpointPages;

        return pages0 != null && pages0.contains(pageId);
    }

    /**
     * Returns {@code true} if remove successfully.
     *
     * @param fullPageId Page ID to remove.
     */
    private boolean removeOnCheckpoint(DirtyFullPageId fullPageId) {
        Segment seg = segment(fullPageId.groupId(), fullPageId.pageId());

        CheckpointPages pages0 = seg.checkpointPages;

        assert pages0 != null : fullPageId;

        return pages0.removeOnCheckpoint(fullPageId);
    }

    /**
     * Makes a full copy of the dirty page for checkpointing, then marks the page as not dirty.
     *
     * @param absPtr Absolute page pointer.
     * @param fullId Full page ID.
     * @param buf Buffer for copy page content for future write via {@link PageStoreWriter}.
     * @param partitionGeneration Current partition generation.
     * @param pageSingleAcquire Page is acquired only once. We don't pin the page second time (until page will not be copied) in case
     *      checkpoint temporary buffer is used.
     * @param pageStoreWriter Checkpoint page writer.
     * @param tracker Checkpoint metrics tracker.
     * @param useTryWriteLockOnPage {@code True} if need to use the <b>try write lock</b> on page, {@code false} for blocking
     *      <b>write lock</b> on page.
     */
    private void copyPageForCheckpoint(
            long absPtr,
            DirtyFullPageId fullId,
            ByteBuffer buf,
            int partitionGeneration,
            boolean pageSingleAcquire,
            PageStoreWriter pageStoreWriter,
            CheckpointMetricsTracker tracker,
            boolean useTryWriteLockOnPage
    ) throws IgniteInternalCheckedException {
        assert absPtr != 0 : fullId.pageId();
        assert isAcquired(absPtr) || !isInCheckpoint(fullId) : fullId.pageId();

        if (useTryWriteLockOnPage) {
            if (!rwLock.tryWriteLock(absPtr + PAGE_LOCK_OFFSET, TAG_LOCK_ALWAYS)) {
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
        } else {
            boolean locked = rwLock.writeLock(absPtr + PAGE_LOCK_OFFSET, TAG_LOCK_ALWAYS);

            assert locked : hexLong(fullId.pageId());
        }

        if (!removeOnCheckpoint(fullId)) {
            rwLock.writeUnlock(absPtr + PAGE_LOCK_OFFSET, TAG_LOCK_ALWAYS);

            if (!pageSingleAcquire) {
                PageHeader.releasePage(absPtr);
            }

            return;
        }

        // Exception protection flag.
        // No need to write if exception occurred.
        boolean canWrite = false;

        try {
            long tmpRelPtr = tempBufferPointer(absPtr);

            if (tmpRelPtr != INVALID_REL_PTR) {
                tempBufferPointer(absPtr, INVALID_REL_PTR);

                long tmpAbsPtr = checkpointPool.absolute(tmpRelPtr);

                copyInBuffer(tmpAbsPtr, buf);

                dirtyFullPageId(tmpAbsPtr, NULL_PAGE);

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

                pageStoreWriter.writePage(fullId, buf, partitionGeneration);

                buf.rewind();

                metrics.incrementWriteToDiskMetric();
            }

            // We pinned the page either when allocated the temp buffer, or when resolved abs pointer.
            // Must release the page only after write unlock.
            PageHeader.releasePage(absPtr);
        }
    }

    /**
     * Tries to copy a page from memory for checkpoint and then pass the contents to {@code pageStoreWriter} if it has not already been
     * written or invalidated (due to partition destruction). {@link PageStoreWriter} will be called when the page will be ready to write.
     *
     * @param fullId Page ID to get byte buffer for. The page ID must be present in the collection returned by the {@link #beginCheckpoint}
     *      method call.
     * @param buf Temporary buffer to write changes into.
     * @param pageStoreWriter Checkpoint page write context.
     * @param tracker Checkpoint metrics tracker.
     * @param useTryWriteLockOnPage {@code True} if need to use the <b>try write lock</b> on page, {@code false} for blocking
     *      <b>write lock</b> on page.
     * @throws IgniteInternalCheckedException If failed to obtain page data.
     */
    public void checkpointWritePage(
            DirtyFullPageId fullId,
            ByteBuffer buf,
            PageStoreWriter pageStoreWriter,
            CheckpointMetricsTracker tracker,
            boolean useTryWriteLockOnPage
    ) throws IgniteInternalCheckedException {
        assert buf.remaining() == pageSize() : "fullId=" + fullId + ", remaining=" + buf.remaining();

        Segment seg = segment(fullId.groupId(), fullId.pageId());

        long absPtr = 0;

        long relPtr;

        int partGen;

        boolean pageSingleAcquire = false;

        seg.readLock().lock();

        try {
            if (!isInCheckpoint(fullId)) {
                return;
            }

            partGen = partGeneration(seg, fullId);

            relPtr = resolveRelativePointer(seg, fullId, partGen);

            // Page may have been cleared during eviction. We have nothing to do in this case.
            if (relPtr == INVALID_REL_PTR) {
                return;
            }

            if (relPtr != OUTDATED_REL_PTR) {
                absPtr = seg.absolute(relPtr);

                if (fullId.partitionGeneration() != partitionGeneration(absPtr)) {
                    return;
                }

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
                relPtr = resolveRelativePointer(seg, fullId, partGeneration(seg, fullId));

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

        copyPageForCheckpoint(
                absPtr,
                fullId,
                buf,
                partGen,
                pageSingleAcquire,
                pageStoreWriter,
                tracker,
                useTryWriteLockOnPage
        );
    }

    /**
     * Get arbitrary page from cp buffer.
     */
    public DirtyFullPageId pullPageFromCpBuffer() {
        long idx = getLong(checkpointPool.lastAllocatedIdxPtr);

        long lastIdx = ThreadLocalRandom.current().nextLong(idx / 2, idx);

        while (--lastIdx > 1) {
            assert (lastIdx & SEGMENT_INDEX_MASK) == 0L;

            long relative = checkpointPool.relative(lastIdx);

            long freePageAbsPtr = checkpointPool.absolute(relative);

            DirtyFullPageId fullPageId = dirtyFullPageId(freePageAbsPtr);

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
     * @param checkpointProgress Progress of the current checkpoint.
     * @return Collection view of dirty page IDs.
     * @throws IgniteInternalException If checkpoint has been already started and was not finished.
     */
    public Collection<DirtyFullPageId> beginCheckpoint(CheckpointProgress checkpointProgress) throws IgniteInternalException {
        if (segments == null) {
            return List.of();
        }

        Set<DirtyFullPageId>[] dirtyPageIds = new Set[segments.length];

        for (int i = 0; i < segments.length; i++) {
            Segment segment = segments[i];

            assert segment.checkpointPages == null : String.format(
                    "Failed to begin checkpoint (it is already in progress): [region=%s, segmentIdx=%s]",
                    dataRegionConfiguration.name(), i
            );

            Set<DirtyFullPageId> segmentDirtyPages = segment.dirtyPages;
            dirtyPageIds[i] = segmentDirtyPages;

            segment.checkpointPages = new CheckpointPages(segmentDirtyPages, checkpointProgress);

            segment.resetDirtyPages();
        }

        checkpointUrgency.set(NOT_REQUIRED);

        PagesWriteThrottlePolicy writeThrottle = this.writeThrottle;
        if (writeThrottle != null) {
            writeThrottle.onBeginCheckpoint();
        }

        return CollectionUtils.concat(dirtyPageIds);
    }

    /**
     * Finishes checkpoint operation.
     */
    public void finishCheckpoint() {
        Segment[] segments = this.segments;
        if (segments == null) {
            return;
        }

        synchronized (segmentsLock) {
            for (Segment seg : segments) {
                seg.checkpointPages = null;
            }
        }

        PagesWriteThrottlePolicy writeThrottle = this.writeThrottle;
        if (writeThrottle != null) {
            writeThrottle.onFinishCheckpoint();
        }
    }

    /**
     * Checks if the Checkpoint Buffer is currently close to exhaustion.
     */
    public boolean isCpBufferOverflowThresholdExceeded() {
        PagesWriteThrottlePolicy writeThrottle = this.writeThrottle;
        if (writeThrottle != null) {
            return writeThrottle.isCpBufferOverflowThresholdExceeded();
        }

        assert started;

        PagePool checkpointPool = this.checkpointPool;

        //noinspection NumericCastThatLosesPrecision
        int checkpointBufLimit = (int) (checkpointPool.pages() * CP_BUF_FILL_THRESHOLD);

        return checkpointPool.size() > checkpointBufLimit;
    }

    /** Returns {@code true} if a page replacement has occurred at least once. */
    @TestOnly
    public boolean pageReplacementOccurred() {
        return pageReplacementWarned > 0;
    }
}
