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

package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagememory.io.PageIo.getType;
import static org.apache.ignite.internal.pagememory.io.PageIo.getVersion;
import static org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId.NULL_PAGE;
import static org.apache.ignite.internal.pagememory.persistence.PartitionMeta.partitionMetaPageId;
import static org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory.TRY_AGAIN_TAG;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.flag;
import static org.apache.ignite.internal.util.StringUtils.hexLong;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PageStoreWriter;
import org.apache.ignite.internal.pagememory.persistence.PageWriteTarget;
import org.apache.ignite.internal.pagememory.persistence.PartitionDestructionLockManager;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta.PartitionMetaSnapshot;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.WriteDirtyPage;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.CheckpointDirtyPagesView;
import org.apache.ignite.internal.pagememory.persistence.io.PartitionMetaIo;
import org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue;
import org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue.Result;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of page writer which able to store pages to disk during checkpoint.
 */
// TODO: IGNITE-23203 Write retry pages in multiple threads
// TODO: IGNITE-26311 Improve dirty page write filtering for deleted partitions
public class CheckpointPagesWriter implements Runnable {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(CheckpointPagesWriter.class);

    /**
     * Maximum number of attempts to write retry dirty pages after which the blocking write lock will be used to write pages. Value is taken
     * speculatively.
     */
    private static final int MAX_ATTEMPT_WRITE_RETRY_DIRTY_PAGES = 32;

    /**
     * Size of a batch of pages that we drain from a single checkpoint buffer at the same time. The value of {@code 10} is chosen
     * arbitrarily. We may reconsider it in <a href="https://issues.apache.org/jira/browse/IGNITE-23106">IGNITE-23106</a> if necessary.
     *
     * @see #drainCheckpointBuffers(ByteBuffer)
     */
    private static final int CP_BUFFER_PAGES_BATCH_THRESHOLD = 10;

    /** Checkpoint specific metrics tracker. */
    private final CheckpointMetricsTracker tracker;

    /** Queue of dirty partitions IDs to write under this task. */
    private final IgniteConcurrentMultiPairQueue<PersistentPageMemory, GroupPartitionId> dirtyPartitionQueue;

    /**
     * List of {@link PersistentPageMemory} instances that have dirty partitions in current checkpoint.
     *
     * @see #drainCheckpointBuffers(ByteBuffer)
     */
    private final List<PersistentPageMemory> pageMemoryList;

    /** Updated partitions -> write statistics. */
    private final ConcurrentMap<GroupPartitionId, PartitionWriteStats> updatedPartitions;

    /** Future which should be finished when all pages would be written. */
    private final CompletableFuture<?> doneFut;

    /** Update heartbeat callback. */
    private final Runnable updateHeartbeat;

    /** Thread local with buffers for the checkpoint threads. Each buffer represent one page for durable memory. */
    private final ThreadLocal<ByteBuffer> threadBuf;

    /** Current checkpoint. This field is updated only by checkpoint thread. */
    private final CheckpointProgressImpl checkpointProgress;

    /** Writer which able to write one page. */
    private final WriteDirtyPage pageWriter;

    /** Page IO registry. */
    private final PageIoRegistry ioRegistry;

    /** Partition meta information manager. */
    private final PartitionMetaManager partitionMetaManager;

    /** Shutdown now. */
    private final BooleanSupplier shutdownNow;

    private final PartitionDestructionLockManager partitionDestructionLockManager;

    /**
     * Creates task for write pages.
     *
     * @param tracker Checkpoint metrics tracker.
     * @param dirtyPartitionQueue Queue of dirty partition IDs to write.
     * @param pageMemoryList List of {@link PersistentPageMemory} instances that have dirty partitions in current checkpoint.
     * @param updatedPartitions Updated partitions.
     * @param doneFut Done future.
     * @param updateHeartbeat Update heartbeat callback.
     * @param threadBuf Thread local byte buffer.
     * @param checkpointProgress Checkpoint progress.
     * @param pageWriter File page store manager.
     * @param ioRegistry Page IO registry.
     * @param partitionMetaManager Partition meta information manager.
     * @param shutdownNow Shutdown supplier.
     * @param partitionDestructionLockManager Partition Destruction Lock Manager.
     */
    CheckpointPagesWriter(
            CheckpointMetricsTracker tracker,
            IgniteConcurrentMultiPairQueue<PersistentPageMemory, GroupPartitionId> dirtyPartitionQueue,
            List<PersistentPageMemory> pageMemoryList,
            ConcurrentMap<GroupPartitionId, PartitionWriteStats> updatedPartitions,
            CompletableFuture<?> doneFut,
            Runnable updateHeartbeat,
            ThreadLocal<ByteBuffer> threadBuf,
            CheckpointProgressImpl checkpointProgress,
            WriteDirtyPage pageWriter,
            PageIoRegistry ioRegistry,
            PartitionMetaManager partitionMetaManager,
            BooleanSupplier shutdownNow,
            PartitionDestructionLockManager partitionDestructionLockManager
    ) {
        this.tracker = tracker;
        this.dirtyPartitionQueue = dirtyPartitionQueue;
        this.pageMemoryList = pageMemoryList;
        this.updatedPartitions = updatedPartitions;
        this.doneFut = doneFut;
        this.updateHeartbeat = updateHeartbeat;
        this.threadBuf = threadBuf;
        this.checkpointProgress = checkpointProgress;
        this.pageWriter = pageWriter;
        this.ioRegistry = ioRegistry;
        this.partitionMetaManager = partitionMetaManager;
        this.shutdownNow = shutdownNow;
        this.partitionDestructionLockManager = partitionDestructionLockManager;
    }

    @Override
    public void run() {
        try {
            Map<PersistentPageMemory, List<DirtyFullPageId>> pageIdsToRetry = new HashMap<>();

            ByteBuffer tmpWriteBuf = threadBuf.get();

            var queueResult = new Result<PersistentPageMemory, GroupPartitionId>();

            while (!shutdownNow.getAsBoolean() && dirtyPartitionQueue.next(queueResult)) {
                updateHeartbeat.run();

                PersistentPageMemory pageMemory = queueResult.getKey();

                PageStoreWriter pageStoreWriter = createPageStoreWriter(pageMemory, pageIdsToRetry);

                writeDirtyPages(pageMemory, queueResult.getValue(), tmpWriteBuf, pageStoreWriter);
            }

            int attemptWriteRetryDirtyPages = 0;

            while (!shutdownNow.getAsBoolean() && !pageIdsToRetry.isEmpty()) {
                updateHeartbeat.run();

                pageIdsToRetry = writeRetryDirtyPages(pageIdsToRetry, tmpWriteBuf, attemptWriteRetryDirtyPages++);
            }

            doneFut.complete(null);
        } catch (Throwable e) {
            doneFut.completeExceptionally(e);
        }
    }

    private void writeDirtyPages(
            PersistentPageMemory pageMemory,
            GroupPartitionId partitionId,
            ByteBuffer tmpWriteBuf,
            PageStoreWriter pageStoreWriter
    ) throws IgniteInternalCheckedException {
        CheckpointDirtyPagesView checkpointDirtyPagesView = checkpointDirtyPagesView(pageMemory, partitionId);

        Lock partitionDestructionLock = partitionDestructionLockManager.destructionLock(partitionId).readLock();

        partitionDestructionLock.lock();

        try {
            addUpdatePartitionCounterIfAbsent(partitionId);

            boolean isMetaWritten = false;

            for (int i = 0; i < checkpointDirtyPagesView.size() && !shutdownNow.getAsBoolean(); i++) {
                updateHeartbeat.run();

                DirtyFullPageId pageId = checkpointDirtyPagesView.get(i);

                if (!isMetaWritten && isPartGenerationMatchWithMeta(pageId, partitionId)) {
                    writePartitionMeta(pageMemory, partitionId, tmpWriteBuf.rewind());

                    isMetaWritten = true;
                }

                writeDirtyPage(pageMemory, pageId, tmpWriteBuf, pageStoreWriter, true);
            }
        } finally {
            partitionDestructionLock.unlock();
        }
    }

    private void writeDirtyPage(
            PersistentPageMemory pageMemory,
            DirtyFullPageId pageId,
            ByteBuffer tmpWriteBuf,
            PageStoreWriter pageStoreWriter,
            boolean useTryWriteLockLockOnPage
    ) throws IgniteInternalCheckedException {
        // Should also be done for partitions that will be destroyed to remove their pages from the data region.
        pageMemory.checkpointWritePage(pageId, tmpWriteBuf.rewind(), pageStoreWriter, tracker, useTryWriteLockLockOnPage);

        drainCheckpointBuffers(tmpWriteBuf);
    }

    private Map<PersistentPageMemory, List<DirtyFullPageId>> writeRetryDirtyPages(
            Map<PersistentPageMemory, List<DirtyFullPageId>> pageIdsToRetry,
            ByteBuffer tmpWriteBuf,
            int attempt
    ) throws IgniteInternalCheckedException {
        boolean useTryWriteLockOnPage = attempt < MAX_ATTEMPT_WRITE_RETRY_DIRTY_PAGES;

        if (LOG.isInfoEnabled()) {
            int pageCount = pageIdsToRetry.values().stream().mapToInt(List::size).sum();

            LOG.info(
                    "Checkpoint pages were not written yet due to unsuccessful page write lock acquisition and will be retried: "
                            + "[pageCount={}, attempt={}, useTryWriteLockOnPage={}]",
                    pageCount, attempt, useTryWriteLockOnPage
            );
        }

        Map<PersistentPageMemory, List<DirtyFullPageId>> newPageIdsToRetry = useTryWriteLockOnPage ? new HashMap<>() : Map.of();

        for (Entry<PersistentPageMemory, List<DirtyFullPageId>> entry : pageIdsToRetry.entrySet()) {
            PersistentPageMemory pageMemory = entry.getKey();

            PageStoreWriter pageStoreWriter = createPageStoreWriter(pageMemory, newPageIdsToRetry);

            GroupPartitionId partitionId = null;

            Lock partitionDestructionLock = null;

            try {
                for (DirtyFullPageId pageId : entry.getValue()) {
                    if (shutdownNow.getAsBoolean()) {
                        return Map.of();
                    }

                    updateHeartbeat.run();

                    if (partitionIdChanged(partitionId, pageId)) {
                        if (partitionDestructionLock != null) {
                            partitionDestructionLock.unlock();
                        }

                        partitionId = GroupPartitionId.convert(pageId);

                        partitionDestructionLock = partitionDestructionLockManager.destructionLock(partitionId).readLock();

                        partitionDestructionLock.lock();
                    }

                    writeDirtyPage(pageMemory, pageId, tmpWriteBuf, pageStoreWriter, useTryWriteLockOnPage);
                }
            } finally {
                if (partitionDestructionLock != null) {
                    partitionDestructionLock.unlock();
                }
            }
        }

        return newPageIdsToRetry;
    }

    /**
     * Checkpoints parts of checkpoint buffers if they are close to overflow. Uses
     * {@link PersistentPageMemory#isCpBufferOverflowThresholdExceeded()} to detect that.
     */
    private void drainCheckpointBuffers(ByteBuffer tmpWriteBuf) throws IgniteInternalCheckedException {
        boolean retry = true;

        while (retry) {
            retry = false;

            // We iterate over a list of page memory instances and delete at most "CP_BUFFER_PAGES_BATCH_THRESHOLD" pages.
            // If any of instances still return "true" from "isCpBufferOverflowThresholdExceeded", then we would repeat this loop using a
            // "retry" flag. This guarantees that every page memory instance has a continuous process of checkpoint buffer draining, meaning
            // that those who wait for a free space will receive it in a short time.
            for (PersistentPageMemory pageMemory : pageMemoryList) {
                int count = 0;

                PageStoreWriter pageStoreWriter = createPageStoreWriter(pageMemory, null);

                while (pageMemory.isCpBufferOverflowThresholdExceeded()) {
                    if (++count >= CP_BUFFER_PAGES_BATCH_THRESHOLD) {
                        retry = true;

                        break;
                    }

                    updateHeartbeat.run();

                    DirtyFullPageId cpPageId = pageMemory.pullPageFromCpBuffer();

                    if (cpPageId.equals(NULL_PAGE)) {
                        break;
                    }

                    GroupPartitionId partitionId = GroupPartitionId.convert(cpPageId);

                    Lock partitionDestructionLock = partitionDestructionLockManager.destructionLock(partitionId).readLock();

                    partitionDestructionLock.lock();

                    try {
                        addUpdatePartitionCounterIfAbsent(partitionId);

                        pageMemory.checkpointWritePage(cpPageId, tmpWriteBuf.rewind(), pageStoreWriter, tracker, true);
                    } finally {
                        partitionDestructionLock.unlock();
                    }
                }
            }
        }
    }

    private void addUpdatePartitionCounterIfAbsent(GroupPartitionId partitionId) {
        // We deliberately avoid "computeIfAbsent" here for the sake of performance.
        // For the overwhelming amount of calls "partitionId" should already be in the set.
        if (!updatedPartitions.containsKey(partitionId)) {
            updatedPartitions.putIfAbsent(partitionId, new PartitionWriteStats());
        }
    }

    private boolean isPartGenerationMatchWithMeta(DirtyFullPageId pageId, GroupPartitionId partitionId) {
        PartitionMeta partitionMeta = partitionMetaManager.getMeta(partitionId);

        return partitionMeta != null && partitionMeta.partitionGeneration() == pageId.partitionGeneration();
    }

    /**
     * Returns a new instance of {@link PageStoreWriter}.
     *
     * @param pageMemory Page memory.
     * @param pagesToRetry Page IDs that need to be rewritten.
     */
    private PageStoreWriter createPageStoreWriter(
            PersistentPageMemory pageMemory,
            @Nullable Map<PersistentPageMemory, List<DirtyFullPageId>> pagesToRetry
    ) {
        return (fullPageId, buf, tag) -> {
            if (tag == TRY_AGAIN_TAG) {
                if (pagesToRetry != null) {
                    pagesToRetry.computeIfAbsent(pageMemory, k -> new ArrayList<>()).add(fullPageId);
                }

                return;
            }

            long pageId = fullPageId.pageId();

            assert getType(buf) != 0 : "Invalid state. Type is 0! pageId = " + hexLong(pageId);
            assert getVersion(buf) != 0 : "Invalid state. Version is 0! pageId = " + hexLong(pageId);
            assert fullPageId.pageIdx() != 0 : "Invalid pageIdx. Index is 0! pageId = " + hexLong(pageId);
            assert !(ioRegistry.resolve(buf) instanceof PartitionMetaIo) : "Invalid IO type. pageId = " + hexLong(pageId);

            if (flag(pageId) == FLAG_DATA) {
                tracker.onDataPageWritten();
            }

            checkpointProgress.writtenPagesCounter().incrementAndGet();

            PageWriteTarget target = pageWriter.write(pageMemory, fullPageId, buf);

            // Record which file type received the write.
            GroupPartitionId partitionId = GroupPartitionId.convert(fullPageId);
            PartitionWriteStats writeStats = updatedPartitions.get(partitionId);

            switch (target) {
                case MAIN_FILE:
                    writeStats.recordMainFileWrite();
                    break;
                case DELTA_FILE:
                    writeStats.recordDeltaFileWrite();
                    break;
                case NONE:
                    // Page write was skipped (e.g., partition being destroyed), don't record.
                    break;
                default:
                    throw new AssertionError("Unexpected PageWriteTarget: " + target);
            }
        };
    }

    private void writePartitionMeta(
            PersistentPageMemory pageMemory,
            GroupPartitionId partitionId,
            ByteBuffer buffer
    ) throws IgniteInternalCheckedException {
        PartitionMeta partitionMeta = partitionMetaManager.getMeta(partitionId);

        // If this happens, then the partition is destroyed.
        if (partitionMeta == null) {
            return;
        }

        PartitionMetaSnapshot partitionMetaSnapshot = partitionMeta.metaSnapshot(checkpointProgress.id());

        partitionMetaManager.writeMetaToBuffer(partitionId, partitionMetaSnapshot, buffer.rewind());

        var fullPageId = new DirtyFullPageId(
                partitionMetaPageId(partitionId.getPartitionId()),
                partitionId.getGroupId(),
                partitionMeta.partitionGeneration()
        );

        PageWriteTarget target = pageWriter.write(pageMemory, fullPageId, buffer.rewind());

        checkpointProgress.writtenPagesCounter().incrementAndGet();

        // Record which file type received the write.
        PartitionWriteStats writeStats = updatedPartitions.get(partitionId);

        switch (target) {
            case MAIN_FILE:
                writeStats.recordMainFileWrite();
                break;
            case DELTA_FILE:
                writeStats.recordDeltaFileWrite();
                break;
            case NONE:
                // Page write was skipped (e.g., partition being destroyed), don't record.
                break;
            default:
                throw new AssertionError("Unexpected PageWriteTarget: " + target);
        }

        updateHeartbeat.run();
    }

    private CheckpointDirtyPagesView checkpointDirtyPagesView(PersistentPageMemory pageMemory, GroupPartitionId partitionId) {
        CheckpointDirtyPages checkpointDirtyPages = checkpointProgress.pagesToWrite();

        assert checkpointDirtyPages != null;

        CheckpointDirtyPagesView partitionView = checkpointDirtyPages.getPartitionView(
                pageMemory,
                partitionId.getGroupId(),
                partitionId.getPartitionId()
        );

        assert partitionView != null : String.format("Unable to find view for dirty pages: [partitionId=%s, pageMemory=%s]", partitionId,
                pageMemory);

        return partitionView;
    }

    private static boolean partitionIdChanged(@Nullable GroupPartitionId partitionId, FullPageId pageId) {
        return partitionId == null || partitionId.getGroupId() != pageId.groupId() || partitionId.getPartitionId() != pageId.partitionId();
    }
}
