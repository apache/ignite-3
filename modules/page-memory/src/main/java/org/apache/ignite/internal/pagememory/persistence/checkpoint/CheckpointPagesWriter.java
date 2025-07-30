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
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PageStoreWriter;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta.PartitionMetaSnapshot;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.WriteDirtyPage;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.CheckpointPagesView;
import org.apache.ignite.internal.pagememory.persistence.io.PartitionMetaIo;
import org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue;
import org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue.Result;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of page writer which able to store pages to disk during checkpoint.
 */
// TODO: IGNITE-23203 Write retry pages in multiple threads 
public class CheckpointPagesWriter implements Runnable {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(CheckpointPagesWriter.class);

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

    /** Updated partitions -> count of written pages. */
    private final ConcurrentMap<GroupPartitionId, LongAdder> updatedPartitions;

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
     */
    CheckpointPagesWriter(
            CheckpointMetricsTracker tracker,
            IgniteConcurrentMultiPairQueue<PersistentPageMemory, GroupPartitionId> dirtyPartitionQueue,
            List<PersistentPageMemory> pageMemoryList,
            ConcurrentMap<GroupPartitionId, LongAdder> updatedPartitions,
            CompletableFuture<?> doneFut,
            Runnable updateHeartbeat,
            ThreadLocal<ByteBuffer> threadBuf,
            CheckpointProgressImpl checkpointProgress,
            WriteDirtyPage pageWriter,
            PageIoRegistry ioRegistry,
            PartitionMetaManager partitionMetaManager,
            BooleanSupplier shutdownNow
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
    }

    @Override
    public void run() {
        try {
            Map<PersistentPageMemory, List<FullPageId>> pageIdsToRetry = new HashMap<>();

            ByteBuffer tmpWriteBuf = threadBuf.get();

            var queueResult = new Result<PersistentPageMemory, GroupPartitionId>();

            while (!shutdownNow.getAsBoolean() && dirtyPartitionQueue.next(queueResult)) {
                updateHeartbeat.run();

                PersistentPageMemory pageMemory = queueResult.getKey();

                PageStoreWriter pageStoreWriter = createPageStoreWriter(pageMemory, pageIdsToRetry);

                writeDirtyPages(pageMemory, queueResult.getValue(), tmpWriteBuf, pageStoreWriter);
            }

            while (!shutdownNow.getAsBoolean() && !pageIdsToRetry.isEmpty()) {
                updateHeartbeat.run();

                pageIdsToRetry = writeRetryDirtyPages(pageIdsToRetry, tmpWriteBuf);
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
        CheckpointPagesView checkpointPagesView = checkpointDirtyPagesView(pageMemory, partitionId);

        checkpointProgress.blockPartitionDestruction(partitionId);

        try {
            if (shouldWriteMetaPage(partitionId)) {
                writePartitionMeta(pageMemory, partitionId, tmpWriteBuf.rewind());
            }

            for (int i = 0; i < checkpointPagesView.dirtyPagesSize() && !shutdownNow.getAsBoolean(); i++) {
                processPage(pageMemory, tmpWriteBuf, pageStoreWriter, checkpointPagesView.getDirtyPage(i));
            }

            for (int i = 0; i < checkpointPagesView.newPagesSize() && !shutdownNow.getAsBoolean(); i++) {
                processPage(pageMemory, tmpWriteBuf, pageStoreWriter, checkpointPagesView.getNewPage(i));
            }
        } finally {
            checkpointProgress.unblockPartitionDestruction(partitionId);
        }
    }

    private void processPage(
            PersistentPageMemory pageMemory,
            ByteBuffer tmpWriteBuf,
            PageStoreWriter pageStoreWriter,
            FullPageId pageId
    )
            throws IgniteInternalCheckedException {
        updateHeartbeat.run();

        if (pageId.pageIdx() == 0) {
            // Skip meta-pages, they are written by "writePartitionMeta".
            return;
        }

        writeDirtyPage(pageMemory, pageId, tmpWriteBuf, pageStoreWriter);
    }

    private void writeDirtyPage(
            PersistentPageMemory pageMemory,
            FullPageId pageId,
            ByteBuffer tmpWriteBuf,
            PageStoreWriter pageStoreWriter
    ) throws IgniteInternalCheckedException {
        // Should also be done for partitions that will be destroyed to remove their pages from the data region.
        pageMemory.checkpointWritePage(pageId, tmpWriteBuf.rewind(), pageStoreWriter, tracker);

        drainCheckpointBuffers(tmpWriteBuf);
    }

    private Map<PersistentPageMemory, List<FullPageId>> writeRetryDirtyPages(
            Map<PersistentPageMemory, List<FullPageId>> pageIdsToRetry,
            ByteBuffer tmpWriteBuf
    ) throws IgniteInternalCheckedException {
        if (LOG.isInfoEnabled()) {
            int pageCount = pageIdsToRetry.values().stream().mapToInt(List::size).sum();

            LOG.info("Checkpoint pages were not written yet due to "
                    + "unsuccessful page write lock acquisition and will be retried [pageCount={}]", pageCount);
        }

        var newPageIdsToRetry = new HashMap<PersistentPageMemory, List<FullPageId>>();

        for (Entry<PersistentPageMemory, List<FullPageId>> entry : pageIdsToRetry.entrySet()) {
            PersistentPageMemory pageMemory = entry.getKey();

            PageStoreWriter pageStoreWriter = createPageStoreWriter(pageMemory, newPageIdsToRetry);

            GroupPartitionId partitionId = null;

            try {
                for (FullPageId pageId : entry.getValue()) {
                    if (shutdownNow.getAsBoolean()) {
                        return Map.of();
                    }

                    updateHeartbeat.run();

                    if (partitionIdChanged(partitionId, pageId)) {
                        if (partitionId != null) {
                            checkpointProgress.unblockPartitionDestruction(partitionId);
                        }

                        partitionId = GroupPartitionId.convert(pageId);

                        checkpointProgress.blockPartitionDestruction(partitionId);
                    }

                    writeDirtyPage(pageMemory, pageId, tmpWriteBuf, pageStoreWriter);
                }
            } finally {
                if (partitionId != null) {
                    checkpointProgress.unblockPartitionDestruction(partitionId);
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

                    FullPageId cpPageId = pageMemory.pullPageFromCpBuffer();

                    if (cpPageId.equals(FullPageId.NULL_PAGE)) {
                        break;
                    }

                    GroupPartitionId partitionId = GroupPartitionId.convert(cpPageId);

                    checkpointProgress.blockPartitionDestruction(partitionId);

                    try {
                        if (shouldWriteMetaPage(partitionId)) {
                            writePartitionMeta(pageMemory, partitionId, tmpWriteBuf.rewind());
                        }

                        pageMemory.checkpointWritePage(cpPageId, tmpWriteBuf.rewind(), pageStoreWriter, tracker);
                    } finally {
                        checkpointProgress.unblockPartitionDestruction(partitionId);
                    }
                }
            }
        }
    }

    /**
     * Returns {@code true} if checkpointer should write meta page of partition. Guaranteed to return {@code true} exactly once for every
     * passed partition ID.
     */
    private boolean shouldWriteMetaPage(GroupPartitionId partitionId) {
        // We deliberately avoid "computeIfAbsent" here for the sake of performance.
        // For the overwhelming amount of calls "partitionId" should already be in the set.
        return !updatedPartitions.containsKey(partitionId)
                && null == updatedPartitions.putIfAbsent(partitionId, new LongAdder());
    }

    /**
     * Returns a new instance of {@link PageStoreWriter}.
     *
     * @param pageMemory Page memory.
     * @param pagesToRetry Page IDs that need to be rewritten.
     */
    private PageStoreWriter createPageStoreWriter(
            PersistentPageMemory pageMemory,
            @Nullable Map<PersistentPageMemory, List<FullPageId>> pagesToRetry
    ) {
        return (fullPageId, buf, newPage, tag) -> {
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

            pageWriter.write(pageMemory, fullPageId, buf, newPage);

            updatedPartitions.get(GroupPartitionId.convert(fullPageId)).increment();
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

        FullPageId fullPageId = new FullPageId(partitionMetaPageId(partitionId.getPartitionId()), partitionId.getGroupId());

        //TODO check
        pageWriter.write(pageMemory, fullPageId, buffer.rewind(), pageMemory.hasLoadedPage(fullPageId));

        checkpointProgress.writtenPagesCounter().incrementAndGet();

        updatedPartitions.get(partitionId).increment();

        updateHeartbeat.run();
    }

    private CheckpointPagesView checkpointDirtyPagesView(PersistentPageMemory pageMemory, GroupPartitionId partitionId) {
        CheckpointDirtyPages checkpointDirtyPages = checkpointProgress.pagesToWrite();

        assert checkpointDirtyPages != null;

        CheckpointPagesView partitionView = checkpointDirtyPages.getPartitionView(
                pageMemory,
                partitionId.getGroupId(),
                partitionId.getPartitionId()
        );

        assert partitionView != null : String.format("Unable to find view for dirty pages: [patitionId=%s, pageMemory=%s]", partitionId,
                pageMemory);

        return partitionView;
    }

    private static boolean partitionIdChanged(@Nullable GroupPartitionId partitionId, FullPageId pageId) {
        return partitionId == null || partitionId.getGroupId() != pageId.groupId() || partitionId.getPartitionId() != pageId.partitionId();
    }
}
