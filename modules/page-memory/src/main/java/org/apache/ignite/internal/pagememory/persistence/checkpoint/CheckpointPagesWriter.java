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
import static org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue.EMPTY;
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
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.CheckpointDirtyPagesView;
import org.apache.ignite.internal.pagememory.persistence.io.PartitionMetaIo;
import org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue;
import org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue.Result;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of page writer which able to store pages to disk during checkpoint.
 */
public class CheckpointPagesWriter implements Runnable {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(CheckpointPagesWriter.class);

    /**
     * Size of a batch of pages that we drain from a single checkpoint buffer at the same time. The value of {@code 10} is chosen
     * arbitrarily. We may reconsider it in <a href="https://issues.apache.org/jira/browse/IGNITE-23106">IGNITE-23106</a> if necessary.
     *
     * @see #drainCheckpointBuffers(ByteBuffer, Map)
     */
    private static final int CP_BUFFER_PAGES_BATCH_THRESHOLD = 10;

    /** Checkpoint specific metrics tracker. */
    private final CheckpointMetricsTracker tracker;

    /** Queue of dirty partitions IDs to write under this task. */
    private final IgniteConcurrentMultiPairQueue<PersistentPageMemory, GroupPartitionId> dirtyPartitionQueue;

    /**
     * List of {@link PersistentPageMemory} instances that have dirty partitions in current checkpoint.
     *
     * @see #drainCheckpointBuffers(ByteBuffer, Map)
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

    void run0() {
        try {
            IgniteConcurrentMultiPairQueue<PersistentPageMemory, FullPageId> pageIdsToRetry = writePages(null);

            while (!pageIdsToRetry.isEmpty()) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("Checkpoint pages were not written yet due to "
                            + "unsuccessful page write lock acquisition and will be retried [pageCount={}]", pageIdsToRetry.size());
                }

                pageIdsToRetry = writePages(pageIdsToRetry);
            }

            doneFut.complete(null);
        } catch (Throwable e) {
            doneFut.completeExceptionally(e);
        }
    }

    @Override
    public void run() {
        try {
            var queueResult = new Result<PersistentPageMemory, GroupPartitionId>();

            while (!shutdownNow.getAsBoolean() && dirtyPartitionQueue.next(queueResult)) {
                updateHeartbeat.run();

                writeDirtyPages(queueResult.getKey(), queueResult.getValue());
            }

            doneFut.complete(null);
        } catch (Throwable e) {
            doneFut.completeExceptionally(e);
        }
    }

    private void writeDirtyPages(PersistentPageMemory pageMemory, GroupPartitionId partitionId) throws IgniteInternalCheckedException {
        CheckpointDirtyPagesView checkpointDirtyPagesView = checkpointDirtyPagesView(pageMemory, partitionId);

        checkpointProgress.onStartPartitionProcessing(partitionId);

        try {
            ByteBuffer tmpWriteBuf = threadBuf.get();

            if (shouldWriteMetaPage(partitionId)) {
                writePartitionMeta(pageMemory, partitionId, tmpWriteBuf.rewind());
            }

            var pageIdsToRetry = new HashMap<PersistentPageMemory, List<FullPageId>>();

            for (int i = 0; i < checkpointDirtyPagesView.size() && !shutdownNow.getAsBoolean(); i++) {
                updateHeartbeat.run();

                FullPageId pageId = checkpointDirtyPagesView.get(i);

                if (pageId.pageIdx() == 0) {
                    // Skip meta-pages, they are written by "writePartitionMeta".
                    continue;
                }

                writeDirtyPage(pageMemory, pageId, tmpWriteBuf, pageIdsToRetry);
            }

            writeRetryDirtyPages(pageIdsToRetry, tmpWriteBuf);
        } finally {
            checkpointProgress.onFinishPartitionProcessing(partitionId);
        }
    }

    private void writeDirtyPage(
            PersistentPageMemory pageMemory,
            FullPageId pageId,
            ByteBuffer tmpWriteBuf,
            Map<PersistentPageMemory, List<FullPageId>> pageIdsToRetry
    ) throws IgniteInternalCheckedException {
        PageStoreWriter pageStoreWriter = createPageStoreWriter(pageMemory, pageIdsToRetry);

        // Should also be done for partitions that will be destroyed to remove their pages from the data region.
        pageMemory.checkpointWritePage(pageId, tmpWriteBuf.rewind(), pageStoreWriter, tracker);

        // TODO: IGNITE-23115 починить
        drainCheckpointBuffers(tmpWriteBuf, new HashMap<>());
    }

    private void writeRetryDirtyPages(
            Map<PersistentPageMemory, List<FullPageId>> pageIdsToRetry,
            ByteBuffer tmpWriteBuf
    ) throws IgniteInternalCheckedException {
        while (!pageIdsToRetry.isEmpty()) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Checkpoint pages were not written yet due to "
                        + "unsuccessful page write lock acquisition and will be retried [pageCount={}]", pageIdsToRetry.size());
            }

            var newPageIdsToRetry = new HashMap<PersistentPageMemory, List<FullPageId>>();

            for (Entry<PersistentPageMemory, List<FullPageId>> entry : pageIdsToRetry.entrySet()) {
                for (FullPageId pageId : entry.getValue()) {
                    if (shutdownNow.getAsBoolean()) {
                        return;
                    }

                    updateHeartbeat.run();

                    writeDirtyPage(entry.getKey(), pageId, tmpWriteBuf, newPageIdsToRetry);
                }
            }

            pageIdsToRetry = newPageIdsToRetry;
        }
    }

    /**
     * Writes dirty pages.
     *
     * @param writePageIds Queue of dirty page IDs to write.
     * @return pagesToRetry Queue dirty page IDs which should be retried.
     */
    private IgniteConcurrentMultiPairQueue<PersistentPageMemory, FullPageId> writePages(
            IgniteConcurrentMultiPairQueue<PersistentPageMemory, FullPageId> writePageIds
    ) throws IgniteInternalCheckedException {
        Map<PersistentPageMemory, List<FullPageId>> pageIdsToRetry = new HashMap<>();

        Map<PersistentPageMemory, PageStoreWriter> pageStoreWriters = new HashMap<>();

        // Page store writers for checkpoint buffer pages are located in a separate map, because they would not add elements to a
        // "pageIdsToRetry" map. Instead, they would ignore unsuccessful write lock acquisitions. It's implemented this way in order to
        // avoid duplicates in "pageIdsToRetry", there must only be a single source of pages to achieve that.
        Map<PersistentPageMemory, PageStoreWriter> cpBufferPageStoreWriters = new HashMap<>();

        ByteBuffer tmpWriteBuf = threadBuf.get();

        Result<PersistentPageMemory, FullPageId> queueResult = new Result<>();

        GroupPartitionId partitionId = null;

        try {
            // TODO https://issues.apache.org/jira/browse/IGNITE-23115 Try to write file per thread.
            while (!shutdownNow.getAsBoolean() && writePageIds.next(queueResult)) {
                updateHeartbeat.run();

                FullPageId fullId = queueResult.getValue();

                PersistentPageMemory pageMemory = queueResult.getKey();

                if (hasPartitionChanged(partitionId, fullId)) {
                    GroupPartitionId newPartitionId = toPartitionId(fullId);

                    // Starting for the new partition.
                    checkpointProgress.onStartPartitionProcessing(newPartitionId);

                    if (partitionId != null) {
                        // Finishing for the previous partition.
                        // TODO https://issues.apache.org/jira/browse/IGNITE-23105 Reimplement partition destruction awaiting.
                        checkpointProgress.onFinishPartitionProcessing(partitionId);
                    }

                    partitionId = newPartitionId;

                    if (shouldWriteMetaPage(partitionId)) {
                        writePartitionMeta(pageMemory, partitionId, tmpWriteBuf.rewind());
                    }
                }

                PageStoreWriter pageStoreWriter = pageStoreWriters.computeIfAbsent(
                        pageMemory,
                        pm -> createPageStoreWriter(pm, pageIdsToRetry)
                );

                if (fullId.pageIdx() == 0) {
                    // Skip meta-pages, they are written by "writePartitionMeta".
                    continue;
                }

                // Should also be done for partitions that will be destroyed to remove their pages from the data region.
                pageMemory.checkpointWritePage(fullId, tmpWriteBuf.rewind(), pageStoreWriter, tracker);

                drainCheckpointBuffers(tmpWriteBuf, cpBufferPageStoreWriters);
            }
        } finally {
            if (partitionId != null) {
                checkpointProgress.onFinishPartitionProcessing(partitionId);
            }
        }

        return pageIdsToRetry.isEmpty() ? EMPTY : new IgniteConcurrentMultiPairQueue<>(pageIdsToRetry);
    }

    /**
     * Checkpoints parts of checkpoint buffers if they are close to overflow. Uses
     * {@link PersistentPageMemory#isCpBufferOverflowThresholdExceeded()} to detect that.
     */
    private void drainCheckpointBuffers(
            ByteBuffer tmpWriteBuf,
            Map<PersistentPageMemory, PageStoreWriter> pageStoreWriters
    ) throws IgniteInternalCheckedException {
        PageStoreWriter pageStoreWriter;
        boolean retry = true;

        while (retry) {
            retry = false;

            // We iterate over a list of page memory instances and delete at most "CP_BUFFER_PAGES_BATCH_THRESHOLD" pages.
            // If any of instances still return "true" from "isCpBufferOverflowThresholdExceeded", then we would repeat this loop using a
            // "retry" flag. This guarantees that every page memory instance has a continuous process of checkpoint buffer draining, meaning
            // that those who wait for a free space will receive it in a short time.
            for (PersistentPageMemory pageMemory : pageMemoryList) {
                int count = 0;

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

                    GroupPartitionId partitionId = toPartitionId(cpPageId);

                    if (shouldWriteMetaPage(partitionId)) {
                        writePartitionMeta(pageMemory, partitionId, tmpWriteBuf.rewind());
                    }

                    pageStoreWriter = pageStoreWriters.computeIfAbsent(
                            pageMemory,
                            pm -> createPageStoreWriter(pm, null)
                    );

                    pageMemory.checkpointWritePage(cpPageId, tmpWriteBuf.rewind(), pageStoreWriter, tracker);
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

            pageWriter.write(pageMemory, fullPageId, buf);

            updatedPartitions.get(toPartitionId(fullPageId)).increment();
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

        pageWriter.write(pageMemory, fullPageId, buffer.rewind());

        checkpointProgress.writtenPagesCounter().incrementAndGet();

        updatedPartitions.get(partitionId).increment();

        updateHeartbeat.run();
    }

    private static boolean hasPartitionChanged(@Nullable GroupPartitionId partitionId, FullPageId pageId) {
        return partitionId == null || partitionId.getPartitionId() != pageId.partitionId() || partitionId.getGroupId() != pageId.groupId();
    }

    private static GroupPartitionId toPartitionId(FullPageId pageId) {
        return new GroupPartitionId(pageId.groupId(), pageId.partitionId());
    }

    private CheckpointDirtyPagesView checkpointDirtyPagesView(PersistentPageMemory pageMemory, GroupPartitionId partitionId) {
        CheckpointDirtyPages checkpointDirtyPages = checkpointProgress.pagesToWrite();

        assert checkpointDirtyPages != null;

        CheckpointDirtyPagesView partitionView = checkpointDirtyPages.getPartitionView(
                pageMemory,
                partitionId.getGroupId(),
                partitionId.getPartitionId()
        );

        assert partitionView != null : String.format("Unable to find view for dirty pages: [patitionId=%s, pageMemory=%s]", partitionId,
                pageMemory);

        return partitionView;
    }
}
