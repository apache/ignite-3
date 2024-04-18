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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
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

    /** Checkpoint specific metrics tracker. */
    private final CheckpointMetricsTracker tracker;

    /**
     * Queue of dirty page IDs to write under this task.
     *
     * <p>Overall pages to write may be greater than this queue, since it may be necessary to retire write some pages due to unsuccessful
     * page write lock acquisition
     */
    private final IgniteConcurrentMultiPairQueue<PersistentPageMemory, FullPageId> writePageIds;

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
     * @param writePageIds Queue of dirty page IDs to write.
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
            IgniteConcurrentMultiPairQueue<PersistentPageMemory, FullPageId> writePageIds,
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
        this.writePageIds = writePageIds;
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
            IgniteConcurrentMultiPairQueue<PersistentPageMemory, FullPageId> pageIdsToRetry = writePages(writePageIds);

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

        ByteBuffer tmpWriteBuf = threadBuf.get();

        Result<PersistentPageMemory, FullPageId> queueResult = new Result<>();

        GroupPartitionId partitionId = null;

        AtomicBoolean writeMetaPage = new AtomicBoolean();

        Set<GroupPartitionId> inProgressPartitions = new HashSet<>();

        try {
            while (!shutdownNow.getAsBoolean() && writePageIds.next(queueResult)) {
                updateHeartbeat.run();

                FullPageId fullId = queueResult.getValue();

                PersistentPageMemory pageMemory = queueResult.getKey();

                if (hasPartitionChanged(partitionId, fullId)) {
                    GroupPartitionId newPartitionId = toPartitionId(fullId);

                    // Starting for the new partition.
                    checkpointProgress.onStartPartitionProcessing(newPartitionId);

                    inProgressPartitions.add(newPartitionId);

                    if (partitionId != null) {
                        // Finishing for the previous partition.
                        checkpointProgress.onFinishPartitionProcessing(partitionId);

                        inProgressPartitions.remove(partitionId);
                    }

                    partitionId = newPartitionId;

                    updatedPartitions.computeIfAbsent(partitionId, partId -> {
                        writeMetaPage.set(true);

                        return new LongAdder();
                    });

                    if (writeMetaPage.get()) {
                        writePartitionMeta(pageMemory, partitionId, tmpWriteBuf.rewind());

                        writeMetaPage.set(false);
                    }
                }

                tmpWriteBuf.rewind();

                PageStoreWriter pageStoreWriter = pageStoreWriters.computeIfAbsent(
                        pageMemory,
                        pm -> createPageStoreWriter(pm, pageIdsToRetry)
                );

                if (fullId.pageIdx() == 0) {
                    // Skip meta-pages, they are written by "writePartitionMeta".
                    continue;
                }

                // Should also be done for partitions that will be destroyed to remove their pages from the data region.
                pageMemory.checkpointWritePage(fullId, tmpWriteBuf, pageStoreWriter, tracker);
            }
        } finally {
            inProgressPartitions.forEach(checkpointProgress::onFinishPartitionProcessing);
        }

        return pageIdsToRetry.isEmpty() ? EMPTY : new IgniteConcurrentMultiPairQueue<>(pageIdsToRetry);
    }

    /**
     * Returns a new instance of {@link PageStoreWriter}.
     *
     * @param pageMemory Page memory.
     * @param pagesToRetry Page IDs that need to be rewritten.
     */
    private PageStoreWriter createPageStoreWriter(
            PersistentPageMemory pageMemory,
            Map<PersistentPageMemory, List<FullPageId>> pagesToRetry
    ) {
        return (fullPageId, buf, tag) -> {
            if (tag == TRY_AGAIN_TAG) {
                pagesToRetry.computeIfAbsent(pageMemory, k -> new ArrayList<>()).add(fullPageId);

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
    }

    private static boolean hasPartitionChanged(@Nullable GroupPartitionId partitionId, FullPageId pageId) {
        return partitionId == null || partitionId.getPartitionId() != pageId.partitionId() || partitionId.getGroupId() != pageId.groupId();
    }

    private static GroupPartitionId toPartitionId(FullPageId pageId) {
        return new GroupPartitionId(pageId.groupId(), pageId.partitionId());
    }
}
