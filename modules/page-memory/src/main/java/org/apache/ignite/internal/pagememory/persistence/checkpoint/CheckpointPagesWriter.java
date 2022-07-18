/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.apache.ignite.internal.util.IgniteUtils.hexLong;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PageStoreWriter;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta.PartitionMetaSnapshot;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.io.PartitionMetaIo;
import org.apache.ignite.internal.pagememory.persistence.store.PageStore;
import org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue;
import org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue.Result;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Implementation of page writer which able to store pages to disk during checkpoint.
 */
public class CheckpointPagesWriter implements Runnable {
    /** Logger. */
    private final IgniteLogger log;

    /** Checkpoint specific metrics tracker. */
    private final CheckpointMetricsTracker tracker;

    /**
     * Queue of dirty page IDs to write under this task.
     *
     * <p>Overall pages to write may be greater than this queue, since it may be necessary to retire write some pages due to unsuccessful
     * page write lock acquisition
     */
    private final IgniteConcurrentMultiPairQueue<PersistentPageMemory, FullPageId> writePageIds;

    /** Queue of dirty partition IDs to update partition meta page under this task. */
    private final IgniteConcurrentMultiPairQueue<PersistentPageMemory, GroupPartitionId> updatePartitionIds;

    /** Page store used to write -> Count of written pages. */
    private final ConcurrentMap<PageStore, LongAdder> updStores;

    /** Future which should be finished when all pages would be written. */
    private final CompletableFuture<?> doneFut;

    /** Some action which will be executed every time before page will be written. */
    private final Runnable beforePageWrite;

    /** Thread local with buffers for the checkpoint threads. Each buffer represent one page for durable memory. */
    private final ThreadLocal<ByteBuffer> threadBuf;

    /** Current checkpoint. This field is updated only by checkpoint thread. */
    private final CheckpointProgressImpl checkpointProgress;

    /** Writer which able to write one page. */
    private final CheckpointPageWriter pageWriter;

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
     * @param updatePartitionIds Queue of dirty partition IDs to update.
     * @param updStores Updating storage.
     * @param doneFut Done future.
     * @param beforePageWrite Action to be performed before every page write.
     * @param log Logger.
     * @param threadBuf Thread local byte buffer.
     * @param checkpointProgress Checkpoint progress.
     * @param pageWriter File page store manager.
     * @param ioRegistry Page IO registry.
     * @param partitionMetaManager Partition meta information manager.
     * @param shutdownNow Shutdown supplier.
     */
    CheckpointPagesWriter(
            IgniteLogger log,
            CheckpointMetricsTracker tracker,
            IgniteConcurrentMultiPairQueue<PersistentPageMemory, FullPageId> writePageIds,
            IgniteConcurrentMultiPairQueue<PersistentPageMemory, GroupPartitionId> updatePartitionIds,
            ConcurrentMap<PageStore, LongAdder> updStores,
            CompletableFuture<?> doneFut,
            Runnable beforePageWrite,
            ThreadLocal<ByteBuffer> threadBuf,
            CheckpointProgressImpl checkpointProgress,
            CheckpointPageWriter pageWriter,
            PageIoRegistry ioRegistry,
            PartitionMetaManager partitionMetaManager,
            BooleanSupplier shutdownNow
    ) {
        this.log = log;
        this.tracker = tracker;
        this.writePageIds = writePageIds;
        this.updatePartitionIds = updatePartitionIds;
        this.updStores = updStores;
        this.doneFut = doneFut;
        this.beforePageWrite = beforePageWrite;
        this.threadBuf = threadBuf;
        this.checkpointProgress = checkpointProgress;
        this.pageWriter = pageWriter;
        this.ioRegistry = ioRegistry;
        this.partitionMetaManager = partitionMetaManager;
        this.shutdownNow = shutdownNow;
    }

    /** {@inheritDoc} */
    @Override
    public void run() {
        try {
            IgniteConcurrentMultiPairQueue<PersistentPageMemory, FullPageId> pageIdsToRetry = writePages(writePageIds);

            while (!pageIdsToRetry.isEmpty()) {
                if (log.isInfoEnabled()) {
                    log.info("Checkpoint pages were not written yet due to "
                            + "unsuccessful page write lock acquisition and will be retried [pageCount={}]", pageIdsToRetry.size());
                }

                pageIdsToRetry = writePages(pageIdsToRetry);
            }

            updatePartitions();

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

        while (!shutdownNow.getAsBoolean() && writePageIds.next(queueResult)) {
            beforePageWrite.run();

            FullPageId fullId = queueResult.getValue();

            PersistentPageMemory pageMemory = queueResult.getKey();

            tmpWriteBuf.rewind();

            PageStoreWriter pageStoreWriter = pageStoreWriters.computeIfAbsent(pageMemory, pm -> createPageStoreWriter(pm, pageIdsToRetry));

            pageMemory.checkpointWritePage(fullId, tmpWriteBuf, pageStoreWriter, tracker);
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
        return new PageStoreWriter() {
            /** {@inheritDoc} */
            @Override
            public void writePage(FullPageId fullPageId, ByteBuffer buf, int tag) throws IgniteInternalCheckedException {
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

                PageStore store = pageWriter.write(fullPageId, buf);

                updStores.computeIfAbsent(store, k -> new LongAdder()).increment();
            }
        };
    }

    /**
     * Updates the partitions meta page.
     *
     * @throws IgniteInternalCheckedException If failed.
     */
    private void updatePartitions() throws IgniteInternalCheckedException {
        Result<PersistentPageMemory, GroupPartitionId> queueResult = new Result<>();

        while (!shutdownNow.getAsBoolean() && updatePartitionIds.next(queueResult)) {
            GroupPartitionId partitionId = queueResult.getValue();

            ByteBuffer buffer = threadBuf.get();

            PartitionMetaSnapshot partitionMetaSnapshot = partitionMetaManager.getMeta(partitionId).metaSnapshot(checkpointProgress.id());

            partitionMetaManager.writeMetaToBuffer(partitionId, partitionMetaSnapshot, buffer.rewind());

            FullPageId fullPageId = new FullPageId(partitionMetaPageId(partitionId.getPartitionId()), partitionId.getGroupId());

            PageStore store = pageWriter.write(fullPageId, buffer.rewind());

            checkpointProgress.writtenPagesCounter().incrementAndGet();

            updStores.computeIfAbsent(store, k -> new LongAdder()).increment();
        }
    }
}
