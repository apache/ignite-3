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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
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

/** Implementation for POC. */
class CheckpointPagesWriter1 implements Runnable {
    private final IgniteConcurrentMultiPairQueue<PersistentPageMemory, GroupPartitionId> dirtyPartitionIdQueue;

    private final CheckpointProgressImpl checkpointProgress;

    private final CheckpointDirtyPages checkpointPages;

    private final CheckpointMetricsTracker tracker;

    private final PartitionMetaManager partitionMetaManager;

    private final WriteDirtyPage pageWriter;

    private final PageIoRegistry ioRegistry;

    private final ConcurrentMap<GroupPartitionId, LongAdder> updatedPartitions;

    private final ThreadLocal<ByteBuffer> threadBuf;

    private final CompletableFuture<Void> doneFut;

    private final BooleanSupplier shutdownNow;

    private final Runnable updateHeartbeat;

    CheckpointPagesWriter1(
            IgniteConcurrentMultiPairQueue<PersistentPageMemory, GroupPartitionId> dirtyPartitionIdQueue,
            CheckpointProgressImpl checkpointProgress,
            CheckpointDirtyPages checkpointPages,
            CheckpointMetricsTracker tracker,
            PartitionMetaManager partitionMetaManager,
            WriteDirtyPage pageWriter,
            PageIoRegistry ioRegistry,
            ConcurrentMap<GroupPartitionId, LongAdder> updatedPartitions,
            ThreadLocal<ByteBuffer> threadBuf,
            CompletableFuture<Void> doneFut,
            BooleanSupplier shutdownNow,
            Runnable updateHeartbeat
    ) {
        this.dirtyPartitionIdQueue = dirtyPartitionIdQueue;
        this.checkpointProgress = checkpointProgress;
        this.checkpointPages = checkpointPages;
        this.tracker = tracker;
        this.partitionMetaManager = partitionMetaManager;
        this.pageWriter = pageWriter;
        this.ioRegistry = ioRegistry;
        this.updatedPartitions = updatedPartitions;
        this.threadBuf = threadBuf;
        this.doneFut = doneFut;
        this.shutdownNow = shutdownNow;
        this.updateHeartbeat = updateHeartbeat;
    }

    @Override
    public void run() {
        try {
            var nextDirtyPartitionId = new Result<PersistentPageMemory, GroupPartitionId>();

            while (!shutdownNow.getAsBoolean() && dirtyPartitionIdQueue.next(nextDirtyPartitionId)) {
                updateHeartbeat.run();

                writePages(nextDirtyPartitionId.getKey(), nextDirtyPartitionId.getValue());
            }

            doneFut.complete(null);
        } catch (Throwable t) {
            doneFut.completeExceptionally(t);
        }
    }

    private void writePages(PersistentPageMemory pageMemory, GroupPartitionId partitionId) throws Exception {
        checkpointProgress.onStartPartitionProcessing(partitionId);

        try {
            CheckpointDirtyPagesView dirtyPagesView = checkpointPages.getPartitionView(
                    pageMemory,
                    partitionId.getGroupId(),
                    partitionId.getPartitionId()
            );

            ByteBuffer tmpWriteBuf = threadBuf.get();

            if (shouldWriteMetaPage(partitionId)) {
                writePartitionMeta(pageMemory, partitionId, tmpWriteBuf.rewind());
            }

            var pageIdsToRetry = new HashMap<PersistentPageMemory, List<FullPageId>>();

            PageStoreWriter pageStoreWriter = createPageStoreWriter(pageMemory, pageIdsToRetry);

            for (int i = 0; i < dirtyPagesView.size() && !shutdownNow.getAsBoolean(); i++) {
                updateHeartbeat.run();

                FullPageId fullId = dirtyPagesView.get(i);

                if (fullId.pageIdx() == 0) {
                    // Skip meta-pages, they are written by "writePartitionMeta".
                    continue;
                }

                pageMemory.checkpointWritePage(fullId, tmpWriteBuf.rewind(), pageStoreWriter, tracker);

                //drainCheckpointBuffers(tmpWriteBuf, cpBufferPageStoreWriters);
            }

            while (!shutdownNow.getAsBoolean() && !pageIdsToRetry.isEmpty()) {
                updateHeartbeat.run();

                var pageIdsToRetry1 = new HashMap<PersistentPageMemory, List<FullPageId>>();
                pageStoreWriter = createPageStoreWriter(pageMemory, pageIdsToRetry1);

                for (List<FullPageId> pageIds : pageIdsToRetry.values()) {
                    for (FullPageId fullId : pageIds) {
                        if (shutdownNow.getAsBoolean()) {
                            return;
                        }

                        updateHeartbeat.run();

                        pageMemory.checkpointWritePage(fullId, tmpWriteBuf.rewind(), pageStoreWriter, tracker);
                    }
                }

                pageIdsToRetry = pageIdsToRetry1;
            }
        } finally {
            checkpointProgress.onFinishPartitionProcessing(partitionId);
        }
    }

    private boolean shouldWriteMetaPage(GroupPartitionId partitionId) {
        return !updatedPartitions.containsKey(partitionId) && null == updatedPartitions.putIfAbsent(partitionId, new LongAdder());
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

    private static GroupPartitionId toPartitionId(FullPageId pageId) {
        return new GroupPartitionId(pageId.groupId(), pageId.partitionId());
    }
}
