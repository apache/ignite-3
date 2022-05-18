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
import static org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl.TRY_AGAIN_TAG;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.IgniteConcurrentMultiPairQueue.EMPTY;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.flag;
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
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl;
import org.apache.ignite.internal.pagememory.persistence.PageStoreWriter;
import org.apache.ignite.internal.pagememory.persistence.store.PageStore;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteLogger;

/**
 * Implementation of page writer which able to store pages to disk during checkpoint.
 */
public class CheckpointPagesWriter implements Runnable {
    /** Logger. */
    private final IgniteLogger log;

    /** Checkpoint specific metrics tracker. */
    private final CheckpointMetricsTracker tracker;

    /** Collection of page IDs to write under this task. Overall pages to write may be greater than this collection. */
    private final IgniteConcurrentMultiPairQueue<PageMemoryImpl, FullPageId> writePageIds;

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

    /** Shutdown now. */
    private final BooleanSupplier shutdownNow;

    /**
     * Creates task for write pages.
     *
     * @param tracker Checkpoint metrics tracker.
     * @param writePageIds Collection of page IDs to write.
     * @param updStores Updating storage.
     * @param doneFut Done future.
     * @param beforePageWrite Action to be performed before every page write.
     * @param log Logger.
     * @param threadBuf Thread local byte buffer.
     * @param checkpointProgress Checkpoint progress.
     * @param pageWriter File page store manager.
     * @param shutdownNow Shutdown supplier.
     */
    CheckpointPagesWriter(
            IgniteLogger log,
            CheckpointMetricsTracker tracker,
            IgniteConcurrentMultiPairQueue<PageMemoryImpl, FullPageId> writePageIds,
            ConcurrentMap<PageStore, LongAdder> updStores,
            CompletableFuture<?> doneFut,
            Runnable beforePageWrite,
            ThreadLocal<ByteBuffer> threadBuf,
            CheckpointProgressImpl checkpointProgress,
            CheckpointPageWriter pageWriter,
            BooleanSupplier shutdownNow
    ) {
        this.log = log;
        this.tracker = tracker;
        this.writePageIds = writePageIds;
        this.updStores = updStores;
        this.doneFut = doneFut;
        this.beforePageWrite = beforePageWrite;
        this.threadBuf = threadBuf;
        this.checkpointProgress = checkpointProgress;
        this.pageWriter = pageWriter;
        this.shutdownNow = shutdownNow;
    }

    /** {@inheritDoc} */
    @Override
    public void run() {
        try {
            IgniteConcurrentMultiPairQueue<PageMemoryImpl, FullPageId> pagesToRetry = writePages(writePageIds);

            if (pagesToRetry.isEmpty()) {
                doneFut.complete(null);
            } else {
                if (log.isInfoEnabled()) {
                    log.info(pagesToRetry.initialSize() + " checkpoint pages were not written yet due to "
                            + "unsuccessful page write lock acquisition and will be retried");
                }

                while (!pagesToRetry.isEmpty()) {
                    pagesToRetry = writePages(pagesToRetry);
                }

                doneFut.complete(null);
            }
        } catch (Throwable e) {
            doneFut.completeExceptionally(e);
        }
    }

    /**
     * Writes pages.
     *
     * @param writePageIds Collections of pages to write.
     * @return pagesToRetry Pages which should be retried.
     */
    private IgniteConcurrentMultiPairQueue<PageMemoryImpl, FullPageId> writePages(
            IgniteConcurrentMultiPairQueue<PageMemoryImpl, FullPageId> writePageIds
    ) throws IgniteInternalCheckedException {
        Map<PageMemoryImpl, List<FullPageId>> pagesToRetry = new HashMap<>();

        Map<PageMemoryImpl, PageStoreWriter> pageStoreWriters = new HashMap<>();

        ByteBuffer tmpWriteBuf = threadBuf.get();

        IgniteConcurrentMultiPairQueue.Result<PageMemoryImpl, FullPageId> res = new IgniteConcurrentMultiPairQueue.Result<>();

        while (!shutdownNow.getAsBoolean() && writePageIds.next(res)) {
            beforePageWrite.run();

            FullPageId fullId = res.getValue();

            PageMemoryImpl pageMemory = res.getKey();

            tmpWriteBuf.rewind();

            PageStoreWriter pageStoreWriter = pageStoreWriters.computeIfAbsent(pageMemory, pm -> createPageStoreWriter(pm, pagesToRetry));

            pageMemory.checkpointWritePage(fullId, tmpWriteBuf, pageStoreWriter, tracker);
        }

        return pagesToRetry.isEmpty() ? EMPTY : new IgniteConcurrentMultiPairQueue<>(pagesToRetry);
    }

    /**
     * Returns a new instance of {@link PageStoreWriter}.
     *
     * @param pageMemory Page memory.
     * @param pagesToRetry Pages that need to be rewritten.
     */
    private PageStoreWriter createPageStoreWriter(
            PageMemoryImpl pageMemory,
            Map<PageMemoryImpl, List<FullPageId>> pagesToRetry
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

                if (flag(pageId) == FLAG_DATA) {
                    tracker.onDataPageWritten();
                }

                checkpointProgress.writtenPagesCounter().incrementAndGet();

                PageStore store = pageWriter.write(fullPageId, buf, tag);

                updStores.computeIfAbsent(store, k -> new LongAdder()).increment();
            }
        };
    }
}
