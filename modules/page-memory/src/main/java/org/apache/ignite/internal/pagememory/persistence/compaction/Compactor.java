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

package org.apache.ignite.internal.pagememory.persistence.compaction;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toCollection;
import static org.apache.ignite.internal.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionProcessingCounterMap;
import org.apache.ignite.internal.pagememory.persistence.store.DeltaFilePageStoreIo;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.pagememory.persistence.store.GroupPageStoresMap.GroupPartitionPageStore;
import org.apache.ignite.internal.thread.IgniteThread;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.worker.IgniteWorker;
import org.apache.ignite.internal.util.worker.IgniteWorkerListener;
import org.jetbrains.annotations.Nullable;

/**
 * Entity to compact delta files.
 *
 * <p>When delta files appear, {@link #triggerCompaction()} must be called to initiate compaction. Then all delta files
 * {@link FilePageStore#getDeltaFileToCompaction() ready for compaction} will be collected and merged with their
 * {@link FilePageStore file page stores} until all delta files are compacted.
 *
 * <p>Delta file compaction process consists of:
 * <ul>
 *  <li>Copying pages from a delta file to a partition file.</li>
 *  <li>Fsync of the partition file.</li>
 *  <li>Remove delta file from {@link FilePageStore} and file system.</li>
 * </ul>
 */
public class Compactor extends IgniteWorker {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(Compactor.class);

    private final Object mux = new Object();

    private final @Nullable ThreadPoolExecutor threadPoolExecutor;

    /** Guarded by {@link #mux}. */
    private boolean addedDeltaFiles;

    private final FilePageStoreManager filePageStoreManager;

    /** Thread local with buffers for the compaction threads. */
    private static final ThreadLocal<ByteBuffer> THREAD_BUF = new ThreadLocal<>();

    /** Partitions for which delta files are currently compacted. */
    private final PartitionProcessingCounterMap partitionCompactionInProgressMap = new PartitionProcessingCounterMap();

    /** Page size in bytes. */
    private final int pageSize;

    /** Failure processor. */
    private final FailureProcessor failureProcessor;

    /**
     * Creates new ignite worker with given parameters.
     *
     * @param log Logger.
     * @param igniteInstanceName Name of the Ignite instance this runnable is used in.
     * @param listener Listener for life-cycle events.
     * @param threads Number of compaction threads.
     * @param filePageStoreManager File page store manager.
     * @param pageSize Page size in bytes.
     * @param failureProcessor Failure processor that is used to handle critical errors.
     */
    public Compactor(
            IgniteLogger log,
            String igniteInstanceName,
            @Nullable IgniteWorkerListener listener,
            ConfigurationValue<Integer> threads,
            FilePageStoreManager filePageStoreManager,
            int pageSize,
            FailureProcessor failureProcessor
    ) {
        super(log, igniteInstanceName, "compaction-thread", listener);

        this.filePageStoreManager = filePageStoreManager;
        this.failureProcessor = failureProcessor;

        int threadCount = threads.value();

        if (threadCount > 1) {
            threadPoolExecutor = new ThreadPoolExecutor(
                    threadCount,
                    threadCount,
                    30_000,
                    MILLISECONDS,
                    new LinkedBlockingQueue<>(),
                    new NamedThreadFactory("compaction-runner-io", log)
            );
        } else {
            threadPoolExecutor = null;
        }

        this.pageSize = pageSize;
    }

    @Override
    protected void body() throws InterruptedException {
        try {
            while (!isCancelled()) {
                waitDeltaFiles();

                if (isCancelled()) {
                    log.info("Skipping the delta file compaction because the node is stopping");

                    return;
                }

                doCompaction();
            }
        } catch (Throwable t) {
            failureProcessor.process(new FailureContext(SYSTEM_WORKER_TERMINATION, t));

            throw new IgniteInternalException(t);
        }
    }

    /**
     * Waiting for delta files.
     *
     * <p>It is expected that only the compactor will call it.
     */
    void waitDeltaFiles() {
        try {
            synchronized (mux) {
                while (!addedDeltaFiles && !isCancelled()) {
                    blockingSectionBegin();

                    try {
                        mux.wait();
                    } finally {
                        blockingSectionEnd();
                    }
                }

                addedDeltaFiles = false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            isCancelled.set(true);
        }
    }

    /**
     * Triggers compacting for new delta files.
     */
    public void triggerCompaction() {
        synchronized (mux) {
            addedDeltaFiles = true;

            mux.notifyAll();
        }
    }

    /**
     * Merges delta files with partition files.
     *
     * <p>Only compactor is expected to call this method. When compaction is {@link #triggerCompaction() triggered} by other threads, we
     * need to compact all delta files for all partitions as long as the delta files exist. Delta files are compacted in batches (one for
     * each partition file) into several threads, which evenly reduces the load for all partition files on reading pages, since when reading
     * pages, we must look for it from the oldest delta file.
     */
    void doCompaction() {
        while (true) {
            // Let's collect one delta file for each partition.
            Queue<DeltaFileForCompaction> queue = filePageStoreManager.allPageStores()
                    .map(groupPartitionFilePageStore -> {
                        DeltaFilePageStoreIo deltaFileToCompaction = groupPartitionFilePageStore.pageStore().getDeltaFileToCompaction();

                        if (deltaFileToCompaction == null) {
                            return null;
                        }

                        return new DeltaFileForCompaction(groupPartitionFilePageStore, deltaFileToCompaction);
                    })
                    .filter(Objects::nonNull)
                    .collect(toCollection(ConcurrentLinkedQueue::new));

            if (queue.isEmpty()) {
                break;
            }

            // TODO Expand the comment. https://issues.apache.org/jira/browse/IGNITE-23056
            LOG.info("Starting new compaction round [files={}]", queue.size());

            long start = System.nanoTime();

            updateHeartbeat();

            int threads = threadPoolExecutor == null ? 1 : threadPoolExecutor.getMaximumPoolSize();

            CompletableFuture<?>[] futures = new CompletableFuture[threads];

            for (int i = 0; i < threads; i++) {
                CompletableFuture<?> future = futures[i] = new CompletableFuture<>();

                Runnable merger = () -> {
                    DeltaFileForCompaction toMerge;

                    try {
                        while (true) {
                            toMerge = queue.poll();

                            if (toMerge == null) {
                                break;
                            }

                            GroupPartitionId groupPartitionId = toMerge.groupPartitionFilePageStore.groupPartitionId();

                            partitionCompactionInProgressMap.incrementPartitionProcessingCounter(groupPartitionId);

                            try {
                                mergeDeltaFileToMainFile(toMerge.groupPartitionFilePageStore.pageStore(), toMerge.deltaFilePageStoreIo);
                            } finally {
                                partitionCompactionInProgressMap.decrementPartitionProcessingCounter(groupPartitionId);
                            }
                        }
                    } catch (Throwable ex) {
                        future.completeExceptionally(ex);
                    }

                    future.complete(null);
                };

                if (isCancelled()) {
                    return;
                }

                if (threadPoolExecutor == null) {
                    merger.run();
                } else {
                    threadPoolExecutor.execute(merger);
                }
            }

            updateHeartbeat();

            // Wait and check for errors.
            CompletableFuture.allOf(futures).join();

            // TODO Expand the comment. https://issues.apache.org/jira/browse/IGNITE-23056, handle an exception too.
            LOG.info("Compaction round finished [duration={}ms]", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        }
    }

    /**
     * Starts the compacter.
     */
    public void start() {
        if (runner() != null) {
            return;
        }

        assert runner() == null : "Compacter is running";

        new IgniteThread(this).start();
    }

    /**
     * Stops the compacter.
     */
    public void stop() throws Exception {
        cancel();

        boolean interrupt = false;

        while (true) {
            try {
                join();

                break;
            } catch (InterruptedException e) {
                interrupt = true;
            }
        }

        if (interrupt) {
            Thread.currentThread().interrupt();
        }

        if (threadPoolExecutor != null) {
            IgniteUtils.shutdownAndAwaitTermination(threadPoolExecutor, 2, TimeUnit.MINUTES);
        }
    }

    @Override
    public void cancel() {
        if (log.isDebugEnabled()) {
            log.debug("Cancelling grid runnable: " + this);
        }

        // Do not interrupt runner thread.
        isCancelled.set(true);

        synchronized (mux) {
            mux.notifyAll();
        }
    }

    /**
     * Merges the main file page store with the delta file page store.
     *
     * <p>Steps:
     * <ul>
     *  <li>Copy pages from delta file page store to file page store.</li>
     *  <li>Fsync the file page store.</li>
     *  <li>Removing the delta file page store from a file page store.</li>
     * </ul>
     *
     * @param filePageStore File page store.
     * @param deltaFilePageStore Delta file page store.
     * @throws Throwable If failed.
     */
    void mergeDeltaFileToMainFile(
            FilePageStore filePageStore,
            DeltaFilePageStoreIo deltaFilePageStore
    ) throws Throwable {
        // Copy pages deltaFilePageStore -> filePageStore.
        ByteBuffer buffer = getThreadLocalBuffer(pageSize);

        for (long pageIndex : deltaFilePageStore.pageIndexes()) {
            updateHeartbeat();

            if (isCancelled()) {
                return;
            }

            if (filePageStore.isMarkedToDestroy()) {
                return;
            }

            long pageOffset = deltaFilePageStore.pageOffset(pageIndex);

            // pageIndex instead of pageId, only for debugging in case of errors
            // since we do not know the pageId until we read it from the pageOffset.
            boolean read = deltaFilePageStore.readWithMergedToFilePageStoreCheck(pageIndex, pageOffset, buffer.rewind(), false);

            assert read : deltaFilePageStore.filePath();

            long pageId = PageIo.getPageId(buffer.rewind());

            assert pageId != 0 : deltaFilePageStore.filePath();

            updateHeartbeat();

            if (isCancelled()) {
                return;
            }

            if (filePageStore.isMarkedToDestroy()) {
                return;
            }

            filePageStore.write(pageId, buffer.rewind(), true);
        }

        // Fsync the file page store.
        updateHeartbeat();

        if (isCancelled()) {
            return;
        }

        if (filePageStore.isMarkedToDestroy()) {
            return;
        }

        filePageStore.sync();

        // Removing the delta file page store from a file page store.
        updateHeartbeat();

        if (isCancelled()) {
            return;
        }

        if (filePageStore.isMarkedToDestroy()) {
            return;
        }

        deltaFilePageStore.markMergedToFilePageStore();

        deltaFilePageStore.stop(true);

        boolean removed = filePageStore.removeDeltaFile(deltaFilePageStore);

        assert removed : filePageStore.filePath();
    }

    /**
     * Prepares the compactor to destroy a partition.
     *
     * <p>If the partition compaction is in progress, then we will wait until it is completed so that there are no errors when we want to
     * destroy the partition file and its delta file, and at this time its compaction occurs.
     *
     * @param groupPartitionId Pair of group ID with partition ID.
     * @return Future at the complete of which we can delete the partition file and its delta files.
     */
    public CompletableFuture<Void> prepareToDestroyPartition(GroupPartitionId groupPartitionId) {
        CompletableFuture<Void> partitionProcessingFuture = partitionCompactionInProgressMap.getProcessedPartitionFuture(groupPartitionId);

        return partitionProcessingFuture == null ? nullCompletedFuture() : partitionProcessingFuture;
    }

    private static ByteBuffer getThreadLocalBuffer(int pageSize) {
        ByteBuffer buffer = THREAD_BUF.get();

        if (buffer == null) {
            buffer = ByteBuffer.allocateDirect(pageSize);

            buffer.order(ByteOrder.nativeOrder());

            THREAD_BUF.set(buffer);
        }

        return buffer;
    }

    /**
     * Delta file for compaction.
     */
    private static class DeltaFileForCompaction {
        private final GroupPartitionPageStore<FilePageStore> groupPartitionFilePageStore;

        private final DeltaFilePageStoreIo deltaFilePageStoreIo;

        private DeltaFileForCompaction(
                GroupPartitionPageStore<FilePageStore> groupPartitionFilePageStore,
                DeltaFilePageStoreIo deltaFilePageStoreIo
        ) {
            this.groupPartitionFilePageStore = groupPartitionFilePageStore;
            this.deltaFilePageStoreIo = deltaFilePageStoreIo;
        }
    }
}
