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

package org.apache.ignite.internal.pagememory.persistence.compaction;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toCollection;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.persistence.store.DeltaFilePageStoreIo;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.thread.IgniteThread;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.worker.IgniteWorker;
import org.apache.ignite.internal.util.worker.IgniteWorkerListener;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * Entity to compact delta files.
 *
 * <p>To start compacting delta files, you need to notify about the appearance of {@link #addDeltaFiles(int) delta files ready for
 * compaction}. Then all delta files {@link FilePageStore#getDeltaFileToCompaction() ready for compaction} will be collected and merged with
 * their {@link FilePageStore file page stores} until all delta files are compacted.
 *
 * <p>Delta file compaction process consists of:
 * <ul>
 *  <li>Copying pages from a delta file to a partition file.</li>
 *  <li>Fsync of the partition file.</li>
 *  <li>Remove delta file from {@link FilePageStore} and file system.</li>
 * </ul>
 */
public class Compactor extends IgniteWorker {
    private final Object mux = new Object();

    private final @Nullable ThreadPoolExecutor threadPoolExecutor;

    private final AtomicInteger deltaFileCount = new AtomicInteger();

    private final FilePageStoreManager filePageStoreManager;

    /** Thread local with buffers for the compaction threads. */
    private final ThreadLocal<ByteBuffer> threadBuf;

    /**
     * Creates new ignite worker with given parameters.
     *
     * @param log Logger.
     * @param igniteInstanceName Name of the Ignite instance this runnable is used in.
     * @param listener Listener for life-cycle events.
     * @param threads Number of compaction threads.
     * @param filePageStoreManager File page store manager.
     * @param pageSize Page size in bytes.
     */
    public Compactor(
            IgniteLogger log,
            String igniteInstanceName,
            @Nullable IgniteWorkerListener listener,
            ConfigurationValue<Integer> threads,
            FilePageStoreManager filePageStoreManager,
            int pageSize
    ) {
        super(log, igniteInstanceName, "compaction-thread", listener);

        this.filePageStoreManager = filePageStoreManager;

        threadBuf = ThreadLocal.withInitial(() -> {
            ByteBuffer tmpWriteBuf = ByteBuffer.allocateDirect(pageSize);

            tmpWriteBuf.order(ByteOrder.nativeOrder());

            return tmpWriteBuf;
        });

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
    }

    /** {@inheritDoc} */
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
            // TODO: IGNITE-16899 By analogy with 2.0, we need to handle the exception (err) by the FailureProcessor

            throw new IgniteInternalException(t);
        }
    }

    /**
     * Waiting for delta files.
     */
    void waitDeltaFiles() {
        try {
            synchronized (mux) {
                while (deltaFileCount.get() == 0 && !isCancelled()) {
                    blockingSectionBegin();

                    try {
                        mux.wait();
                    } finally {
                        blockingSectionEnd();
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            isCancelled.set(true);
        }
    }

    /**
     * Adds the number of delta files to compact.
     *
     * @param count Number of delta files.
     */
    public void addDeltaFiles(int count) {
        assert count >= 0;

        if (count > 0) {
            deltaFileCount.addAndGet(count);

            synchronized (mux) {
                mux.notifyAll();
            }
        }
    }

    /**
     * Merges delta files with partition files.
     */
    void doCompaction() {
        // Let's collect one delta file for each partition.
        Queue<IgniteBiTuple<FilePageStore, DeltaFilePageStoreIo>> queue = filePageStoreManager.allPageStores().stream()
                .flatMap(List::stream)
                .map(filePageStore -> {
                    DeltaFilePageStoreIo deltaFileToCompaction = filePageStore.getDeltaFileToCompaction();

                    return deltaFileToCompaction == null ? null : new IgniteBiTuple<>(filePageStore, deltaFileToCompaction);
                })
                .filter(Objects::nonNull)
                .collect(toCollection(ConcurrentLinkedQueue::new));

        assert !queue.isEmpty();

        updateHeartbeat();

        int threads = threadPoolExecutor == null ? 1 : threadPoolExecutor.getMaximumPoolSize();

        CompletableFuture<?>[] futures = new CompletableFuture[threads];

        for (int i = 0; i < threads; i++) {
            CompletableFuture<?> future = futures[i] = new CompletableFuture<>();

            Runnable merger = () -> {
                IgniteBiTuple<FilePageStore, DeltaFilePageStoreIo> toMerge;

                try {
                    while ((toMerge = queue.poll()) != null) {
                        mergeDeltaFileToMainFile(toMerge.get1(), toMerge.get2());
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

    /** {@inheritDoc} */
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
        ByteBuffer buffer = threadBuf.get();

        for (long pageIndex : deltaFilePageStore.pageIndexes()) {
            updateHeartbeat();

            if (isCancelled()) {
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

            filePageStore.write(pageId, buffer.rewind(), true);
        }

        // Fsync the file page store.
        updateHeartbeat();

        if (isCancelled()) {
            return;
        }

        filePageStore.sync();

        // Removing the delta file page store from a file page store.
        updateHeartbeat();

        if (isCancelled()) {
            return;
        }

        boolean removed = filePageStore.removeDeltaFile(deltaFilePageStore);

        assert removed : filePageStore.filePath();

        deltaFilePageStore.markMergedToFilePageStore();

        deltaFilePageStore.stop(true);

        deltaFileCount.decrementAndGet();
    }
}
