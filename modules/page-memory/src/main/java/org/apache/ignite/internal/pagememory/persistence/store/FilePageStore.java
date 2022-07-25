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

package org.apache.ignite.internal.pagememory.persistence.store;

import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageIndex;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * FilePageStore is a {@link PageStore} implementation that uses regular files to store pages.
 *
 * <p>It consists of the main file page store and delta file page stores, when reading the page at the beginning, the page is searched in
 * the delta files and only then in the main file.
 *
 * <p>On a physical level each instance of {@code FilePageStore} corresponds to a partition file assigned to the local node.
 *
 * <p>Actual read and write operations are performed with {@link FilePageStoreIo} and {@link DeltaFilePageStoreIo}.
 *
 * <p>To create a delta file first invoke {@link #getOrCreateNewDeltaFile(Supplier)} then fill it and then invoke {@link
 * #completeNewDeltaFile()}.
 */
public class FilePageStore implements PageStore {
    private static final VarHandle PAGE_COUNT;

    private static final VarHandle NEW_DELTA_FILE_PAGE_STORE_IO_FUTURE;

    static {
        try {
            PAGE_COUNT = MethodHandles.lookup().findVarHandle(FilePageStore.class, "pageCount", int.class);

            NEW_DELTA_FILE_PAGE_STORE_IO_FUTURE = MethodHandles.lookup().findVarHandle(
                    FilePageStore.class,
                    "newDeltaFilePageStoreIoFuture",
                    CompletableFuture.class
            );
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** File page store version. */
    public static final int VERSION_1 = 1;

    /** Delta file page store IO version. */
    public static final int DELTA_FILE_VERSION_1 = 1;

    /** File page store IO. */
    private final FilePageStoreIo filePageStoreIo;

    /** Page count. */
    private volatile int pageCount;

    /** New page allocation listener. */
    private volatile @Nullable PageAllocationListener pageAllocationListener;

    /** Delta file page store IOs. */
    private final List<DeltaFilePageStoreIo> deltaFilePageStoreIos;

    /** Future with a new delta file page store. */
    private volatile @Nullable CompletableFuture<DeltaFilePageStoreIo> newDeltaFilePageStoreIoFuture;

    /** {@link DeltaFilePageStoreIo} factory. */
    private volatile @Nullable DeltaFilePageStoreIoFactory deltaFilePageStoreIoFactory;

    /** Callback on completion of delta file page store creation. */
    private volatile @Nullable CompleteCreationDeltaFilePageStoreIoCallback completeCreationDeltaFilePageStoreIoCallback;

    /**
     * Constructor.
     *
     * @param filePageStoreIo File page store IO.
     * @param deltaFilePageStoreIos Delta file page store IOs.
     */
    public FilePageStore(
            FilePageStoreIo filePageStoreIo,
            DeltaFilePageStoreIo... deltaFilePageStoreIos
    ) {
        if (deltaFilePageStoreIos.length > 0) {
            Arrays.sort(deltaFilePageStoreIos, Comparator.comparingInt(DeltaFilePageStoreIo::fileIndex).reversed());
        }

        this.filePageStoreIo = filePageStoreIo;
        this.deltaFilePageStoreIos = new CopyOnWriteArrayList<>(Arrays.asList(deltaFilePageStoreIos));
    }

    /** {@inheritDoc} */
    @Override
    public void stop(boolean clean) throws IgniteInternalCheckedException {
        filePageStoreIo.stop(clean);

        for (DeltaFilePageStoreIo deltaFilePageStoreIo : deltaFilePageStoreIos) {
            deltaFilePageStoreIo.stop(clean);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int allocatePage() throws IgniteInternalCheckedException {
        ensure();

        int pageIdx = (int) PAGE_COUNT.getAndAdd(this, 1);

        PageAllocationListener listener = this.pageAllocationListener;

        if (listener != null) {
            listener.onPageAllocated(pageIdx);
        }

        return pageIdx;
    }

    /** {@inheritDoc} */
    @Override
    public int pages() {
        return pageCount;
    }

    /**
     * Sets the page count.
     *
     * @param pageCount New page count.
     */
    public void pages(int pageCount) {
        assert pageCount >= 0 : pageCount;

        this.pageCount = pageCount;
    }

    /**
     * Reads a page, unlike {@link #read(long, ByteBuffer, boolean)}, does not check the {@code pageId} so that its {@code pageIdx} is not
     * greater than the {@link #pages() number of allocated pages}.
     *
     * @param pageId Page ID.
     * @param pageBuf Page buffer to read into.
     * @param keepCrc By default, reading zeroes CRC which was on page store, but you can keep it in {@code pageBuf} if set {@code true}.
     * @throws IgniteInternalCheckedException If reading failed (IO error occurred).
     */
    public void readWithoutPageIdCheck(long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteInternalCheckedException {
        for (DeltaFilePageStoreIo deltaFilePageStoreIo : deltaFilePageStoreIos) {
            long pageOff = deltaFilePageStoreIo.pageOffset(pageId);

            if (pageOff >= 0) {
                deltaFilePageStoreIo.read(pageId, pageOff, pageBuf, keepCrc);

                return;
            }
        }

        filePageStoreIo.read(pageId, filePageStoreIo.pageOffset(pageId), pageBuf, keepCrc);
    }

    /** {@inheritDoc} */
    @Override
    public void read(long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteInternalCheckedException {
        assert pageIndex(pageId) <= pageCount : "pageIdx=" + pageIndex(pageId) + ", pageCount=" + pageCount;

        for (DeltaFilePageStoreIo deltaFilePageStoreIo : deltaFilePageStoreIos) {
            long pageOff = deltaFilePageStoreIo.pageOffset(pageId);

            if (pageOff >= 0) {
                deltaFilePageStoreIo.read(pageId, pageOff, pageBuf, keepCrc);

                return;
            }
        }

        filePageStoreIo.read(pageId, filePageStoreIo.pageOffset(pageId), pageBuf, keepCrc);
    }

    /** {@inheritDoc} */
    @Override
    public void write(long pageId, ByteBuffer pageBuf, boolean calculateCrc) throws IgniteInternalCheckedException {
        assert pageIndex(pageId) <= pageCount : "pageIdx=" + pageIndex(pageId) + ", pageCount=" + pageCount;

        filePageStoreIo.write(pageId, pageBuf, calculateCrc);
    }

    /** {@inheritDoc} */
    @Override
    public void sync() throws IgniteInternalCheckedException {
        filePageStoreIo.sync();
    }

    /** {@inheritDoc} */
    @Override
    public boolean exists() {
        return filePageStoreIo.exists();
    }

    /** {@inheritDoc} */
    @Override
    public void ensure() throws IgniteInternalCheckedException {
        filePageStoreIo.ensure();
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        filePageStoreIo.close();

        for (DeltaFilePageStoreIo deltaFilePageStoreIo : deltaFilePageStoreIos) {
            deltaFilePageStoreIo.close();
        }
    }

    /**
     * Returns size of the page store in bytes.
     *
     * <p>May differ from {@link #pages} * {@link FilePageStoreIo#pageSize()} due to delayed writes or due to other implementation specific
     * details.
     *
     * @throws IgniteInternalCheckedException If an I/O error occurs.
     */
    public long size() throws IgniteInternalCheckedException {
        return filePageStoreIo.size();
    }

    /**
     * Returns file page store path.
     */
    public Path filePath() {
        return filePageStoreIo.filePath();
    }

    /**
     * Returns file page store header size.
     */
    public int headerSize() {
        return filePageStoreIo.headerSize();
    }

    /**
     * Sets the new page allocation listener.
     *
     * @param listener New page allocation listener.
     */
    public void setPageAllocationListener(PageAllocationListener listener) {
        pageAllocationListener = listener;
    }

    /**
     * Sets the delta file page store factory.
     *
     * @param factory Factory.
     */
    public void setDeltaFilePageStoreIoFactory(DeltaFilePageStoreIoFactory factory) {
        deltaFilePageStoreIoFactory = factory;
    }

    /**
     * Sets the callback on completion of delta file page store creation.
     *
     * @param callback Callback.
     */
    public void setCompleteCreationDeltaFilePageStoreIoCallback(CompleteCreationDeltaFilePageStoreIoCallback callback) {
        completeCreationDeltaFilePageStoreIoCallback = callback;
    }

    /**
     * Gets or creates a new delta file, a new delta file will be created when the previous one is {@link #completeNewDeltaFile()
     * completed}.
     *
     * <p>Thread safe.
     *
     * @param pageIndexesSupplier Page indexes supplier for the new delta file page store.
     * @return Future that will be completed when the new delta file page store is created.
     */
    public CompletableFuture<DeltaFilePageStoreIo> getOrCreateNewDeltaFile(Supplier<int[]> pageIndexesSupplier) {
        assert deltaFilePageStoreIoFactory != null;

        CompletableFuture<DeltaFilePageStoreIo> future = this.newDeltaFilePageStoreIoFuture;

        if (future != null) {
            return future;
        }

        if (!NEW_DELTA_FILE_PAGE_STORE_IO_FUTURE.compareAndSet(this, null, future = new CompletableFuture<>())) {
            // Another thread started creating a delta file.
            return newDeltaFilePageStoreIoFuture;
        }

        int nextIndex = deltaFilePageStoreIos.isEmpty() ? 0 : deltaFilePageStoreIos.get(0).fileIndex() + 1;

        DeltaFilePageStoreIo deltaFilePageStoreIo = deltaFilePageStoreIoFactory.create(nextIndex, pageIndexesSupplier.get());

        // Should add to the head, since read operations should always start from the most recent.
        deltaFilePageStoreIos.add(0, deltaFilePageStoreIo);

        future.complete(deltaFilePageStoreIo);

        return future;
    }

    /**
     * Completes the {@link #getOrCreateNewDeltaFile(Supplier) creation} of a new delta file.
     *
     * <p>Thread safe.
     *
     * @throws IgniteInternalCheckedException If failed.
     */
    public void completeNewDeltaFile() throws IgniteInternalCheckedException {
        assert completeCreationDeltaFilePageStoreIoCallback != null;

        CompletableFuture<DeltaFilePageStoreIo> future = this.newDeltaFilePageStoreIoFuture;

        if (future == null) {
            // Already completed in another thread.
            return;
        }

        if (!NEW_DELTA_FILE_PAGE_STORE_IO_FUTURE.compareAndSet(this, future, null)) {
            // Another thread completes the new delta file.
            return;
        }

        completeCreationDeltaFilePageStoreIoCallback.onCompletionOfCreation(future.join());
    }

    /**
     * Returns the number of delta files.
     */
    public int deltaFileCount() {
        return deltaFilePageStoreIos.size();
    }
}
