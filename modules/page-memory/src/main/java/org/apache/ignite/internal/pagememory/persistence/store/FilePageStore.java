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

package org.apache.ignite.internal.pagememory.persistence.store;

import static java.util.Collections.unmodifiableList;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageIndex;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
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
 * <p>To create a delta file first invoke {@link #getOrCreateNewDeltaFile(IntFunction, Supplier)} then fill it and then invoke {@link
 * #completeNewDeltaFile()}.
 */
public class FilePageStore implements PageStore {
    private static final VarHandle PAGE_COUNT;

    private static final VarHandle NEW_DELTA_FILE_PAGE_STORE_IO_FUTURE;

    private static final VarHandle DELTA_FILE_PAGE_STORE_IOS;

    static {
        try {
            PAGE_COUNT = MethodHandles.lookup().findVarHandle(FilePageStore.class, "pageCount", int.class);

            NEW_DELTA_FILE_PAGE_STORE_IO_FUTURE = MethodHandles.lookup().findVarHandle(
                    FilePageStore.class,
                    "newDeltaFilePageStoreIoFuture",
                    CompletableFuture.class
            );

            DELTA_FILE_PAGE_STORE_IOS = MethodHandles.lookup().findVarHandle(FilePageStore.class, "deltaFilePageStoreIos", List.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** File page store version. */
    public static final int VERSION_1 = 1;

    /** Delta file page store IO version. */
    public static final int DELTA_FILE_VERSION_1 = 1;

    /** Latest file page store version. */
    public static final int LATEST_FILE_PAGE_STORE_VERSION = VERSION_1;

    /** Latest delta file page store IO version. */
    public static final int LATEST_DELTA_FILE_PAGE_STORE_VERSION = DELTA_FILE_VERSION_1;

    /** File page store IO. */
    private final FilePageStoreIo filePageStoreIo;

    /** Page count. */
    private volatile int pageCount;

    /** Number of pages persisted to the disk during the last checkpoint. */
    private volatile int checkpointedPageCount;

    /** New page allocation listener. */
    private volatile @Nullable PageAllocationListener pageAllocationListener;

    /** Delta file page store IOs. Copy-on-write list. */
    private volatile List<DeltaFilePageStoreIo> deltaFilePageStoreIos;

    /** Future with a new delta file page store. */
    private volatile @Nullable CompletableFuture<DeltaFilePageStoreIo> newDeltaFilePageStoreIoFuture;

    /** Flag that the file and its delta files will be destroyed. */
    private volatile boolean toDestroy;

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
        this.deltaFilePageStoreIos = Arrays.asList(deltaFilePageStoreIos);
    }

    @Override
    public void stop(boolean clean) throws IgniteInternalCheckedException {
        filePageStoreIo.stop(clean);

        for (DeltaFilePageStoreIo deltaFilePageStoreIo : deltaFilePageStoreIos) {
            deltaFilePageStoreIo.stop(clean);
        }
    }

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

    @Override
    public int pages() {
        return pageCount;
    }

    /**
     * Initializes the page count.
     *
     * @param pageCount New page count.
     */
    public void pages(int pageCount) {
        assert pageCount >= 0 : pageCount;

        this.pageCount = pageCount;
        this.checkpointedPageCount = pageCount;
    }

    /** Returns number of pages that were successfully checkpointed. */
    public int checkpointedPageCount() {
        return checkpointedPageCount;
    }

    /**
     * Sets number of pages that were successfully checkpointed.
     *
     * @param checkpointedPageCount New checkpointed page count.
     */
    public void checkpointedPageCount(int checkpointedPageCount) {
        assert pageCount >= 0 : pageCount;

        this.checkpointedPageCount = checkpointedPageCount;
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
                if (deltaFilePageStoreIo.readWithMergedToFilePageStoreCheck(pageId, pageOff, pageBuf, keepCrc)) {
                    return;
                }
            }
        }

        filePageStoreIo.read(pageId, filePageStoreIo.pageOffset(pageId), pageBuf, keepCrc);
    }

    @Override
    public void read(long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteInternalCheckedException {
        assert pageIndex(pageId) <= pageCount : "pageIdx=" + pageIndex(pageId) + ", pageCount=" + pageCount;

        readWithoutPageIdCheck(pageId, pageBuf, keepCrc);
    }

    @Override
    public void write(long pageId, ByteBuffer pageBuf) throws IgniteInternalCheckedException {
        assert pageIndex(pageId) <= pageCount : "pageIdx=" + pageIndex(pageId) + ", pageCount=" + pageCount;

        filePageStoreIo.write(pageId, pageBuf);
    }

    @Override
    public void sync() throws IgniteInternalCheckedException {
        filePageStoreIo.sync();
    }

    @Override
    public boolean exists() {
        return filePageStoreIo.exists();
    }

    @Override
    public void ensure() throws IgniteInternalCheckedException {
        filePageStoreIo.ensure();
    }

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
     * Returns full size of the page store and its delta files in bytes.
     *
     * <p>May differ from {@link #pages} * {@link FilePageStoreIo#pageSize()} due to delayed writes or due to other implementation specific
     * details.
     *
     * @throws IgniteInternalCheckedException If an I/O error occurs.
     */
    public long fullSize() throws IgniteInternalCheckedException {
        long size = filePageStoreIo.size();

        for (DeltaFilePageStoreIo deltaFilePageStoreIo : deltaFilePageStoreIos) {
            size += deltaFilePageStoreIo.size();
        }

        return size;
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
     * Gets or creates a new delta file, a new delta file will be created when the previous one is {@link #completeNewDeltaFile()
     * completed}.
     *
     * <p>Thread safe.
     *
     * @param deltaFilePathFunction Function to get the path to the delta file page store, the argument is the index of the delta file.
     * @param pageIndexesSupplier Page indexes supplier that will only be called if the current thread creates a delta file page store.
     * @return Future that will be completed when the new delta file page store is created.
     */
    public CompletableFuture<DeltaFilePageStoreIo> getOrCreateNewDeltaFile(
            IntFunction<Path> deltaFilePathFunction,
            Supplier<int[]> pageIndexesSupplier
    ) {
        CompletableFuture<DeltaFilePageStoreIo> future = this.newDeltaFilePageStoreIoFuture;

        if (future != null) {
            return future;
        }

        if (!NEW_DELTA_FILE_PAGE_STORE_IO_FUTURE.compareAndSet(this, null, future = new CompletableFuture<>())) {
            // Another thread started creating a delta file.
            return newDeltaFilePageStoreIoFuture;
        }

        DeltaFilePageStoreIo newDeltaFilePageStoreIo = addNewDeltaFilePageStoreIoToHead(pageIndexesSupplier.get(), deltaFilePathFunction);

        future.complete(newDeltaFilePageStoreIo);

        return future;
    }

    private DeltaFilePageStoreIo addNewDeltaFilePageStoreIoToHead(int[] pageIndexes, IntFunction<Path> deltaFilePathFunction) {
        List<DeltaFilePageStoreIo> previousValue;
        List<DeltaFilePageStoreIo> newValue;

        DeltaFilePageStoreIo newDeltaFilePageStoreIo;

        do {
            previousValue = deltaFilePageStoreIos;

            int nextIndex = previousValue.isEmpty() ? 0 : previousValue.get(0).fileIndex() + 1;

            var header = new DeltaFilePageStoreIoHeader(
                    LATEST_DELTA_FILE_PAGE_STORE_VERSION,
                    nextIndex,
                    filePageStoreIo.pageSize(),
                    pageIndexes
            );

            newDeltaFilePageStoreIo = new DeltaFilePageStoreIo(
                    filePageStoreIo.ioFactory,
                    deltaFilePathFunction.apply(nextIndex),
                    header
            );

            newValue = new ArrayList<>(previousValue.size() + 1);

            // Should add to the head, since read operations should always start from the most recent.
            newValue.add(newDeltaFilePageStoreIo);
            newValue.addAll(previousValue);

            newValue = unmodifiableList(newValue);
        } while (!DELTA_FILE_PAGE_STORE_IOS.compareAndSet(this, previousValue, newValue));

        return newDeltaFilePageStoreIo;
    }

    /**
     * Returns the new delta file store that was created in {@link #getOrCreateNewDeltaFile(IntFunction, Supplier)} and not yet completed by
     * {@link #completeNewDeltaFile()}.
     */
    public @Nullable CompletableFuture<DeltaFilePageStoreIo> getNewDeltaFile() {
        return newDeltaFilePageStoreIoFuture;
    }

    /**
     * Completes the {@link #getOrCreateNewDeltaFile(IntFunction, Supplier) creation} of a new delta file.
     *
     * <p>Thread safe.
     */
    public void completeNewDeltaFile() {
        CompletableFuture<DeltaFilePageStoreIo> future = this.newDeltaFilePageStoreIoFuture;

        if (future == null) {
            // Already completed in another thread.
            return;
        }

        NEW_DELTA_FILE_PAGE_STORE_IO_FUTURE.compareAndSet(this, future, null);
    }

    /**
     * Returns the number of delta files.
     */
    public int deltaFileCount() {
        return deltaFilePageStoreIos.size();
    }

    /**
     * Returns the delta file to compaction (oldest), {@code null} if there is nothing to compact.
     *
     * <p>Thread safe.
     */
    public @Nullable DeltaFilePageStoreIo getDeltaFileToCompaction() {
        // Snapshot of delta files.
        List<DeltaFilePageStoreIo> deltaFilePageStoreIos = this.deltaFilePageStoreIos;

        // If there are no delta files or if last is just created, then it cannot be compacted yet.
        if (deltaFilePageStoreIos.isEmpty() || (deltaFilePageStoreIos.size() == 1 && newDeltaFilePageStoreIoFuture != null)) {
            return null;
        }

        return deltaFilePageStoreIos.get(deltaFilePageStoreIos.size() - 1);
    }

    /**
     * Deletes delta file.
     *
     * <p>Thread safe.</p>
     *
     * @param deltaFilePageStoreIo Delta file to be deleted.
     * @return {@code True} if the delta file being removed was present.
     */
    public boolean removeDeltaFile(DeltaFilePageStoreIo deltaFilePageStoreIo) {
        List<DeltaFilePageStoreIo> previousValue;
        List<DeltaFilePageStoreIo> newValue;

        boolean removed;

        do {
            previousValue = deltaFilePageStoreIos;

            newValue = new ArrayList<>(previousValue);

            removed = newValue.remove(deltaFilePageStoreIo);

            newValue = unmodifiableList(newValue);
        } while (!DELTA_FILE_PAGE_STORE_IOS.compareAndSet(this, previousValue, newValue));

        return removed;
    }

    /**
     * Marks that the file page store and its delta files will be destroyed.
     */
    public void markToDestroy() {
        toDestroy = true;
    }

    /**
     * Checks if the file page store and its delta files are marked for destruction.
     */
    public boolean isMarkedToDestroy() {
        return toDestroy;
    }
}
