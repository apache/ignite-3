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
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * FilePageStore is a {@link PageStore} implementation that uses regular files to store pages.
 *
 * <p>Actual read and write operations are performed with {@link FileIo} abstract interface, list of its implementations is a good source
 * of information about functionality in Ignite Native Persistence.
 *
 * <p>On a physical level each instance of {@code FilePageStore} corresponds to a partition file assigned to the local node.
 *
 * <p>Consists of:
 * <ul>
 *     <li>Header - {@link FilePageStoreHeader}. </li>
 *     <li>Body - data pages are multiples of {@link FilePageStoreHeader#pageSize() pageSize}.</li>
 * </ul>
 */
// TODO: IGNITE-17372 модифицировать описание
// TODO: IGNITE-17372 тут еще надо будет методы поправить
public class FilePageStore implements PageStore {
    /** File page store version. */
    public static final int VERSION_1 = 1;

    /** Delta file page store IO version. */
    public static final int DELTA_FILE_VERSION_1 = 1;

    /** File page store IO. */
    private final FilePageStoreIo filePageStoreIo;

    /** Page count. */
    private final AtomicInteger pageCount = new AtomicInteger();

    /** New page allocation listener. */
    private volatile @Nullable PageAllocationListener pageAllocationListener;

    /** Delta file page store IOs. */
    private final List<DeltaFilePageStoreIo> deltaFilePageStoreIos;

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

        int pageIdx = pageCount.getAndIncrement();

        PageAllocationListener listener = this.pageAllocationListener;

        if (listener != null) {
            listener.onPageAllocated(pageIdx);
        }

        return pageIdx;
    }

    /** {@inheritDoc} */
    @Override
    public int pages() {
        return pageCount.get();
    }

    /**
     * Sets the page count.
     *
     * @param pageCount New page count.
     */
    public void pages(int pageCount) {
        assert pageCount >= 0 : pageCount;

        this.pageCount.set(pageCount);
    }

    /**
     * Reads a page, unlike {@link #read(long, ByteBuffer, boolean)}, checks the page offset in the file not logically (pageOffset <= {@link
     * #pages()} * {@link FilePageStoreIo#pageSize()}) but physically (pageOffset <= {@link #size()}), which can affect performance when
     * used in production code.
     *
     * @param pageId Page ID.
     * @param pageBuf Page buffer to read into.
     * @param keepCrc By default, reading zeroes CRC which was on page store, but you can keep it in {@code pageBuf} if set {@code
     * keepCrc}.
     * @throws IgniteInternalCheckedException If reading failed (IO error occurred).
     */
    public void readByPhysicalOffset(long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteInternalCheckedException {
        // TODO: IGNITE-17372 возможно надо будет переименовать или вроде того
        filePageStoreIo.read(pageId, pageBuf, keepCrc);
    }

    /** {@inheritDoc} */
    @Override
    public void read(long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteInternalCheckedException {
        assert pageIndex(pageId) <= pageCount.get() : "pageIdx=" + pageIndex(pageId) + ", pageCount=" + pageCount.get();

        filePageStoreIo.read(pageId, pageBuf, keepCrc);
    }

    /** {@inheritDoc} */
    @Override
    public void write(long pageId, ByteBuffer pageBuf, boolean calculateCrc) throws IgniteInternalCheckedException {
        assert pageIndex(pageId) <= pageCount.get() : "pageIdx=" + pageIndex(pageId) + ", pageCount=" + pageCount.get();

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
}
