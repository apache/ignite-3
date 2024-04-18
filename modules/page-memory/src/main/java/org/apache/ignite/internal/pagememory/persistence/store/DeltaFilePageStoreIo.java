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

import static java.nio.ByteOrder.nativeOrder;
import static java.util.Arrays.binarySearch;
import static org.apache.ignite.internal.pagememory.persistence.store.DeltaFilePageStoreIoHeader.checkFileIndex;
import static org.apache.ignite.internal.pagememory.persistence.store.DeltaFilePageStoreIoHeader.checkFilePageIndexes;
import static org.apache.ignite.internal.pagememory.persistence.store.DeltaFilePageStoreIoHeader.readHeader;
import static org.apache.ignite.internal.pagememory.persistence.store.PageStoreUtils.checkFilePageSize;
import static org.apache.ignite.internal.pagememory.persistence.store.PageStoreUtils.checkFileVersion;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageIndex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/**
 * Implementation of the class for working with the delta file page storage IO.
 */
public class DeltaFilePageStoreIo extends AbstractFilePageStoreIo {
    private final DeltaFilePageStoreIoHeader header;

    /** Lock to prevent reads after merging with a file page store. */
    private final IgniteSpinBusyLock mergedBusyLock = new IgniteSpinBusyLock();

    /**
     * Constructor.
     *
     * @param ioFactory {@link FileIo} factory.
     * @param filePath File page store path.
     * @param header Delta file page store header.
     */
    public DeltaFilePageStoreIo(
            FileIoFactory ioFactory,
            Path filePath,
            DeltaFilePageStoreIoHeader header
    ) {
        super(ioFactory, filePath);

        this.header = header;
    }

    /** {@inheritDoc} */
    @Override
    public int pageSize() {
        return header.pageSize();
    }

    /** {@inheritDoc} */
    @Override
    public int headerSize() {
        return header.headerSize();
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer headerBuffer() {
        return header.toByteBuffer();
    }

    /** {@inheritDoc} */
    @Override
    public void checkHeader(FileIo fileIo) throws IOException {
        DeltaFilePageStoreIoHeader header = readHeader(fileIo, ByteBuffer.allocate(pageSize()).order(nativeOrder()));

        if (header == null) {
            throw new IOException("Missing file header");
        }

        checkFileVersion(this.header.version(), header.version());
        checkFileIndex(this.header.index(), header.index());
        checkFilePageSize(this.header.pageSize(), header.pageSize());
        checkFilePageIndexes(this.header.pageIndexes(), header.pageIndexes());
    }

    /**
     * Returns page offset within the store file, {@code -1} if page not found in delta file.
     *
     * @param pageId Page ID.
     */
    @Override
    public long pageOffset(long pageId) {
        return pageOffset(pageIndex(pageId));
    }

    /**
     * Returns page offset within the store file, {@code -1} if page not found in delta file.
     *
     * @param pageIdx Page index.
     */
    public long pageOffset(int pageIdx) {
        int searchResult = binarySearch(header.pageIndexes(), pageIdx);

        if (searchResult < 0) {
            return -1;
        }

        return (long) searchResult * pageSize() + headerSize();
    }

    /**
     * Reads a page.
     *
     * @param pageId Page ID.
     * @param pageBuf Page buffer to read into.
     * @param keepCrc By default, reading zeroes CRC which was on page store, but you can keep it in {@code pageBuf} if set {@code true}.
     * @return {@code True} if the page was successfully read, otherwise the delta file page store is {@link #markMergedToFilePageStore()
     * merged} with the file page store (must be read from file page store).
     * @throws IgniteInternalCheckedException If reading failed (IO error occurred).
     */
    public boolean readWithMergedToFilePageStoreCheck(
            long pageId,
            long pageOff,
            ByteBuffer pageBuf,
            boolean keepCrc
    ) throws IgniteInternalCheckedException {
        if (!mergedBusyLock.enterBusy()) {
            return false;
        }

        try {
            super.read(pageId, pageOff, pageBuf, keepCrc);

            return true;
        } finally {
            mergedBusyLock.leaveBusy();
        }
    }

    /**
     * Returns the index of the delta file page store.
     */
    public int fileIndex() {
        return header.index();
    }

    /**
     * Marks that the delta file page store has been merged with the file page store.
     *
     * <p>It waits for all current {@link #readWithMergedToFilePageStoreCheck(long, long, ByteBuffer, boolean) readings} to end, and
     * subsequent ones will return {@code false} (will need to read from the file page store not from delta file page store).
     */
    public void markMergedToFilePageStore() {
        mergedBusyLock.block();
    }

    /**
     * Returns page indexes of the delta file page store.
     */
    public int[] pageIndexes() {
        return header.pageIndexes();
    }
}
