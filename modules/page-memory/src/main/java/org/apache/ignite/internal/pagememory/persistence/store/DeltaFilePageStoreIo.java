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

import static java.nio.ByteOrder.nativeOrder;
import static java.util.Arrays.binarySearch;
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

/**
 * todo: IGNITE-17372 добавить описание.
 */
class DeltaFilePageStoreIo extends AbstractFilePageStoreIo {
    private final DeltaFilePageStoreIoHeader header;

    /**
     * Constructor.
     *
     * @param ioFactory {@link FileIo} factory.
     * @param filePath File page store path.
     * @param header Delta file page store header.
     */
    DeltaFilePageStoreIo(
            FileIoFactory ioFactory,
            Path filePath,
            DeltaFilePageStoreIoHeader header
    ) {
        super(ioFactory, filePath);

        this.header = header;
    }

    /** {@inheritDoc} */
    @Override
    int pageSize() {
        return header.pageSize();
    }

    /** {@inheritDoc} */
    @Override
    int headerSize() {
        return header.headerSize();
    }

    /** {@inheritDoc} */
    @Override
    ByteBuffer headerBuffer() {
        return header.toByteBuffer();
    }

    /** {@inheritDoc} */
    @Override
    void checkHeader(FileIo fileIo) throws IOException {
        DeltaFilePageStoreIoHeader header = readHeader(fileIo, ByteBuffer.allocate(pageSize()).order(nativeOrder()));

        if (header == null) {
            throw new IOException("Missing file header");
        }

        checkFileVersion(this.header.version(), header.version());
        checkFilePageSize(this.header.pageSize(), header.pageSize());
        checkFilePageIndexes(this.header.pageIndexes(), header.pageIndexes());
    }

    /** {@inheritDoc} */
    @Override
    long pageOffset(long pageId) {
        // TODO: IGNITE-17372 не забыть добавить описание а также учесть логикой в FilePageStore

        int searchResult = binarySearch(header.pageIndexes(), pageIndex(pageId));

        if (searchResult < 0) {
            return searchResult;
        }

        return (long) searchResult * pageSize() + headerSize();
    }
}
