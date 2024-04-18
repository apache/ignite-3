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
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreHeader.readHeader;
import static org.apache.ignite.internal.pagememory.persistence.store.PageStoreUtils.checkFilePageSize;
import static org.apache.ignite.internal.pagememory.persistence.store.PageStoreUtils.checkFileVersion;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;

/**
 * Implementation of the class for working with the file page file storage IO.
 */
public class FilePageStoreIo extends AbstractFilePageStoreIo {
    private final FilePageStoreHeader header;

    /**
     * Constructor.
     *
     * @param ioFactory {@link FileIo} factory.
     * @param filePath File page store path.
     * @param header File page store header.
     */
    public FilePageStoreIo(
            FileIoFactory ioFactory,
            Path filePath,
            FilePageStoreHeader header
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
        FilePageStoreHeader header = readHeader(fileIo, ByteBuffer.allocate(pageSize()).order(nativeOrder()));

        if (header == null) {
            throw new IOException("Missing file header");
        }

        checkFileVersion(this.header.version(), header.version());
        checkFilePageSize(this.header.pageSize(), header.pageSize());
    }

    /** {@inheritDoc} */
    @Override
    public void read(long pageId, long pageOff, ByteBuffer pageBuf, boolean keepCrc) throws IgniteInternalCheckedException {
        super.read(pageId, pageOff, pageBuf, keepCrc);
    }

    /** {@inheritDoc} */
    @Override
    public long pageOffset(long pageId) {
        return (long) PageIdUtils.pageIndex(pageId) * pageSize() + headerSize();
    }
}
