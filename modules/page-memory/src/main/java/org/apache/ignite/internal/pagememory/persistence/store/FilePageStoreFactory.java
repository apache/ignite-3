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

import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreHeader.readHeader;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.io.PartitionMetaIo;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Checks version in files if it's present on the disk, creates store with the latest version otherwise.
 */
// TODO: IGNITE-17295 не забудь добавить описание ко всему этому дерьму
class FilePageStoreFactory {
    /** Latest page store version. */
    public final int latestVersion = FilePageStore.VERSION_1;

    private final FileIoFactory fileIoFactory;

    private final int pageSize;

    private PageIoRegistry ioRegistry;

    /**
     * Constructor.
     *
     * @param fileIoFactory File IO factory.
     * @param ioRegistry Page IO registry.
     * @param pageSize Page size in bytes.
     */
    public FilePageStoreFactory(
            FileIoFactory fileIoFactory,
            PageIoRegistry ioRegistry,
            int pageSize
    ) {
        this.fileIoFactory = fileIoFactory;
        this.ioRegistry = ioRegistry;
        this.pageSize = pageSize;
    }

    /**
     * Creates instance of {@link FilePageStore}.
     *
     * @param filePath File page store path.
     * @return File page store.
     * @throws IgniteInternalCheckedException if failed
     */
    public FilePageStore createPageStore(Path filePath) throws IgniteInternalCheckedException {
        if (!Files.exists(filePath)) {
            return createPageStore(filePath, new FilePageStoreHeader(latestVersion, pageSize), 1);
        }

        try (FileIo fileIo = fileIoFactory.create(filePath)) {
            FilePageStoreHeader header = readHeader(fileIo);

            // First page should be PartitionMetaIo.
            int pageCount = 1;

            if (header == null) {
                header = new FilePageStoreHeader(latestVersion, pageSize);
            } else {
                if (fileIo.size() > header.headerSize()) {
                    pageCount = readPageCount(fileIo, header.headerSize());
                }
            }

            return createPageStore(filePath, header, pageCount);
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Error while creating file page store [file=" + filePath + "]:", e);
        }
    }

    private FilePageStore createPageStore(
            Path filePath,
            FilePageStoreHeader header,
            int pageCount
    ) throws IgniteInternalCheckedException {
        if (header.version() == FilePageStore.VERSION_1) {
            return new FilePageStore(header.version(), header.pageSize(), header.headerSize(), pageCount, filePath, fileIoFactory);
        }

        throw new IgniteInternalCheckedException(String.format(
                "Unknown version of file page store [version=%s, file=%s]",
                header.version(),
                filePath
        ));
    }

    private int readPageCount(FileIo fileIo, int headerSize) throws IOException {
        ByteBuffer buffer = allocateBuffer(pageSize);

        try {
            fileIo.readFully(buffer, headerSize);

            PartitionMetaIo partitionMetaIo = ioRegistry.resolve(buffer.rewind());

            return partitionMetaIo.getPageCount(GridUnsafe.bufferAddress(buffer));
        } catch (IgniteInternalCheckedException e) {
            throw new IOException("Failed read partition meta", e);
        } finally {
            freeBuffer(buffer);
        }
    }
}
