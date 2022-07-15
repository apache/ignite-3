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
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta;
import org.apache.ignite.internal.pagememory.persistence.io.PartitionMetaIo;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Factory for creating {@link PartitionFilePageStore}.
 */
class FilePageStoreFactory {
    /** Latest file page store version. */
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
     * Creates instance of {@link PartitionFilePageStore}.
     *
     * <p>If the partition file exists, it will read its {@link FilePageStoreHeader header} and {@link PartitionMetaIo meta} ({@code
     * pageIdx == 0}) to create the {@link PartitionFilePageStore}.
     *
     * @param filePath File page store path.
     * @param readIntoBuffer Direct byte buffer for reading {@link PartitionMetaIo meta} from {@code filePath}.
     * @return File page store.
     * @throws IgniteInternalCheckedException if failed
     */
    public PartitionFilePageStore createPageStore(
            Path filePath,
            ByteBuffer readIntoBuffer
    ) throws IgniteInternalCheckedException {
        assert readIntoBuffer.remaining() == pageSize : "Expected pageSize=" + pageSize + ", bufferSize=" + readIntoBuffer.remaining();

        if (!Files.exists(filePath)) {
            return createPageStore(filePath, new FilePageStoreHeader(latestVersion, pageSize), newPartitionMeta());
        }

        try (FileIo fileIo = fileIoFactory.create(filePath)) {
            FilePageStoreHeader header = readHeader(fileIo);

            if (header == null) {
                header = new FilePageStoreHeader(latestVersion, pageSize);
            }

            PartitionMeta meta = readPartitionMeta(fileIo, header.headerSize(), readIntoBuffer);

            return createPageStore(filePath, header, meta);
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Error while creating file page store [file=" + filePath + "]:", e);
        }
    }

    private PartitionFilePageStore createPageStore(
            Path filePath,
            FilePageStoreHeader header,
            PartitionMeta partitionMeta
    ) throws IgniteInternalCheckedException {
        if (header.version() == FilePageStore.VERSION_1) {
            return new PartitionFilePageStore(
                    new FilePageStore(
                            header.version(),
                            header.pageSize(),
                            header.headerSize(),
                            partitionMeta.pageCount(),
                            filePath,
                            fileIoFactory
                    ),
                    partitionMeta
            );
        }

        throw new IgniteInternalCheckedException(String.format(
                "Unknown version of file page store [version=%s, file=%s]",
                header.version(),
                filePath
        ));
    }

    private PartitionMeta readPartitionMeta(FileIo fileIo, int headerSize, ByteBuffer readIntoBuffer) throws IOException {
        if (fileIo.size() <= headerSize) {
            return newPartitionMeta();
        }

        try {
            fileIo.readFully(readIntoBuffer, headerSize);

            long pageAddr = bufferAddress(readIntoBuffer);

            if (PartitionMetaIo.getType(pageAddr) <= 0) {
                return newPartitionMeta();
            }

            PartitionMetaIo partitionMetaIo = ioRegistry.resolve(pageAddr);

            return new PartitionMeta(
                    partitionMetaIo.getTreeRootPageId(pageAddr),
                    partitionMetaIo.getReuseListRootPageId(pageAddr),
                    partitionMetaIo.getPageCount(pageAddr)
            );
        } catch (IgniteInternalCheckedException e) {
            throw new IOException("Failed read partition meta", e);
        }
    }

    private static PartitionMeta newPartitionMeta() {
        return new PartitionMeta(0, 0, 1);
    }
}
