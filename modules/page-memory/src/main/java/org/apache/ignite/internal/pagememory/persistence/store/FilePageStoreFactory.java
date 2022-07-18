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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Factory for creating {@link FilePageStore}.
 */
class FilePageStoreFactory {
    /** Latest file page store version. */
    private final int latestVersion = FilePageStore.VERSION_1;

    private final FileIoFactory fileIoFactory;

    private final int pageSize;

    /**
     * Constructor.
     *
     * @param fileIoFactory File IO factory.
     * @param pageSize Page size in bytes.
     */
    public FilePageStoreFactory(FileIoFactory fileIoFactory, int pageSize) {
        this.fileIoFactory = fileIoFactory;
        this.pageSize = pageSize;
    }

    /**
     * Creates instance of {@link FilePageStore}.
     *
     * <p>If the file exists, an attempt will be made to read its {@link FilePageStoreHeader header} and create the {@link FilePageStore}.
     *
     * @param filePath File page store path.
     * @param headerBuffer Buffer for reading {@link FilePageStoreHeader header} from {@code filePath}.
     * @return File page store.
     * @throws IgniteInternalCheckedException if failed
     */
    public FilePageStore createPageStore(Path filePath, ByteBuffer headerBuffer) throws IgniteInternalCheckedException {
        assert headerBuffer.remaining() == pageSize : headerBuffer.remaining();

        if (!Files.exists(filePath)) {
            return createPageStore(filePath, new FilePageStoreHeader(latestVersion, pageSize));
        }

        try (FileIo fileIo = fileIoFactory.create(filePath)) {
            FilePageStoreHeader header = readHeader(fileIo, headerBuffer);

            if (header == null) {
                header = new FilePageStoreHeader(latestVersion, pageSize);
            }

            return createPageStore(filePath, header);
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Error while creating file page store [file=" + filePath + "]", e);
        }
    }

    private FilePageStore createPageStore(
            Path filePath,
            FilePageStoreHeader header
    ) throws IgniteInternalCheckedException {
        if (header.version() == FilePageStore.VERSION_1) {
            return new FilePageStore(
                    header.version(),
                    header.pageSize(),
                    header.headerSize(),
                    filePath,
                    fileIoFactory
            );
        }

        throw new IgniteInternalCheckedException(String.format(
                "Unknown version of file page store [version=%s, file=%s]",
                header.version(),
                filePath
        ));
    }
}
