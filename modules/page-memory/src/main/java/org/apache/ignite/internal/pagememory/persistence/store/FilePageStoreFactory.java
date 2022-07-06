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
import static org.apache.ignite.internal.util.IgniteUtils.readableSize;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Checks version in files if it's present on the disk, creates store with the latest version otherwise.
 */
class FilePageStoreFactory {
    /** Latest page store version. */
    public final int latestVersion = FilePageStore.VERSION_1;

    /** {@link FileIo} factory. */
    private final FileIoFactory fileIoFactory;

    /** Page size in bytes. */
    private final int pageSize;

    /**
     * Constructor.
     *
     * @param fileIoFactory File IO factory.
     * @param pageSize Page size in bytes.
     */
    public FilePageStoreFactory(
            FileIoFactory fileIoFactory,
            int pageSize
    ) {
        this.fileIoFactory = fileIoFactory;
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
        try (FileIo fileIo = fileIoFactory.create(filePath)) {
            FilePageStoreHeader header = readHeader(fileIo);

            if (header == null) {
                header = new FilePageStoreHeader(latestVersion, pageSize);

                fileIo.writeFully(header.toByteBuffer(), 0);

                fileIo.force();
            } else {
                checkHeader(header);
            }

            return createPageStore(filePath, header);
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Error while creating file page store [file=" + filePath + "]:", e);
        }
    }

    private FilePageStore createPageStore(
            Path filePath,
            FilePageStoreHeader header
    ) throws IgniteInternalCheckedException {
        if (header.version() == FilePageStore.VERSION_1) {
            return new FilePageStore(filePath, fileIoFactory, pageSize);
        }

        throw new IgniteInternalCheckedException(String.format(
                "Unknown version of file page store [version=%s, file=%s]",
                header.version(),
                filePath
        ));
    }

    private void checkHeader(FilePageStoreHeader header) throws IOException {
        if (header.pageSize() != pageSize) {
            throw new IOException(String.format(
                    "Invalid file pageSize [expected=%s, actual=%s]",
                    readableSize(pageSize, false),
                    readableSize(header.pageSize(), false)
            ));
        }
    }
}
