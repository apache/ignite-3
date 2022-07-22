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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Factory for creating {@link FilePageStore}.
 */
class FilePageStoreFactory {
    /** Latest file page store version. */
    private final int latestFilePageStoreVersion = FilePageStore.VERSION_1;

    /** Latest delta file page store IO version. */
    private final int latestDeltaFileVersion = FilePageStore.DELTA_FILE_VERSION_1;

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
     * <p>Page stores are created based on their headers, for a file page stores with no header, the latest version is generated for delta
     * file page store files, headers must be present.
     *
     * @param headerBuffer Buffer for reading headers.
     * @param filePageStorePath File page store path.
     * @param deltaFilePaths Paths to existing delta files page stores of the file page storage.
     * @throws IgniteInternalCheckedException if failed
     */
    public FilePageStore createPageStore(
            ByteBuffer headerBuffer,
            Path filePageStorePath,
            Path... deltaFilePaths
    ) throws IgniteInternalCheckedException {
        assert headerBuffer.remaining() == pageSize : headerBuffer.remaining();

        if (!Files.exists(filePageStorePath)) {
            assert deltaFilePaths.length == 0 : Arrays.toString(deltaFilePaths);

            return createFilePageStore(filePageStorePath, new FilePageStoreHeader(latestFilePageStoreVersion, pageSize));
        }

        try (FileIo fileIo = fileIoFactory.create(filePageStorePath)) {
            FilePageStoreHeader header = FilePageStoreHeader.readHeader(fileIo, headerBuffer);

            if (header == null) {
                header = new FilePageStoreHeader(latestFilePageStoreVersion, pageSize);
            }

            if (deltaFilePaths.length == 0) {
                return createFilePageStore(filePageStorePath, header);
            }

            DeltaFilePageStoreIo[] deltaFileIos = new DeltaFilePageStoreIo[deltaFilePaths.length];

            for (int i = 0; i < deltaFilePaths.length; i++) {
                Path deltaFilePath = deltaFilePaths[i];

                assert Files.exists(deltaFilePath) : deltaFilePath;

                try (FileIo deltaFileIo = fileIoFactory.create(deltaFilePath)) {
                    DeltaFilePageStoreIoHeader deltaFileHeader = DeltaFilePageStoreIoHeader.readHeader(deltaFileIo, headerBuffer.rewind());

                    assert deltaFileHeader != null : deltaFileHeader;

                    deltaFileIos[i] = createDeltaFilePageStoreIo(deltaFilePath, deltaFileHeader);
                }
            }

            return createFilePageStore(filePageStorePath, header, deltaFileIos);
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Error while creating file page store [file=" + filePageStorePath + "]", e);
        }
    }

    private FilePageStore createFilePageStore(
            Path filePath,
            FilePageStoreHeader header,
            DeltaFilePageStoreIo... deltaFileIos
    ) throws IgniteInternalCheckedException {
        if (header.version() == FilePageStore.VERSION_1) {
            return new FilePageStore(new FilePageStoreIo(fileIoFactory, filePath, header), deltaFileIos);
        }

        throw new IgniteInternalCheckedException(String.format(
                "Unknown version of file page store [version=%s, file=%s]",
                header.version(),
                filePath
        ));
    }

    private DeltaFilePageStoreIo createDeltaFilePageStoreIo(
            Path filePath,
            DeltaFilePageStoreIoHeader header
    ) throws IgniteInternalCheckedException {
        if (header.version() == FilePageStore.DELTA_FILE_VERSION_1) {
            return new DeltaFilePageStoreIo(fileIoFactory, filePath, header);
        }

        throw new IgniteInternalCheckedException(String.format(
                "Unknown version of delta file page store [version=%s, file=%s]",
                header.version(),
                filePath
        ));
    }

    /**
     * Creates a delta file page store of the latest version.
     *
     * @param filePath Path to the delta file page store.
     * @param index Delta file page store index.
     * @param pageIndexes Page indexes.
     */
    public DeltaFilePageStoreIo createLatestVersion(Path filePath, int index, int[] pageIndexes) {
        return new DeltaFilePageStoreIo(
                fileIoFactory,
                filePath,
                new DeltaFilePageStoreIoHeader(
                        latestDeltaFileVersion,
                        index,
                        pageSize,
                        pageIndexes
                )
        );
    }
}
