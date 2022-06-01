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
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Checks version in files if it's present on the disk, creates store with the latest version otherwise.
 */
class FilePageStoreFactory {
    /** Latest page store version. */
    public final int latestVersion = FilePageStore.VERSION;

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
     * @param type Data type, can be {@link PageStore#TYPE_IDX} or {@link PageStore#TYPE_DATA}.
     * @param filePath File page store path.
     * @return File page store.
     * @throws IgniteInternalCheckedException if failed
     */
    public FilePageStore createPageStore(byte type, Path filePath) throws IgniteInternalCheckedException {
        if (!Files.exists(filePath)) {
            return createPageStore(type, filePath, pageSize, latestVersion);
        }

        try (FileIo fileIo = fileIoFactory.create(filePath)) {
            int commonHeaderSize = FilePageStore.COMMON_HEADER_SIZE;

            if (fileIo.size() < commonHeaderSize) {
                return createPageStore(type, filePath, pageSize, latestVersion);
            }

            ByteBuffer commonHeader = ByteBuffer.allocate(commonHeaderSize).order(ByteOrder.nativeOrder());

            fileIo.readFully(commonHeader);

            commonHeader.rewind();

            // Read signature.
            commonHeader.getLong();

            int ver = commonHeader.getInt();

            return createPageStore(type, filePath, pageSize, ver);
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Error while creating file page store [file=" + filePath + "]:", e);
        }
    }

    /**
     * Instantiates specific version of {@link FilePageStore}.
     *
     * @param type Data type, can be {@link PageStore#TYPE_IDX} or {@link PageStore#TYPE_DATA}.
     * @param filePath File page store path.
     * @param ver File page store version.
     * @param pageSize Page size in bytes.
     */
    private FilePageStore createPageStore(
            byte type,
            Path filePath,
            int pageSize,
            int ver
    ) throws IgniteInternalCheckedException {
        switch (ver) {
            case FilePageStore.VERSION:
                return new FilePageStore(type, filePath, fileIoFactory, pageSize);

            default:
                throw new IgniteInternalCheckedException(String.format(
                        "Unknown version of file page store [version=%s, file=%s]",
                        ver,
                        filePath
                ));
        }
    }
}
