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
import static org.apache.ignite.internal.util.IgniteUtils.hexLong;
import static org.apache.ignite.internal.util.IgniteUtils.readableSize;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.fileio.FileIo;
import org.jetbrains.annotations.Nullable;

/**
 * {@link FilePageStore} header.
 *
 * <p>Total length in bytes {@link #headerSize()}.
 *
 * <ul>
 *     <li>{@link #SIGNATURE signature} (8 bytes)</li>
 *     <li>{@link #version version} (4 bytes)</li>
 *     <li>{@link #pageSize pageSize} (4 bytes)</li>
 * </ul>
 */
class FilePageStoreHeader {
    /** Page store file signature. */
    private static final long SIGNATURE = 0xF19AC4FE60C530B8L;

    /** Size of the common file page store header for all versions, in bytes. */
    private static final int COMMON_HEADER_SIZE = 8/*SIGNATURE*/ + 4/*VERSION*/ + 4/*page size*/;

    private final int version;

    private final int pageSize;

    /**
     * Constructor.
     *
     * @param version File page store version.
     * @param pageSize Page size in bytes.
     */
    FilePageStoreHeader(int version, int pageSize) {
        assert pageSize >= COMMON_HEADER_SIZE : pageSize;

        this.version = version;
        this.pageSize = pageSize;
    }

    /**
     * Returns the version of the file page store.
     */
    int version() {
        return version;
    }

    /**
     * Returns the page size in bytes.
     */
    int pageSize() {
        return pageSize;
    }

    /**
     * Returns the size (aligned to {@link #pageSize()}) of the header in bytes.
     */
    int headerSize() {
        return pageSize;
    }

    /**
     * Converts the file page store header (aligned to {@link #pageSize()}) to a {@link ByteBuffer} for writing to a file.
     */
    ByteBuffer toByteBuffer() {
        return ByteBuffer.allocate(headerSize()).order(nativeOrder()).rewind()
                .putLong(SIGNATURE)
                .putInt(version)
                .putInt(pageSize)
                .rewind();
    }

    /**
     * Reads the header of a file page store.
     *
     * @param fileIo File page store fileIo.
     * @param readIntoBuffer Buffer for reading {@link FilePageStoreHeader header} from {@code fileIo}.
     * @throws IOException If there are errors when reading the file page store header.
     */
    static @Nullable FilePageStoreHeader readHeader(FileIo fileIo, ByteBuffer readIntoBuffer) throws IOException {
        assert readIntoBuffer.remaining() >= COMMON_HEADER_SIZE : readIntoBuffer.remaining();

        if (fileIo.size() < COMMON_HEADER_SIZE) {
            return null;
        }

        fileIo.readFully(readIntoBuffer, 0);

        long signature = readIntoBuffer.rewind().getLong();

        if (SIGNATURE != signature) {
            throw new IOException(String.format(
                    "Invalid file signature [expected=%s, actual=%s]",
                    hexLong(SIGNATURE),
                    hexLong(signature))
            );
        }

        return new FilePageStoreHeader(readIntoBuffer.getInt(), readIntoBuffer.getInt());
    }

    /**
     * Checks the {@link FilePageStoreHeader#pageSize() page size in bytes}.
     *
     * @param header File page store header.
     * @param pageSize Expected page size in bytes.
     * @throws IOException If the page size in bytes does not match the page size in bytes from the header.
     */
    static void checkHeaderPageSize(FilePageStoreHeader header, int pageSize) throws IOException {
        if (header.pageSize() != pageSize) {
            throw new IOException(String.format(
                    "Invalid file pageSize [expected=%s, actual=%s]",
                    readableSize(pageSize, false),
                    readableSize(header.pageSize(), false)
            ));
        }
    }

    /**
     * Checks the {@link FilePageStoreHeader#version() version}.
     *
     * @param header File page store header.
     * @param version Expected version.
     * @throws IOException If the version does not match the version from the header.
     */
    static void checkHeaderVersion(FilePageStoreHeader header, int version) throws IOException {
        if (header.version() != version) {
            throw new IOException(String.format(
                    "Invalid file version [expected=%s, actual=%s]",
                    version,
                    header.version()
            ));
        }
    }
}
