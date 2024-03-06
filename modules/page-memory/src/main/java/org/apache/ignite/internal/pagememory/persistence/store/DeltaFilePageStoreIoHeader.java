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
import static org.apache.ignite.internal.util.StringUtils.hexLong;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.ignite.internal.fileio.FileIo;
import org.jetbrains.annotations.Nullable;

/**
 * {@link DeltaFilePageStoreIo} header.
 *
 * <p>Total length in bytes {@link #headerSize()}.
 *
 * <ul>
 *     <li>{@link #SIGNATURE signature} (8 bytes)</li>
 *     <li>{@link #version() version} (4 bytes)</li>
 *     <li>{@link #pageSize() pageSize} (4 bytes)</li>
 *     <li>{@link #pageIndexes() pageIndexes}
 *         <ul>
 *             <li>array length (4 bytes)</li>
 *             <li>array elements (array length * 4 bytes)</li>
 *         </ul>
 *     </li>
 * </ul>
 */
public class DeltaFilePageStoreIoHeader {
    /** File signature. */
    private static final long SIGNATURE = 0xDEAFAEE072020173L;

    /** Size of the common delta file page store header for all versions, in bytes. */
    private static final int COMMON_HEADER_SIZE =
            8/* SIGNATURE */ + 4/* version */ + 4/* index */ + 4/* page size */ + 4/* page index array length */;

    private final int version;

    private final int index;

    private final int pageSize;

    private final int[] pageIndexes;

    private final int headerSize;

    /**
     * Constructor.
     *
     * @param version Delta file page store version.
     * @param index Delta file page store index.
     * @param pageSize Page size in bytes.
     * @param pageIndexes Page indexes.
     */
    public DeltaFilePageStoreIoHeader(int version, int index, int pageSize, int[] pageIndexes) {
        assert pageSize >= COMMON_HEADER_SIZE : pageSize;
        assert index >= 0 : index;

        this.version = version;
        this.index = index;
        this.pageSize = pageSize;
        this.pageIndexes = pageIndexes;

        int size = COMMON_HEADER_SIZE + (4 * pageIndexes.length);

        if (size % pageSize != 0) {
            size = ((size / pageSize) + 1) * pageSize;
        }

        headerSize = size;
    }

    /**
     * Returns the version of the delta file page store.
     */
    public int version() {
        return version;
    }

    /**
     * Returns the index of the delta file page store.
     */
    public int index() {
        return index;
    }

    /**
     * Returns the page size in bytes.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * Returns the size (aligned to {@link #pageSize()}) of the header in bytes.
     */
    public int headerSize() {
        return headerSize;
    }

    /**
     * Returns page indexes.
     */
    public int[] pageIndexes() {
        return pageIndexes;
    }

    /**
     * Converts the delta file page store header (aligned to {@link #pageSize()}) to a {@link ByteBuffer} for writing to a file.
     */
    public ByteBuffer toByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(headerSize).order(nativeOrder())
                .putLong(SIGNATURE)
                .putInt(version)
                .putInt(index)
                .putInt(pageSize)
                .putInt(pageIndexes.length);

        if (pageIndexes.length > 0) {
            buffer.asIntBuffer().put(pageIndexes);
        }

        return buffer;
    }

    /**
     * Reads the header of the delta file page store.
     *
     * @param fileIo Delta file page store fileIo.
     * @param headerBuffer Buffer for reading {@link DeltaFilePageStoreIoHeader header} from {@code fileIo}.
     * @throws IOException If there are errors when reading the delta file page store header.
     */
    public static @Nullable DeltaFilePageStoreIoHeader readHeader(FileIo fileIo, ByteBuffer headerBuffer) throws IOException {
        assert headerBuffer.remaining() >= COMMON_HEADER_SIZE : headerBuffer.remaining();
        assert headerBuffer.order() == nativeOrder() : headerBuffer.order();

        if (fileIo.size() < COMMON_HEADER_SIZE) {
            return null;
        }

        fileIo.readFully(headerBuffer, 0);

        long signature = headerBuffer.rewind().getLong();

        if (SIGNATURE != signature) {
            throw new IOException(String.format(
                    "Invalid file signature [expected=%s, actual=%s]",
                    hexLong(SIGNATURE),
                    hexLong(signature))
            );
        }

        int version = headerBuffer.getInt();
        int index = headerBuffer.getInt();
        int pageSize = headerBuffer.getInt();
        int arrayLen = headerBuffer.getInt();

        if (arrayLen == 0) {
            return new DeltaFilePageStoreIoHeader(version, index, pageSize, new int[0]);
        }

        assert headerBuffer.remaining() % 4 == 0 : fileIo;

        int[] pageIndexes = new int[arrayLen];

        int i = 0;
        int filePosition = headerBuffer.capacity();

        while (i < arrayLen) {
            if (headerBuffer.remaining() == 0) {
                fileIo.readFully(headerBuffer.rewind(), filePosition);

                filePosition += headerBuffer.rewind().capacity();

                assert headerBuffer.remaining() % 4 == 0 : fileIo;
            }

            int len = Math.min(headerBuffer.remaining() / 4, arrayLen - i);

            headerBuffer.asIntBuffer().get(pageIndexes, i, len);

            headerBuffer.position(headerBuffer.position() + len * 4);

            i += len;
        }

        return new DeltaFilePageStoreIoHeader(version, index, pageSize, pageIndexes);
    }

    /**
     * Checks the index of the file.
     *
     * @param expIndex Expected index.
     * @param actIndex Actual index.
     * @throws IOException If the indexes does not match.
     */
    public static void checkFileIndex(int expIndex, int actIndex) throws IOException {
        if (expIndex != actIndex) {
            throw new IOException(String.format(
                    "Invalid file indexes [expected=%s, actual=%s]",
                    expIndex,
                    actIndex
            ));
        }
    }

    /**
     * Checks the page indexes of the file.
     *
     * @param expPageIndexes Expected page indexes.
     * @param actPageIndexes Actual page indexes.
     * @throws IOException If the page indexes does not match.
     */
    public static void checkFilePageIndexes(int[] expPageIndexes, int[] actPageIndexes) throws IOException {
        if (!Arrays.equals(expPageIndexes, actPageIndexes)) {
            throw new IOException(String.format(
                    "Invalid file pageIndexes [expected=%s, actual=%s]",
                    Arrays.toString(expPageIndexes),
                    Arrays.toString(actPageIndexes)
            ));
        }
    }
}
