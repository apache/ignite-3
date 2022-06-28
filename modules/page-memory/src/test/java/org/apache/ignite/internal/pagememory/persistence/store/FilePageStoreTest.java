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
import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagememory.io.PageIo.getCrc;
import static org.apache.ignite.internal.pagememory.persistence.store.PageStore.TYPE_DATA;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.pagememory.TestPageIoModule.TestPageIo;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link FilePageStore} testing.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class FilePageStoreTest {
    private static final int PAGE_SIZE = 1024;

    @WorkDirectory
    private Path workDir;

    @Test
    void testStop() throws Exception {
        Path testFilePath = workDir.resolve("test");

        // Checks uninitialized store with non-existent file.

        FilePageStore filePageStore0 = createFilePageStore(testFilePath);

        assertDoesNotThrow(() -> filePageStore0.stop(false));
        assertDoesNotThrow(() -> filePageStore0.stop(true));

        // Checks uninitialized store with existent file.

        Files.write(testFilePath, new byte[1024]);

        FilePageStore filePageStore1 = createFilePageStore(testFilePath);

        assertDoesNotThrow(() -> filePageStore1.stop(false));
        assertTrue(Files.exists(testFilePath));

        assertDoesNotThrow(() -> filePageStore1.stop(true));
        assertFalse(Files.exists(testFilePath));

        // Checks initialized store.

        FilePageStore filePageStore2 = createFilePageStore(testFilePath);

        filePageStore2.ensure();

        assertDoesNotThrow(() -> filePageStore2.stop(false));
        assertTrue(Files.exists(testFilePath));

        assertDoesNotThrow(() -> filePageStore2.stop(true));
        assertFalse(Files.exists(testFilePath));
    }

    @Test
    void testClose() throws Exception {
        Path testFilePath = workDir.resolve("test");

        // Checks uninitialized store with non-existent file.

        FilePageStore filePageStore0 = createFilePageStore(testFilePath);

        assertDoesNotThrow(filePageStore0::close);

        // Checks uninitialized store with existent file.

        Files.write(testFilePath, new byte[1024]);

        FilePageStore filePageStore1 = createFilePageStore(testFilePath);

        assertDoesNotThrow(filePageStore1::close);
        assertTrue(Files.exists(testFilePath));

        // Checks initialized store.

        Files.delete(testFilePath);

        FilePageStore filePageStore2 = createFilePageStore(testFilePath);

        filePageStore2.ensure();

        assertDoesNotThrow(filePageStore2::close);
        assertTrue(Files.exists(testFilePath));
    }

    @Test
    void testExist() throws Exception {
        Path testFilePath = workDir.resolve("test");

        // Checks uninitialized store with non-existent file.

        FilePageStore filePageStore0 = createFilePageStore(testFilePath);

        assertFalse(filePageStore0.exists());

        // Checks uninitialized store with existent file.

        Files.createFile(testFilePath);

        FilePageStore filePageStore1 = createFilePageStore(testFilePath);

        assertFalse(filePageStore1.exists());

        // Checks initialized store.

        FilePageStore filePageStore2 = createFilePageStore(testFilePath);

        filePageStore2.ensure();

        assertTrue(filePageStore2.exists());

        // Checks after closing the initialized store.

        filePageStore2.close();

        FilePageStore filePageStore3 = createFilePageStore(testFilePath);

        assertTrue(filePageStore3.exists());
    }

    @Test
    void testEnsure() throws Exception {
        Path testFilePath = workDir.resolve("test");

        FilePageStore filePageStore0 = createFilePageStore(testFilePath);

        assertDoesNotThrow(filePageStore0::ensure);
        assertTrue(Files.exists(testFilePath));
        assertEquals(PAGE_SIZE, testFilePath.toFile().length());

        assertDoesNotThrow(filePageStore0::ensure);
        assertTrue(Files.exists(testFilePath));
        assertEquals(PAGE_SIZE, testFilePath.toFile().length());

        filePageStore0.close();

        FilePageStore filePageStore1 = createFilePageStore(testFilePath);

        assertDoesNotThrow(filePageStore1::ensure);
        assertTrue(Files.exists(testFilePath));
        assertEquals(PAGE_SIZE, testFilePath.toFile().length());

        assertDoesNotThrow(filePageStore1::ensure);
        assertTrue(Files.exists(testFilePath));
        assertEquals(PAGE_SIZE, testFilePath.toFile().length());
    }

    @Test
    void testAllocatePage() throws Exception {
        Path testFilePath = workDir.resolve("test");

        // Checks allocation without writing pages.

        FilePageStore filePageStore0 = createFilePageStore(testFilePath);

        assertEquals(0, filePageStore0.pages());

        assertEquals(0, filePageStore0.allocatePage());

        assertEquals(1, filePageStore0.pages());

        assertEquals(1, filePageStore0.allocatePage());

        assertEquals(2, filePageStore0.pages());

        filePageStore0.close();

        FilePageStore filePageStore1 = createFilePageStore(testFilePath);

        assertEquals(0, filePageStore1.pages());

        filePageStore1.ensure();

        assertEquals(0, filePageStore1.pages());

        assertEquals(0, filePageStore1.allocatePage());

        assertEquals(1, filePageStore1.pages());

        assertEquals(1, filePageStore1.allocatePage());

        assertEquals(2, filePageStore1.pages());

        filePageStore1.close();

        // Checks allocation with writing pages.

        FilePageStore filePageStore2 = createFilePageStore(testFilePath);

        assertEquals(0, filePageStore2.pages());

        filePageStore2.write(createPageId(filePageStore2), createPageByteBuffer(), 0, true);

        assertEquals(1, filePageStore2.pages());

        filePageStore2.write(createPageId(filePageStore2), createPageByteBuffer(), 0, true);

        assertEquals(2, filePageStore2.pages());

        filePageStore2.close();

        FilePageStore filePageStore3 = createFilePageStore(testFilePath);

        assertEquals(0, filePageStore3.pages());

        filePageStore3.ensure();

        assertEquals(2, filePageStore3.pages());

        assertEquals(2, filePageStore3.allocatePage());

        assertEquals(3, filePageStore3.pages());
    }

    @Test
    void testSync() throws Exception {
        Path testFilePath = workDir.resolve("test");

        FilePageStore filePageStore = createFilePageStore(testFilePath);

        assertDoesNotThrow(filePageStore::sync);
        assertTrue(Files.exists(testFilePath));
        assertEquals(PAGE_SIZE, testFilePath.toFile().length());

        filePageStore.write(createPageId(filePageStore), createPageByteBuffer(), 0, true);

        assertDoesNotThrow(filePageStore::sync);
        assertTrue(Files.exists(testFilePath));
        assertEquals(2 * PAGE_SIZE, testFilePath.toFile().length());
    }

    @Test
    void testSize() throws Exception {
        Path testFilePath = workDir.resolve("test");

        FilePageStore filePageStore0 = createFilePageStore(testFilePath);

        assertEquals(0, filePageStore0.size());

        filePageStore0.ensure();

        assertEquals(PAGE_SIZE, filePageStore0.size());

        filePageStore0.write(createPageId(filePageStore0), createPageByteBuffer(), 0, true);

        assertEquals(2 * PAGE_SIZE, filePageStore0.size());

        filePageStore0.close();

        FilePageStore filePageStore1 = createFilePageStore(testFilePath);

        assertEquals(0, filePageStore1.size());

        filePageStore1.ensure();

        assertEquals(2 * PAGE_SIZE, filePageStore1.size());
    }

    @Test
    void testVersion() {
        assertEquals(1, createFilePageStore(workDir.resolve("test")).version());
    }

    @Test
    void testPageOffset() {
        FilePageStore filePageStore = createFilePageStore(workDir.resolve("test"));

        assertEquals(PAGE_SIZE, filePageStore.pageOffset(pageId(0, FLAG_DATA, 0)));
        assertEquals(2 * PAGE_SIZE, filePageStore.pageOffset(pageId(0, FLAG_DATA, 1)));
        assertEquals(3 * PAGE_SIZE, filePageStore.pageOffset(pageId(0, FLAG_DATA, 2)));
    }

    @Test
    void testHeader() throws Exception {
        Path testFilePath = workDir.resolve("test");

        FilePageStore filePageStore = createFilePageStore(testFilePath);

        // Checks success read header.

        assertEquals(PAGE_SIZE, filePageStore.headerSize());

        ByteBuffer headerBuffer = ByteBuffer.allocate(PAGE_SIZE).order(nativeOrder());

        assertDoesNotThrow(() -> filePageStore.readHeader(headerBuffer));

        headerBuffer.rewind();

        assertEquals(0xF19AC4FE60C530B8L, headerBuffer.getLong());
        assertEquals(1, headerBuffer.getInt());
        assertEquals(TYPE_DATA, headerBuffer.get());
        assertEquals(PAGE_SIZE, headerBuffer.getInt());

        // Checks fail read header.

        checkFailReadHeader(
                testFilePath,
                filePageStore,
                copyOf(headerBuffer).putLong(-1),
                "signature"
        );

        checkFailReadHeader(
                testFilePath,
                filePageStore,
                copyOf(headerBuffer).putInt(8, -1),
                "version"
        );

        checkFailReadHeader(
                testFilePath,
                filePageStore,
                copyOf(headerBuffer).put(12, (byte) -1),
                "type"
        );

        checkFailReadHeader(
                testFilePath,
                filePageStore,
                copyOf(headerBuffer).putInt(13, -1),
                "pageSize"
        );
    }

    @Test
    void testWrite() throws Exception {
        Path testFilePath = workDir.resolve("test");

        FilePageStore filePageStore = createFilePageStore(testFilePath);

        filePageStore.ensure();

        long expPageId = createPageId(filePageStore);

        ByteBuffer pageByteBuffer = createPageByteBuffer();

        PageWriteListener pageWriteListener = spy(new PageWriteListener() {
            /** {@inheritDoc} */
            @Override
            public void accept(long pageId, ByteBuffer buf) {
                assertEquals(expPageId, pageId);

                assertSame(pageByteBuffer, buf);

                assertEquals(PAGE_SIZE, testFilePath.toFile().length());

                assertNotEquals(0, PageIo.getCrc(pageByteBuffer));
            }
        });

        filePageStore.addWriteListener(pageWriteListener);

        filePageStore.write(expPageId, pageByteBuffer, 0, true);

        verify(pageWriteListener, times(1)).accept(anyLong(), any(ByteBuffer.class));

        assertEquals(2 * PAGE_SIZE, testFilePath.toFile().length());

        assertEquals(0, PageIo.getCrc(pageByteBuffer));
    }

    @Test
    void testRead() throws Exception {
        Path testFilePath = workDir.resolve("test");

        FilePageStore filePageStore = createFilePageStore(testFilePath);

        filePageStore.ensure();

        long expPageId = createPageId(filePageStore);

        ByteBuffer pageByteBuffer = createPageByteBuffer();

        // Puts random bytes after: type (2 byte) + version (2 byte) + crc (4 byte).
        pageByteBuffer.position(8).put(randomBytes(128));

        PageWriteListener pageWriteListener = spy(new PageWriteListener() {
            /** {@inheritDoc} */
            @Override
            public void accept(long pageId, ByteBuffer buf) {
                // No-op.
            }
        });

        filePageStore.addWriteListener(pageWriteListener);

        filePageStore.write(expPageId, pageByteBuffer.rewind(), 0, true);

        ByteBuffer readBuffer = ByteBuffer.allocate(PAGE_SIZE).order(pageByteBuffer.order());

        assertTrue(filePageStore.read(expPageId, readBuffer, false));
        assertEquals(pageByteBuffer.rewind(), readBuffer.rewind());
        assertEquals(0, getCrc(readBuffer));

        readBuffer = ByteBuffer.allocate(PAGE_SIZE).order(pageByteBuffer.order());

        assertTrue(filePageStore.read(expPageId, readBuffer, true));
        assertNotEquals(0, getCrc(readBuffer));
    }

    /**
     * Checks that if some part of the header is broken, then there will be an error when reading it.
     *
     * @param filePath File page store path.
     * @param filePageStore File page store.
     * @param headerBuffer Byte buffer with header content to write to {@code filePath}.
     * @param expBrokenHeaderPart Expected broken header part.
     */
    private static void checkFailReadHeader(
            Path filePath,
            FilePageStore filePageStore,
            ByteBuffer headerBuffer,
            String expBrokenHeaderPart
    ) throws Exception {
        try (FileIo fileIo = new RandomAccessFileIoFactory().create(filePath)) {
            fileIo.writeFully(headerBuffer, 0);

            fileIo.force();

            IgniteInternalCheckedException exception = assertThrows(
                    IgniteInternalCheckedException.class,
                    () -> filePageStore.readHeader(ByteBuffer.allocate(headerBuffer.limit()).order(headerBuffer.order()))
            );

            assertThat(exception.getCause(), instanceOf(IOException.class));

            assertThat(
                    exception.getCause().getMessage(),
                    startsWith(String.format("Failed to verify, file='%s' (invalid file %s)", filePath, expBrokenHeaderPart))
            );
        }
    }

    private static byte[] randomBytes(int len) {
        byte[] res = new byte[len];

        ThreadLocalRandom.current().nextBytes(res);

        return res;
    }

    private static ByteBuffer copyOf(ByteBuffer buffer) {
        ByteBuffer res = ByteBuffer.allocate(buffer.limit()).order(buffer.order());

        res.put(buffer.rewind());

        return res.rewind();
    }

    private static ByteBuffer createPageByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocateDirect(PAGE_SIZE).order(nativeOrder());

        new TestPageIo().initNewPage(GridUnsafe.bufferAddress(buffer), 0, PAGE_SIZE);

        return buffer;
    }

    private static long createPageId(FilePageStore filePageStore) throws Exception {
        return pageId(pageId(0, FLAG_DATA, (int) filePageStore.allocatePage()));
    }

    private static FilePageStore createFilePageStore(Path filePath) {
        return new FilePageStore(TYPE_DATA, filePath, new RandomAccessFileIoFactory(), PAGE_SIZE);
    }
}
