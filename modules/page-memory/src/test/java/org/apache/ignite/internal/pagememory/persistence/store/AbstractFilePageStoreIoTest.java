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

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.pagememory.io.PageIo.getCrc;
import static org.apache.ignite.internal.pagememory.persistence.store.TestPageStoreUtils.createDataPageId;
import static org.apache.ignite.internal.pagememory.persistence.store.TestPageStoreUtils.createPageByteBuffer;
import static org.apache.ignite.internal.pagememory.persistence.store.TestPageStoreUtils.randomBytes;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Abstract class for testing descendants of {@link AbstractFilePageStoreIo}.
 */
@ExtendWith(WorkDirectoryExtension.class)
public abstract class AbstractFilePageStoreIoTest extends BaseIgniteAbstractTest {
    protected static final int PAGE_SIZE = 1024;

    @WorkDirectory
    protected Path workDir;

    /**
     * Creates an instance of {@link AbstractFilePageStoreIo}.
     *
     * @param filePath File page store path.
     * @param ioFactory {@link FileIo} factory.
     */
    abstract <T extends AbstractFilePageStoreIo> T createFilePageStoreIo(Path filePath, FileIoFactory ioFactory);

    /**
     * Creates an instance of {@link AbstractFilePageStoreIo}.
     *
     * @param filePath File page store path.
     */
    <T extends AbstractFilePageStoreIo> T createFilePageStoreIo(Path filePath) {
        return createFilePageStoreIo(filePath, new RandomAccessFileIoFactory());
    }

    @Test
    void testStop() throws Exception {
        Path testFilePath0 = workDir.resolve("test0");
        Path testFilePath1 = workDir.resolve("test1");

        // Checks uninitialized store.

        try (
                AbstractFilePageStoreIo filePageStoreIo0 = createFilePageStoreIo(testFilePath0);
                AbstractFilePageStoreIo filePageStoreIo1 = createFilePageStoreIo(testFilePath1);
        ) {
            assertDoesNotThrow(() -> filePageStoreIo0.stop(false));
            assertDoesNotThrow(() -> filePageStoreIo1.stop(true));

            assertFalse(Files.exists(testFilePath0));
            assertFalse(Files.exists(testFilePath1));
        }

        // Checks initialized store.

        Path testFilePath2 = workDir.resolve("test2");
        Path testFilePath3 = workDir.resolve("test3");

        try (
                AbstractFilePageStoreIo filePageStore2 = createFilePageStoreIo(testFilePath2);
                AbstractFilePageStoreIo filePageStore3 = createFilePageStoreIo(testFilePath3);
        ) {
            filePageStore2.ensure();
            filePageStore3.ensure();

            assertDoesNotThrow(() -> filePageStore2.stop(false));
            assertTrue(Files.exists(testFilePath2));

            assertDoesNotThrow(() -> filePageStore3.stop(true));
            assertFalse(Files.exists(testFilePath3));
        }
    }

    @Test
    void testClose() throws Exception {
        Path testFilePath0 = workDir.resolve("test0");

        // Checks uninitialized store.

        try (AbstractFilePageStoreIo filePageStoreIo0 = createFilePageStoreIo(testFilePath0)) {
            assertDoesNotThrow(filePageStoreIo0::close);
            assertFalse(Files.exists(testFilePath0));
        }

        // Checks initialized store.

        Path testFilePath1 = workDir.resolve("test0");

        try (AbstractFilePageStoreIo filePageStoreIo1 = createFilePageStoreIo(testFilePath1)) {
            filePageStoreIo1.ensure();

            assertDoesNotThrow(filePageStoreIo1::close);
            assertTrue(Files.exists(testFilePath1));
        }
    }

    @Test
    void testExist() throws Exception {
        Path testFilePath = workDir.resolve("test");

        // Checks uninitialized store with not exists file.

        try (AbstractFilePageStoreIo filePageStoreIo0 = createFilePageStoreIo(testFilePath)) {
            assertFalse(filePageStoreIo0.exists());
        }

        // Checks uninitialized store with existent file.

        try (AbstractFilePageStoreIo filePageStoreIo1 = createFilePageStoreIo(Files.createFile(testFilePath))) {
            assertFalse(filePageStoreIo1.exists());
        }

        // Checks initialized store.

        try (AbstractFilePageStoreIo filePageStoreIo2 = createFilePageStoreIo(testFilePath)) {
            filePageStoreIo2.ensure();

            assertTrue(filePageStoreIo2.exists());
        }

        // Checks after closing the initialized store.

        try (AbstractFilePageStoreIo filePageStoreIo3 = createFilePageStoreIo(testFilePath)) {
            assertTrue(filePageStoreIo3.exists());
        }
    }

    @Test
    void testEnsure() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (AbstractFilePageStoreIo filePageStore0 = createFilePageStoreIo(testFilePath)) {
            assertDoesNotThrow(filePageStore0::ensure);
            assertDoesNotThrow(filePageStore0::ensure);
        }

        try (AbstractFilePageStoreIo filePageStore1 = createFilePageStoreIo(testFilePath)) {
            assertDoesNotThrow(filePageStore1::ensure);
            assertDoesNotThrow(filePageStore1::ensure);
        }
    }

    @Test
    void testSync() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (AbstractFilePageStoreIo filePageStoreIo = createFilePageStoreIo(testFilePath)) {
            assertDoesNotThrow(filePageStoreIo::sync);

            filePageStoreIo.write(createDataPageId(() -> 0), createPageByteBuffer(0, PAGE_SIZE));

            assertDoesNotThrow(filePageStoreIo::sync);
            assertEquals(2 * PAGE_SIZE, testFilePath.toFile().length());
        }
    }

    @Test
    void testSize() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (AbstractFilePageStoreIo filePageStoreIo0 = createFilePageStoreIo(testFilePath)) {
            assertEquals(0, filePageStoreIo0.size());

            filePageStoreIo0.ensure();

            assertEquals(PAGE_SIZE, filePageStoreIo0.size());

            filePageStoreIo0.write(createDataPageId(() -> 0), createPageByteBuffer(0, PAGE_SIZE));

            assertEquals(2 * PAGE_SIZE, filePageStoreIo0.size());
        }

        try (AbstractFilePageStoreIo filePageStoreIo1 = createFilePageStoreIo(testFilePath)) {
            assertEquals(0, filePageStoreIo1.size());

            filePageStoreIo1.ensure();

            assertEquals(2 * PAGE_SIZE, filePageStoreIo1.size());
        }
    }

    @Test
    void testWrite() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (AbstractFilePageStoreIo filePageStoreIo = createFilePageStoreIo(testFilePath)) {
            filePageStoreIo.ensure();

            long expPageId = createDataPageId(() -> 0);

            ByteBuffer pageByteBuffer = createPageByteBuffer(0, PAGE_SIZE);

            filePageStoreIo.write(expPageId, pageByteBuffer);

            assertEquals(2 * PAGE_SIZE, testFilePath.toFile().length());

            assertEquals(0, getCrc(pageByteBuffer));
        }
    }

    @Test
    void testRead() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (AbstractFilePageStoreIo filePageStoreIo = createFilePageStoreIo(testFilePath)) {
            filePageStoreIo.ensure();

            long expPageId = createDataPageId(() -> 0);

            ByteBuffer pageByteBuffer = createPageByteBuffer(expPageId, PAGE_SIZE);

            // Puts random bytes after: type (2 byte) + version (2 byte) + crc (4 byte).
            pageByteBuffer.position(8).put(randomBytes(128));

            filePageStoreIo.write(expPageId, pageByteBuffer.rewind());

            ByteBuffer readBuffer = ByteBuffer.allocate(PAGE_SIZE).order(pageByteBuffer.order());

            filePageStoreIo.read(expPageId, filePageStoreIo.pageOffset(expPageId), readBuffer, false);

            assertEquals(pageByteBuffer.rewind(), readBuffer.rewind());
            assertEquals(0, getCrc(readBuffer));

            readBuffer = ByteBuffer.allocate(PAGE_SIZE).order(pageByteBuffer.order());

            filePageStoreIo.read(expPageId, filePageStoreIo.pageOffset(expPageId), readBuffer, true);

            assertNotEquals(0, getCrc(readBuffer));

            // Checks for reading a page beyond the file.
            expPageId = createDataPageId(() -> 1);

            filePageStoreIo.read(expPageId, filePageStoreIo.pageOffset(expPageId), readBuffer.rewind(), true);

            assertEquals(ByteBuffer.allocate(PAGE_SIZE).order(pageByteBuffer.order()), readBuffer.rewind());
        }
    }

    @Test
    void testFilePath() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (AbstractFilePageStoreIo filePageStoreIo = createFilePageStoreIo(testFilePath)) {
            assertEquals(testFilePath, filePageStoreIo.filePath());
        }
    }

    @Test
    void testPageSize() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (AbstractFilePageStoreIo filePageStoreIo = createFilePageStoreIo(testFilePath)) {
            assertEquals(PAGE_SIZE, filePageStoreIo.pageSize());
        }
    }

    @Test
    void testRenameFilePath() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (AbstractFilePageStoreIo filePageStoreIo = createFilePageStoreIo(testFilePath)) {
            filePageStoreIo.ensure();

            // Nothing should happen.
            assertDoesNotThrow(() -> filePageStoreIo.renameFilePath(testFilePath));

            assertEquals(testFilePath, filePageStoreIo.filePath());

            Path testFilePath0 = workDir.resolve("test0");

            filePageStoreIo.renameFilePath(testFilePath0);

            assertEquals(testFilePath0, filePageStoreIo.filePath());

            // Check that when writing one page and renaming the file, we will not lose anything.
            long expPageId = createDataPageId(() -> 0);

            ByteBuffer pageByteBuffer = createPageByteBuffer(0, PAGE_SIZE);

            filePageStoreIo.write(expPageId, pageByteBuffer);

            Path testFilePath1 = workDir.resolve("test1");

            filePageStoreIo.renameFilePath(testFilePath1);

            assertEquals(testFilePath1, filePageStoreIo.filePath());

            assertEquals(2 * PAGE_SIZE, Files.size(testFilePath1));
        }
    }

    @Test
    void testRenameAndEnsure() throws Exception {
        Path filePath = workDir.resolve("test");

        FileIoFactory ioFactory = spy(new RandomAccessFileIoFactory());

        try (AbstractFilePageStoreIo filePageStoreIo = createFilePageStoreIo(filePath, ioFactory)) {
            filePageStoreIo.ensure();

            clearInvocations(ioFactory);

            Path newFilePath = workDir.resolve("test0");

            filePageStoreIo.renameFilePath(newFilePath);

            filePageStoreIo.ensure();

            verify(ioFactory).create(newFilePath, CREATE, READ, WRITE);
        }
    }

    @RepeatedTest(100)
    void testRenameAndEnsureRace() throws Exception {
        Path filePath = workDir.resolve("test");

        FileIoFactory ioFactory = spy(new RandomAccessFileIoFactory());

        try (AbstractFilePageStoreIo filePageStoreIo = createFilePageStoreIo(filePath, ioFactory)) {
            filePageStoreIo.ensure();

            clearInvocations(ioFactory);

            Path newFilePath = workDir.resolve("test0");

            runRace(
                    () -> filePageStoreIo.renameFilePath(newFilePath),
                    filePageStoreIo::ensure
            );

            verify(ioFactory).create(newFilePath, CREATE, READ, WRITE);
        }
    }

    @ParameterizedTest
    @MethodSource("ioFactories")
    void testRenameAndReadRace(FileIoFactory ioFactory) throws Exception {
        Path filePath = workDir.resolve("test");

        try (AbstractFilePageStoreIo filePageStoreIo = createFilePageStoreIo(filePath, ioFactory)) {
            filePageStoreIo.ensure();

            long expPageId = createDataPageId(() -> 1);

            ByteBuffer byteBuffer = createPageByteBuffer(expPageId, PAGE_SIZE);

            filePageStoreIo.write(expPageId, byteBuffer.rewind());

            // Loop works way better than @RepeatedTest when you need to reproduce a particularly naughty race.
            for (int i = 0; i < 1000; i++) {
                Path newFilePath = workDir.resolve("test" + i);

                byteBuffer.rewind();

                runRace(
                        () -> filePageStoreIo.renameFilePath(newFilePath),
                        () -> filePageStoreIo.read(expPageId, filePageStoreIo.pageOffset(expPageId), byteBuffer, false)
                );
            }
        }
    }

    private static FileIoFactory[] ioFactories() {
        return new FileIoFactory[]{new RandomAccessFileIoFactory()};
    }
}
