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

package org.apache.ignite.internal.fileio;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Arrays.copyOfRange;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;

/**
 * An abstract class for testing {@link FileIo} implementations.
 */
@ExtendWith(WorkDirectoryExtension.class)
public abstract class AbstractFileIoTest {
    protected FileIoFactory fileIoFactory;

    @WorkDirectory
    protected Path workDir;

    /**
     * Returns the class inheritor of {@link FileIo} that should be created from {@link #fileIoFactory}.
     */
    protected abstract Class<? extends FileIo> fileIoClass();

    @Test
    void testFileIoFactory() throws Exception {
        assertThat(fileIoFactory.create(workDir.resolve("test0")), instanceOf(fileIoClass()));
        assertThat(fileIoFactory.create(workDir.resolve("test1"), CREATE, READ, WRITE), instanceOf(fileIoClass()));
    }

    @Test
    void testPosition() throws Exception {
        FileIo fileIo = fileIoFactory.create(workDir.resolve("test"));

        assertEquals(0, fileIo.position());

        fileIo.position(100);

        assertEquals(100, fileIo.position());
    }

    @Test
    void testRead() throws Exception {
        checkReadOperation(FileIo::read);
    }

    @Test
    void testReadByPosition() throws Exception {
        checkReadByPositionOperation((fileIo, position, buffer) -> fileIo.read(buffer, position));
    }

    @Test
    void testReadToByteArray() throws Exception {
        checkReadOperation((fileIo, buffer) -> fileIo.read(buffer.array(), 0, 1024));
    }

    @Test
    void testReadFully() throws Exception {
        checkReadOperation(FileIo::readFully);
    }

    @Test
    void testReadByPositionFully() throws Exception {
        checkReadByPositionOperation((fileIo, position, buffer) -> fileIo.read(buffer, position));
    }

    @Test
    void testReadToByteArrayFully() throws Exception {
        checkReadOperation((fileIo, buffer) -> fileIo.readFully(buffer.array(), 0, 1024));
    }

    @Test
    void testWrite() throws Exception {
        checkWriteOperation(FileIo::write);
    }

    @Test
    void testWriteByPosition() throws Exception {
        checkWriteByPositionOperation((fileIo, position, buffer) -> fileIo.write(buffer, position));
    }

    @Test
    void testWriteFromByteArray() throws Exception {
        checkWriteFromByteArrayOperation((fileIo, off, bytes) -> fileIo.write(bytes, (int) off, Math.min(1024, bytes.length)));
    }

    @Test
    void testWriteFully() throws Exception {
        checkWriteOperation(FileIo::writeFully);
    }

    @Test
    void testWriteByPositionFully() throws Exception {
        checkWriteByPositionOperation((fileIo, position, buffer) -> fileIo.writeFully(buffer, position));
    }

    @Test
    void testWriteFromByteArrayFully() throws Exception {
        checkWriteFromByteArrayOperation((fileIo, off, bytes) -> fileIo.writeFully(bytes, (int) off, Math.min(1024, bytes.length)));
    }

    @Test
    void testMap() throws Exception {
        Path testFilePath = workDir.resolve("test");

        FileIo fileIo = fileIoFactory.create(testFilePath);

        MappedByteBuffer mmap = fileIo.map(1024);

        assertNotNull(mmap);

        assertEquals(0, mmap.position());

        assertEquals(1024, mmap.capacity());
    }

    /**
     * Checks that no exceptions will be thrown when calling {@link FileIo#force()} and {@link FileIo#force(boolean)}, and after calling
     * these methods, the written content can be read from the file (or os cache).
     *
     * @throws Exception If failed.
     */
    @Test
    void testForce() throws Exception {
        Path testFilePath = workDir.resolve("test");

        FileIo fileIo = fileIoFactory.create(testFilePath);

        ByteBuffer randomBytes = ByteBuffer.wrap(randomByteArray(1024));

        assertEquals(1024, fileIo.writeFully(randomBytes));

        assertDoesNotThrow((Executable) fileIo::force);

        assertArrayEquals(toByteArray(randomBytes), toByteArray(testFilePath));

        randomBytes = ByteBuffer.wrap(randomByteArray(1024));

        assertEquals(1024, fileIo.writeFully(randomBytes, 0));

        assertDoesNotThrow(() -> fileIo.force(true));

        assertArrayEquals(toByteArray(randomBytes), toByteArray(testFilePath));

        randomBytes = ByteBuffer.wrap(randomByteArray(1024));

        assertEquals(1024, fileIo.writeFully(randomBytes, 0));

        assertDoesNotThrow(() -> fileIo.force(false));

        assertArrayEquals(toByteArray(randomBytes), toByteArray(testFilePath));
    }

    @Test
    void testSize() throws Exception {
        Path testFilePath = workDir.resolve("test");

        FileIo fileIo = fileIoFactory.create(testFilePath);

        assertEquals(0, fileIo.size());

        ByteBuffer randomBytes = ByteBuffer.wrap(randomByteArray(1024));

        assertEquals(1024, fileIo.writeFully(randomBytes));

        assertEquals(1024, fileIo.size());

        assertEquals(1024, fileIoFactory.create(testFilePath).size());
    }

    @Test
    void testClear() throws Exception {
        Path testFilePath = workDir.resolve("test");

        FileIo fileIo = fileIoFactory.create(testFilePath);

        fileIo.clear();

        assertEquals(0, fileIo.position());
        assertEquals(0, fileIo.size());
        assertArrayEquals(new byte[0], toByteArray(testFilePath));

        assertEquals(1024, fileIo.writeFully(ByteBuffer.wrap(randomByteArray(1024))));

        fileIo.clear();

        assertEquals(0, fileIo.position());
        assertEquals(0, fileIo.size());
        assertArrayEquals(new byte[0], toByteArray(testFilePath));
    }

    @Test
    void testClose() throws Exception {
        Path testFilePath = workDir.resolve("test");

        FileIo fileIo0 = fileIoFactory.create(testFilePath);

        fileIo0.close();

        assertThrows(IOException.class, () -> fileIo0.read(ByteBuffer.allocate(0)));

        FileIo fileIo1 = fileIoFactory.create(testFilePath);

        assertEquals(1024, fileIo1.writeFully(ByteBuffer.wrap(randomByteArray(1024))));

        fileIo1.close();

        assertThrows(IOException.class, () -> fileIo1.read(ByteBuffer.allocate(0)));
    }

    /**
     * Checks that when reading from a {@link FileIo} into a {@link ByteBuffer} (1024 bytes), it will be filled correctly (all 4096 bytes
     * will be read, and they will be consistent) and the {@link FileIo#position()} will increase after each successful read.
     *
     * @param readOperation I/O read operation from {@link FileIo}.
     * @throws Exception If failed.
     */
    private void checkReadOperation(IoOperation<ByteBuffer, Integer> readOperation) throws Exception {
        byte[] randomBytes = randomByteArray(4 * 1024);

        Path testFilePath = workDir.resolve("test");

        writeBytes(testFilePath, randomBytes);

        ByteBuffer buffer = ByteBuffer.allocate(1024);

        FileIo fileIo = fileIoFactory.create(testFilePath);

        assertEquals(1024, readOperation.apply(fileIo, buffer.rewind()));
        assertArrayEquals(copyOfRange(randomBytes, 0, 1024), toByteArray(buffer));
        assertEquals(1024, fileIo.position());

        assertEquals(1024, readOperation.apply(fileIo, buffer.rewind()));
        assertArrayEquals(copyOfRange(randomBytes, 1024, 2 * 1024), toByteArray(buffer));
        assertEquals(2 * 1024, fileIo.position());

        assertEquals(1024, readOperation.apply(fileIo, buffer.rewind()));
        assertArrayEquals(copyOfRange(randomBytes, 2 * 1024, 3 * 1024), toByteArray(buffer));
        assertEquals(3 * 1024, fileIo.position());

        assertEquals(1024, readOperation.apply(fileIo, buffer.rewind()));
        assertArrayEquals(copyOfRange(randomBytes, 3 * 1024, 4 * 1024), toByteArray(buffer));
        assertEquals(4 * 1024, fileIo.position());

        assertEquals(-1, readOperation.apply(fileIo, buffer.rewind()));
        assertEquals(4 * 1024, fileIo.position());
    }

    /**
     * Checks that when reading from a {@link FileIo} by position into the {@link ByteBuffer} (1024 bytes), it will be filled correctly (all
     * 2048 bytes will be read, and they will be consistent) and the {@link FileIo#position()} will not change.
     *
     * @param readOperation I/O read operation from {@link FileIo} by position.
     * @throws Exception If failed.
     */
    private void checkReadByPositionOperation(IoOperationByPosition<ByteBuffer, Integer> readOperation) throws Exception {
        byte[] randomBytes = randomByteArray(4 * 1024);

        Path testFilePath = workDir.resolve("test");

        writeBytes(testFilePath, randomBytes);

        ByteBuffer buffer = ByteBuffer.allocate(1024);

        FileIo fileIo = fileIoFactory.create(testFilePath);

        assertEquals(1024, readOperation.apply(fileIo, 1024, buffer.rewind()));
        assertArrayEquals(copyOfRange(randomBytes, 1024, 2 * 1024), toByteArray(buffer));
        assertEquals(0, fileIo.position());

        assertEquals(1024, readOperation.apply(fileIo, 2 * 1024, buffer.rewind()));
        assertArrayEquals(copyOfRange(randomBytes, 2 * 1024, 3 * 1024), toByteArray(buffer));
        assertEquals(0, fileIo.position());

        assertEquals(-1, readOperation.apply(fileIo, 4 * 1024, buffer.rewind()));
        assertEquals(0, fileIo.position());
    }

    /**
     * Checks that when writing from the {@link ByteBuffer} (1024 bytes) to the {@link FileIo}, it will be filled correctly (all 4096 bytes
     * will be written, and they will be consistent) and the {@link FileIo#position()} will increase after each successful write.
     *
     * @param writeOperation I/O write operation to {@link FileIo}.
     * @throws Exception If failed.
     */
    private void checkWriteOperation(IoOperation<ByteBuffer, Integer> writeOperation) throws Exception {
        byte[] randomBytes = randomByteArray(4 * 1024);

        ByteBuffer randomByteBuffer = ByteBuffer.wrap(randomBytes);

        Path testFilePath = workDir.resolve("test");

        FileIo fileIo = fileIoFactory.create(testFilePath);

        assertEquals(1024, writeOperation.apply(fileIo, rangeBuffer(randomBytes, 0, 1024)));
        assertEquals(1024, fileIo.position());

        assertEquals(1024, writeOperation.apply(fileIo, rangeBuffer(randomBytes, 1024, 2 * 1024)));
        assertEquals(2 * 1024, fileIo.position());

        assertEquals(1024, writeOperation.apply(fileIo, sliceBuffer(randomByteBuffer, 2 * 1024, 3 * 1024)));
        assertEquals(3 * 1024, fileIo.position());

        assertEquals(1024, writeOperation.apply(fileIo, sliceBuffer(randomByteBuffer, 3 * 1024, 4 * 1024)));
        assertEquals(4 * 1024, fileIo.position());

        assertEquals(0, writeOperation.apply(fileIo, ByteBuffer.wrap(new byte[0])));
        assertEquals(4 * 1024, fileIo.position());

        fileIo.force();

        assertArrayEquals(randomBytes, toByteArray(testFilePath));
    }

    /**
     * Checks that when writing from the {@code byte[]} (1024 bytes) to the {@link FileIo}, it will be filled correctly (all 4096 bytes will
     * be written, and they will be consistent) and the {@link FileIo#position()} will increase after each successful write.
     *
     * @param writeOperation I/O write operation to {@link FileIo}.
     * @throws Exception If failed.
     */
    private void checkWriteFromByteArrayOperation(IoOperationByPosition<byte[], Integer> writeOperation) throws Exception {
        byte[] randomBytes = randomByteArray(4 * 1024);

        Path testFilePath = workDir.resolve("test");

        FileIo fileIo = fileIoFactory.create(testFilePath);

        assertEquals(1024, writeOperation.apply(fileIo, 0, randomBytes));
        assertEquals(1024, fileIo.position());

        assertEquals(1024, writeOperation.apply(fileIo, 1024, randomBytes));
        assertEquals(2 * 1024, fileIo.position());

        assertEquals(1024, writeOperation.apply(fileIo, 2 * 1024, randomBytes));
        assertEquals(3 * 1024, fileIo.position());

        assertEquals(1024, writeOperation.apply(fileIo, 3 * 1024, randomBytes));
        assertEquals(4 * 1024, fileIo.position());

        assertEquals(0, writeOperation.apply(fileIo, 0, new byte[0]));
        assertEquals(4 * 1024, fileIo.position());

        fileIo.force();

        assertArrayEquals(randomBytes, toByteArray(testFilePath));
    }

    /**
     * Checks that when writing from the {@link ByteBuffer} (1024 bytes) to the {@link FileIo} by position, it will be filled correctly (all
     * 2048 bytes will be written, and they will be consistent) and the {@link FileIo#position()} will not change.
     *
     * @param writeOperation I/O write operation to {@link FileIo} by position.
     * @throws Exception If failed.
     */
    private void checkWriteByPositionOperation(IoOperationByPosition<ByteBuffer, Integer> writeOperation) throws Exception {
        byte[] randomBytes = randomByteArray(4 * 1024);

        ByteBuffer randomByteBuffer = ByteBuffer.wrap(randomBytes);

        Path testFilePath = workDir.resolve("test");

        FileIo fileIo = fileIoFactory.create(testFilePath);

        assertEquals(1024, writeOperation.apply(fileIo, 1024, rangeBuffer(randomBytes, 1024, 2 * 1024)));
        assertEquals(0, fileIo.position());

        assertEquals(1024, writeOperation.apply(fileIo, 2 * 1024, sliceBuffer(randomByteBuffer, 3 * 1024, 4 * 1024)));
        assertEquals(0, fileIo.position());

        assertEquals(0, writeOperation.apply(fileIo, 3 * 1024, ByteBuffer.wrap(new byte[0])));
        assertEquals(0, fileIo.position());

        fileIo.force();

        byte[] expectedBytes = new byte[3 * 1024];

        System.arraycopy(randomBytes, 1024, expectedBytes, 1024, 1024);
        System.arraycopy(randomBytes, 3 * 1024, expectedBytes, 2 * 1024, 1024);

        assertArrayEquals(expectedBytes, toByteArray(testFilePath));
    }

    private interface IoOperation<T, R> {
        R apply(FileIo fileIo, T t) throws IOException;
    }

    private interface IoOperationByPosition<T, R> {
        R apply(FileIo fileIo, long position, T t) throws IOException;
    }

    private static void writeBytes(Path filePath, byte[] bytes) throws Exception {
        Files.write(filePath, bytes, CREATE, WRITE);
    }

    private static byte[] randomByteArray(int len) {
        byte[] bytes = new byte[len];

        ThreadLocalRandom.current().nextBytes(bytes);

        return bytes;
    }

    private static byte[] toByteArray(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.limit()];

        buffer.rewind().get(bytes);

        return bytes;
    }

    private static byte[] toByteArray(Path filePath) throws Exception {
        return Files.readAllBytes(filePath);
    }

    private static ByteBuffer rangeBuffer(byte[] bytes, int from, int to) {
        return ByteBuffer.wrap(copyOfRange(bytes, from, to));
    }

    private static ByteBuffer sliceBuffer(ByteBuffer buffer, int from, int to) {
        return buffer.position(from).limit(to).slice();
    }
}
