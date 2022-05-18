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

package org.apache.ignite.internal.fileio;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Arrays.copyOfRange;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * An abstract class for testing {@link FileIo} implementations.
 */
@ExtendWith(WorkDirectoryExtension.class)
public abstract class AbstractFileIoTest {
    protected FileIoFactory fileIoFactory;

    @WorkDirectory
    protected Path workDir;

    @Test
    abstract void testFileIoFactory() throws Exception;

    @Test
    void testPosition() throws Exception {
        FileIo fileIo = fileIoFactory.create(workDir.resolve("test"));

        assertEquals(0, fileIo.position());

        fileIo.position(100);

        assertEquals(100, fileIo.position());
    }

    @Test
    void testRead() throws Exception {
        byte[] randomBytes = randomByteArray(4 * 1024);

        Path testFilePath = workDir.resolve("test");

        writeBytes(testFilePath, randomBytes);

        ByteBuffer buffer = ByteBuffer.allocate(1024);

        FileIo fileIo = fileIoFactory.create(testFilePath);

        assertEquals(1024, fileIo.read(buffer.rewind()));
        assertArrayEquals(copyOfRange(randomBytes, 0, 1024), toByteArray(buffer));
        assertEquals(1024, fileIo.position());

        assertEquals(1024, fileIo.read(buffer.rewind()));
        assertArrayEquals(copyOfRange(randomBytes, 1024, 2 * 1024), toByteArray(buffer));
        assertEquals(2 * 1024, fileIo.position());

        assertEquals(1024, fileIo.read(buffer.rewind()));
        assertArrayEquals(copyOfRange(randomBytes, 2 * 1024, 3 * 1024), toByteArray(buffer));
        assertEquals(3 * 1024, fileIo.position());

        assertEquals(1024, fileIo.read(buffer.rewind()));
        assertArrayEquals(copyOfRange(randomBytes, 3 * 1024, 4 * 1024), toByteArray(buffer));
        assertEquals(4 * 1024, fileIo.position());

        assertEquals(-1, fileIo.read(buffer.rewind()));
        assertEquals(4 * 1024, fileIo.position());
    }

    @Test
    void testReadByPosition() throws Exception {
        byte[] randomBytes = randomByteArray(4 * 1024);

        Path testFilePath = workDir.resolve("test");

        writeBytes(testFilePath, randomBytes);

        ByteBuffer buffer = ByteBuffer.allocate(1024);

        FileIo fileIo = fileIoFactory.create(testFilePath);

        assertEquals(1024, fileIo.read(buffer.rewind(), 1024));
        assertArrayEquals(copyOfRange(randomBytes, 1024, 2 * 1024), toByteArray(buffer));
        assertEquals(0, fileIo.position());

        assertEquals(1024, fileIo.read(buffer.rewind(), 2 * 1024));
        assertArrayEquals(copyOfRange(randomBytes, 2 * 1024, 3 * 1024), toByteArray(buffer));
        assertEquals(0, fileIo.position());

        assertEquals(-1, fileIo.read(buffer.rewind(), 4 * 1024));
        assertEquals(0, fileIo.position());
    }

    @Test
    void testReadToByteArray() throws Exception {
        byte[] randomBytes = randomByteArray(4 * 1024);

        Path testFilePath = workDir.resolve("test");

        writeBytes(testFilePath, randomBytes);

        byte[] buffer = new byte[1024];

        FileIo fileIo = fileIoFactory.create(testFilePath);

        assertEquals(1024, fileIo.read(buffer, 0, 1024));
        assertArrayEquals(copyOfRange(randomBytes, 0, 1024), buffer);
        assertEquals(1024, fileIo.position());

        assertEquals(1024, fileIo.read(buffer, 0, 1024));
        assertArrayEquals(copyOfRange(randomBytes, 1024, 2 * 1024), buffer);
        assertEquals(2 * 1024, fileIo.position());

        assertEquals(1024, fileIo.read(buffer, 0, 1024));
        assertArrayEquals(copyOfRange(randomBytes, 2 * 1024, 3 * 1024), buffer);
        assertEquals(3 * 1024, fileIo.position());

        assertEquals(1024, fileIo.read(buffer, 0, 1024));
        assertArrayEquals(copyOfRange(randomBytes, 3 * 1024, 4 * 1024), buffer);
        assertEquals(4 * 1024, fileIo.position());

        assertEquals(-1, fileIo.read(buffer, 0, 1024));
        assertEquals(4 * 1024, fileIo.position());
    }

    @Test
    void testReadFully() throws Exception {
        byte[] randomBytes = randomByteArray(4 * 1024);

        Path testFilePath = workDir.resolve("test");

        writeBytes(testFilePath, randomBytes);

        ByteBuffer buffer = ByteBuffer.allocate(1024);

        FileIo fileIo = fileIoFactory.create(testFilePath);

        assertEquals(1024, fileIo.readFully(buffer.rewind()));
        assertArrayEquals(copyOfRange(randomBytes, 0, 1024), toByteArray(buffer));
        assertEquals(1024, fileIo.position());

        assertEquals(1024, fileIo.readFully(buffer.rewind()));
        assertArrayEquals(copyOfRange(randomBytes, 1024, 2 * 1024), toByteArray(buffer));
        assertEquals(2 * 1024, fileIo.position());

        assertEquals(1024, fileIo.readFully(buffer.rewind()));
        assertArrayEquals(copyOfRange(randomBytes, 2 * 1024, 3 * 1024), toByteArray(buffer));
        assertEquals(3 * 1024, fileIo.position());

        assertEquals(1024, fileIo.readFully(buffer.rewind()));
        assertArrayEquals(copyOfRange(randomBytes, 3 * 1024, 4 * 1024), toByteArray(buffer));
        assertEquals(4 * 1024, fileIo.position());

        assertEquals(-1, fileIo.readFully(buffer.rewind()));
        assertEquals(4 * 1024, fileIo.position());
    }

    @Test
    void testReadByPositionFully() throws Exception {
        byte[] randomBytes = randomByteArray(4 * 1024);

        Path testFilePath = workDir.resolve("test");

        writeBytes(testFilePath, randomBytes);

        ByteBuffer buffer = ByteBuffer.allocate(1024);

        FileIo fileIo = fileIoFactory.create(testFilePath);

        assertEquals(1024, fileIo.readFully(buffer.rewind(), 1024));
        assertArrayEquals(copyOfRange(randomBytes, 1024, 2 * 1024), toByteArray(buffer));
        assertEquals(0, fileIo.position());

        assertEquals(1024, fileIo.readFully(buffer.rewind(), 2 * 1024));
        assertArrayEquals(copyOfRange(randomBytes, 2 * 1024, 3 * 1024), toByteArray(buffer));
        assertEquals(0, fileIo.position());

        assertEquals(-1, fileIo.readFully(buffer.rewind(), 4 * 1024));
        assertEquals(0, fileIo.position());
    }

    @Test
    void testReadToByteArrayFully() throws Exception {
        byte[] randomBytes = randomByteArray(4 * 1024);

        Path testFilePath = workDir.resolve("test");

        writeBytes(testFilePath, randomBytes);

        byte[] buffer = new byte[1024];

        FileIo fileIo = fileIoFactory.create(testFilePath);

        assertEquals(1024, fileIo.readFully(buffer, 0, 1024));
        assertArrayEquals(copyOfRange(randomBytes, 0, 1024), buffer);
        assertEquals(1024, fileIo.position());

        assertEquals(1024, fileIo.readFully(buffer, 0, 1024));
        assertArrayEquals(copyOfRange(randomBytes, 1024, 2 * 1024), buffer);
        assertEquals(2 * 1024, fileIo.position());

        assertEquals(1024, fileIo.readFully(buffer, 0, 1024));
        assertArrayEquals(copyOfRange(randomBytes, 2 * 1024, 3 * 1024), buffer);
        assertEquals(3 * 1024, fileIo.position());

        assertEquals(1024, fileIo.readFully(buffer, 0, 1024));
        assertArrayEquals(copyOfRange(randomBytes, 3 * 1024, 4 * 1024), buffer);
        assertEquals(4 * 1024, fileIo.position());

        assertEquals(-1, fileIo.readFully(buffer, 0, 1024));
        assertEquals(4 * 1024, fileIo.position());
    }

    @Test
    void testWrite() throws Exception {
        byte[] randomBytes = randomByteArray(4 * 1024);

        Path testFilePath = workDir.resolve("test");

        FileIo fileIo = fileIoFactory.create(testFilePath);

        assertEquals(1024, fileIo.write(ByteBuffer.wrap(copyOfRange(randomBytes, 0, 1024))));
        assertEquals(1024, fileIo.position());

        assertEquals(1024, fileIo.write(ByteBuffer.wrap(copyOfRange(randomBytes, 1024, 2 * 1024))));
        assertEquals(2 * 1024, fileIo.position());

        assertEquals(1024, fileIo.write(ByteBuffer.wrap(copyOfRange(randomBytes, 2 * 1024, 3 * 1024))));
        assertEquals(3 * 1024, fileIo.position());

        assertEquals(1024, fileIo.write(ByteBuffer.wrap(copyOfRange(randomBytes, 3 * 1024, 4 * 1024))));
        assertEquals(4 * 1024, fileIo.position());

        assertEquals(0, fileIo.write(ByteBuffer.wrap(new byte[0])));
        assertEquals(4 * 1024, fileIo.position());

        fileIo.force();

        assertArrayEquals(randomBytes, toByteArray(testFilePath));
    }

    @Test
    void testWriteByPosition() throws Exception {
        byte[] randomBytes = randomByteArray(4 * 1024);

        Path testFilePath = workDir.resolve("test");

        FileIo fileIo = fileIoFactory.create(testFilePath);

        assertEquals(1024, fileIo.write(ByteBuffer.wrap(copyOfRange(randomBytes, 1024, 2 * 1024)), 1024));
        assertEquals(0, fileIo.position());

        assertEquals(1024, fileIo.write(ByteBuffer.wrap(copyOfRange(randomBytes, 3 * 1024, 4 * 1024)), 2 * 1024));
        assertEquals(0, fileIo.position());

        assertEquals(0, fileIo.write(ByteBuffer.wrap(new byte[0]), 3 * 1024));
        assertEquals(0, fileIo.position());

        fileIo.force();

        byte[] expectedBytes = new byte[3 * 1024];

        System.arraycopy(randomBytes, 1024, expectedBytes, 1024, 1024);
        System.arraycopy(randomBytes, 3 * 1024, expectedBytes, 2 * 1024, 1024);

        assertArrayEquals(expectedBytes, toByteArray(testFilePath));
    }

    @Test
    void testWriteFromByteArray() throws Exception {
        byte[] randomBytes = randomByteArray(4 * 1024);

        Path testFilePath = workDir.resolve("test");

        FileIo fileIo = fileIoFactory.create(testFilePath);

        assertEquals(1024, fileIo.write(randomBytes, 0, 1024));
        assertEquals(1024, fileIo.position());

        assertEquals(1024, fileIo.write(randomBytes, 1024, 1024));
        assertEquals(2 * 1024, fileIo.position());

        assertEquals(1024, fileIo.write(randomBytes, 2 * 1024, 1024));
        assertEquals(3 * 1024, fileIo.position());

        assertEquals(1024, fileIo.write(randomBytes, 3 * 1024, 1024));
        assertEquals(4 * 1024, fileIo.position());

        assertEquals(0, fileIo.write(randomBytes, 4 * 1024, 0));
        assertEquals(4 * 1024, fileIo.position());

        fileIo.force();

        assertArrayEquals(randomBytes, toByteArray(testFilePath));
    }

    @Test
    void testWriteFully() throws Exception {
        byte[] randomBytes = randomByteArray(4 * 1024);

        Path testFilePath = workDir.resolve("test");

        FileIo fileIo = fileIoFactory.create(testFilePath);

        assertEquals(1024, fileIo.writeFully(ByteBuffer.wrap(copyOfRange(randomBytes, 0, 1024))));
        assertEquals(1024, fileIo.position());

        assertEquals(1024, fileIo.writeFully(ByteBuffer.wrap(copyOfRange(randomBytes, 1024, 2 * 1024))));
        assertEquals(2 * 1024, fileIo.position());

        assertEquals(1024, fileIo.writeFully(ByteBuffer.wrap(copyOfRange(randomBytes, 2 * 1024, 3 * 1024))));
        assertEquals(3 * 1024, fileIo.position());

        assertEquals(1024, fileIo.writeFully(ByteBuffer.wrap(copyOfRange(randomBytes, 3 * 1024, 4 * 1024))));
        assertEquals(4 * 1024, fileIo.position());

        assertEquals(0, fileIo.writeFully(ByteBuffer.wrap(new byte[0])));
        assertEquals(4 * 1024, fileIo.position());

        fileIo.force();

        assertArrayEquals(randomBytes, toByteArray(testFilePath));
    }

    @Test
    void testWriteByPositionFully() throws Exception {
        byte[] randomBytes = randomByteArray(4 * 1024);

        Path testFilePath = workDir.resolve("test");

        FileIo fileIo = fileIoFactory.create(testFilePath);

        assertEquals(1024, fileIo.writeFully(ByteBuffer.wrap(copyOfRange(randomBytes, 1024, 2 * 1024)), 1024));
        assertEquals(0, fileIo.position());

        assertEquals(1024, fileIo.writeFully(ByteBuffer.wrap(copyOfRange(randomBytes, 3 * 1024, 4 * 1024)), 2 * 1024));
        assertEquals(0, fileIo.position());

        assertEquals(0, fileIo.writeFully(ByteBuffer.wrap(new byte[0]), 3 * 1024));
        assertEquals(0, fileIo.position());

        fileIo.force();

        byte[] expectedBytes = new byte[3 * 1024];

        System.arraycopy(randomBytes, 1024, expectedBytes, 1024, 1024);
        System.arraycopy(randomBytes, 3 * 1024, expectedBytes, 2 * 1024, 1024);

        assertArrayEquals(expectedBytes, toByteArray(testFilePath));
    }

    @Test
    void testWriteFromByteArrayFully() throws Exception {
        byte[] randomBytes = randomByteArray(4 * 1024);

        Path testFilePath = workDir.resolve("test");

        FileIo fileIo = fileIoFactory.create(testFilePath);

        assertEquals(1024, fileIo.writeFully(randomBytes, 0, 1024));
        assertEquals(1024, fileIo.position());

        assertEquals(1024, fileIo.writeFully(randomBytes, 1024, 1024));
        assertEquals(2 * 1024, fileIo.position());

        assertEquals(1024, fileIo.writeFully(randomBytes, 2 * 1024, 1024));
        assertEquals(3 * 1024, fileIo.position());

        assertEquals(1024, fileIo.writeFully(randomBytes, 3 * 1024, 1024));
        assertEquals(4 * 1024, fileIo.position());

        assertEquals(0, fileIo.writeFully(randomBytes, 4 * 1024, 0));
        assertEquals(4 * 1024, fileIo.position());

        fileIo.force();

        assertArrayEquals(randomBytes, toByteArray(testFilePath));
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

    @Test
    void testForce() throws Exception {
        Path testFilePath = workDir.resolve("test");

        FileIo fileIo = fileIoFactory.create(testFilePath);

        ByteBuffer randomBytes = ByteBuffer.wrap(randomByteArray(1024));

        assertEquals(1024, fileIo.writeFully(randomBytes));

        fileIo.force();

        assertArrayEquals(toByteArray(randomBytes), toByteArray(testFilePath));

        randomBytes = ByteBuffer.wrap(randomByteArray(1024));

        assertEquals(1024, fileIo.writeFully(randomBytes, 0));

        fileIo.force(true);

        assertArrayEquals(toByteArray(randomBytes), toByteArray(testFilePath));

        randomBytes = ByteBuffer.wrap(randomByteArray(1024));

        assertEquals(1024, fileIo.writeFully(randomBytes, 0));

        fileIo.force(false);

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

    @Test
    void testTransferTo() throws Exception {
        Path test0FilePath = workDir.resolve("test0");

        byte[] randomByteArray = randomByteArray(1024);

        writeBytes(test0FilePath, randomByteArray);

        FileIo fileIo = fileIoFactory.create(test0FilePath);

        Path test1FilePath = workDir.resolve("test1");

        try (FileChannel fh = FileChannel.open(test1FilePath, CREATE, WRITE, READ)) {
            assertEquals(512, fileIo.transferTo(512, 512, fh));
            assertEquals(0, fileIo.position());

            fh.force(true);
        }

        assertArrayEquals(copyOfRange(randomByteArray, 512, 1024), toByteArray(test1FilePath));
    }

    @Test
    void testTransferFrom() throws Exception {
        Path test0FilePath = workDir.resolve("test0");

        byte[] randomByteArray = randomByteArray(1024);

        writeBytes(test0FilePath, randomByteArray);

        Path test1FilePath = workDir.resolve("test1");

        FileIo fileIo = fileIoFactory.create(test1FilePath);

        try (FileChannel fh = FileChannel.open(test0FilePath, CREATE, WRITE, READ)) {
            assertEquals(0, fileIo.transferFrom(fh, 512, 512));
            assertEquals(0, fileIo.position());

            fileIo.writeFully(ByteBuffer.allocate(1024));

            assertEquals(512, fileIo.transferFrom(fh, 512, 512));
            assertEquals(1024, fileIo.position());

            fileIo.force();
        }

        byte[] expectedBytes = new byte[1024];

        System.arraycopy(randomByteArray, 0, expectedBytes, 512, 512);

        assertArrayEquals(expectedBytes, toByteArray(test1FilePath));
    }

    // TODO: IGNITE-16988 continue write tests

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
}
