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
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Arrays.copyOfRange;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
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
        byte[] randomByteArray = randomByteArray(4 * 1024);

        Path testFilePath = workDir.resolve("test");

        fillFile(testFilePath, randomByteArray);

        ByteBuffer buffer = ByteBuffer.allocate(1024);

        FileIo fileIo = fileIoFactory.create(testFilePath);

        for (int i = 0; i < 4; i++) {
            assertEquals(1024, fileIo.read(buffer.rewind()), "i=" + i);

            assertArrayEquals(copyOfRange(randomByteArray, i * 1024, (i + 1) * 1024), toByteArray(buffer), "i=" + i);
        }

        assertEquals(-1, fileIo.read(buffer.rewind()));
    }

    @Test
    void testReadByPosition() throws Exception {
        byte[] randomByteArray = randomByteArray(4 * 1024);

        Path testFilePath = workDir.resolve("test");

        fillFile(testFilePath, randomByteArray);

        ByteBuffer buffer = ByteBuffer.allocate(1024);

        FileIo fileIo = fileIoFactory.create(testFilePath);

        for (int i = 1; i < 3; i++) {
            assertEquals(1024, fileIo.read(buffer.rewind(), i * 1024), "i=" + i);

            assertArrayEquals(copyOfRange(randomByteArray, i * 1024, (i + 1) * 1024), toByteArray(buffer), "i=" + i);
        }

        assertEquals(-1, fileIo.read(buffer.rewind(), 4 * 1024));
    }

    @Test
    void testReadToByteArray() throws Exception {
        byte[] randomByteArray = randomByteArray(4 * 1024);

        Path testFilePath = workDir.resolve("test");

        fillFile(testFilePath, randomByteArray);

        byte[] buffer = new byte[1024];

        FileIo fileIo = fileIoFactory.create(testFilePath);

        for (int i = 0; i < 4; i++) {
            assertEquals(1024, fileIo.read(buffer, 0, 1024), "i=" + i);

            assertArrayEquals(copyOfRange(randomByteArray, i * 1024, (i + 1) * 1024), buffer, "i=" + i);
        }

        assertEquals(-1, fileIo.read(buffer, 0, 1024));
    }

    @Test
    void testReadFully() throws Exception {
        byte[] randomByteArray = randomByteArray(4 * 1024);

        Path testFilePath = workDir.resolve("test");

        fillFile(testFilePath, randomByteArray);

        ByteBuffer buffer = ByteBuffer.allocate(1024);

        FileIo fileIo = fileIoFactory.create(testFilePath);

        for (int i = 0; i < 4; i++) {
            assertEquals(1024, fileIo.readFully(buffer.rewind()), "i=" + i);

            assertArrayEquals(copyOfRange(randomByteArray, i * 1024, (i + 1) * 1024), toByteArray(buffer), "i=" + i);
        }

        assertEquals(-1, fileIo.readFully(buffer.rewind()));
    }

    @Test
    void testReadByPositionFully() throws Exception {
        byte[] randomByteArray = randomByteArray(4 * 1024);

        Path testFilePath = workDir.resolve("test");

        fillFile(testFilePath, randomByteArray);

        ByteBuffer buffer = ByteBuffer.allocate(1024);

        FileIo fileIo = fileIoFactory.create(testFilePath);

        for (int i = 1; i < 3; i++) {
            assertEquals(1024, fileIo.readFully(buffer.rewind(), i * 1024), "i=" + i);

            assertArrayEquals(copyOfRange(randomByteArray, i * 1024, (i + 1) * 1024), toByteArray(buffer), "i=" + i);
        }

        assertEquals(-1, fileIo.readFully(buffer.rewind(), 4 * 1024));
    }

    @Test
    void testReadToByteArrayFully() throws Exception {
        byte[] randomByteArray = randomByteArray(4 * 1024);

        Path testFilePath = workDir.resolve("test");

        fillFile(testFilePath, randomByteArray);

        byte[] buffer = new byte[1024];

        FileIo fileIo = fileIoFactory.create(testFilePath);

        for (int i = 0; i < 4; i++) {
            assertEquals(1024, fileIo.readFully(buffer, 0, 1024), "i=" + i);

            assertArrayEquals(copyOfRange(randomByteArray, i * 1024, (i + 1) * 1024), buffer, "i=" + i);
        }

        assertEquals(-1, fileIo.readFully(buffer, 0, 1024));
    }

    // TODO: IGNITE-16988 continue write tests

    private static void fillFile(Path filePath, byte[] bytes) throws Exception {
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
}
