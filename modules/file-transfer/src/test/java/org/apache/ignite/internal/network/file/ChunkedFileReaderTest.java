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

package org.apache.ignite.internal.network.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
class ChunkedFileReaderTest {
    private static final int CHUNK_SIZE = 1024;

    @WorkDirectory
    private Path workDir;

    @Test
    void readEmptyFile() throws IOException {
        Path empty = FileGenerator.randomFile(workDir, 0);
        try (ChunkedFileReader reader = ChunkedFileReader.open(empty.toFile(), CHUNK_SIZE)) {
            assertFalse(reader.hasNextChunk());
            IOException exception = assertThrows(IOException.class, reader::readNextChunk);
            assertEquals("No more chunks to read", exception.getMessage());
        }
    }

    @Test
    void readFileWithLengthLessThanChunkSize() throws IOException {
        Path file = FileGenerator.randomFile(workDir, CHUNK_SIZE - 1);
        try (ChunkedFileReader reader = ChunkedFileReader.open(file.toFile(), CHUNK_SIZE)) {
            assertTrue(reader.hasNextChunk());
            assertEquals(CHUNK_SIZE - 1, reader.readNextChunk().length);
            assertFalse(reader.hasNextChunk());
            IOException exception = assertThrows(IOException.class, reader::readNextChunk);
            assertEquals("No more chunks to read", exception.getMessage());
        }
    }

    @Test
    void readFileWithLengthEqualToChunkSize() throws IOException {
        Path file = FileGenerator.randomFile(workDir, CHUNK_SIZE);
        try (ChunkedFileReader reader = ChunkedFileReader.open(file.toFile(), CHUNK_SIZE)) {
            assertTrue(reader.hasNextChunk());
            assertEquals(CHUNK_SIZE, reader.readNextChunk().length);
            assertFalse(reader.hasNextChunk());
            IOException exception = assertThrows(IOException.class, reader::readNextChunk);
            assertEquals("No more chunks to read", exception.getMessage());
        }
    }

    @Test
    void readFileWithLengthGreaterThanChunkSize() throws IOException {
        Path file = FileGenerator.randomFile(workDir, CHUNK_SIZE + 1);
        try (ChunkedFileReader reader = ChunkedFileReader.open(file.toFile(), CHUNK_SIZE)) {
            assertTrue(reader.hasNextChunk());
            assertEquals(CHUNK_SIZE, reader.readNextChunk().length);
            assertTrue(reader.hasNextChunk());
            assertEquals(1, reader.readNextChunk().length);
            assertFalse(reader.hasNextChunk());
            IOException exception = assertThrows(IOException.class, reader::readNextChunk);
            assertEquals("No more chunks to read", exception.getMessage());
        }
    }
}
