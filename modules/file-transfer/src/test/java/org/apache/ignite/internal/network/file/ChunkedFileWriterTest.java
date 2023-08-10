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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.internal.network.file.messages.FileChunkMessage;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.testframework.matchers.PathMatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(WorkDirectoryExtension.class)
class ChunkedFileWriterTest {
    private static final int CHUNK_SIZE = 1024;

    @WorkDirectory
    private Path workDir;

    private Path writerDir;

    private static Stream<Arguments> fileLengths() {
        return Stream.of(
                Arguments.of(0),
                Arguments.of(CHUNK_SIZE - 1),
                Arguments.of(CHUNK_SIZE),
                Arguments.of(CHUNK_SIZE + 1)
        );
    }

    @BeforeEach
    void setUp() throws IOException {
        writerDir = Files.createDirectories(workDir.resolve("writer"));
    }

    @ParameterizedTest
    @MethodSource("fileLengths")
    void writeWhenLengthIsKnown(int length) throws IOException {
        Path pathToRead = FileGenerator.randomFile(workDir, length);
        Path pathToWrite = writerDir.resolve(pathToRead.getFileName());
        try (FileChunkMessagesStream stream = FileChunkMessagesStream.fromPath(CHUNK_SIZE, UUID.randomUUID(), pathToRead);
                ChunkedFileWriter writer = ChunkedFileWriter.open(pathToWrite, length)) {
            while (stream.hasNextMessage()) {
                writer.write(stream.nextMessage());
            }

            // The writer is finished.
            assertTrue(writer.isFinished());

            // The file should be written.
            assertThat(pathToWrite, PathMatcher.hasSameContentAndName(pathToRead));
        }
    }

    @ParameterizedTest
    @MethodSource("fileLengths")
    void writeWhenLengthIsUnknown(int length) throws IOException {
        Path pathToRead = FileGenerator.randomFile(workDir, length);
        Path pathToWrite = writerDir.resolve(pathToRead.getFileName());
        try (FileChunkMessagesStream stream = FileChunkMessagesStream.fromPath(CHUNK_SIZE, UUID.randomUUID(), pathToRead);
                ChunkedFileWriter writer = ChunkedFileWriter.open(pathToWrite)) {
            while (stream.hasNextMessage()) {
                writer.write(stream.nextMessage());
            }

            // We don't know the length of the file, so we need to write the file size after all the chunks are written.
            assertFalse(writer.isFinished());

            // Write the file size.
            writer.fileSize(length);

            // Now the writer is finished.
            assertTrue(writer.isFinished());

            // The file should be written.
            assertThat(pathToWrite, PathMatcher.hasSameContentAndName(pathToRead));
        }
    }

    @ParameterizedTest
    @MethodSource("fileLengths")
    void writeChunksInReverseOrder(int length) throws IOException {
        Path pathToRead = FileGenerator.randomFile(workDir, length);
        Path pathToWrite = writerDir.resolve(pathToRead.getFileName());
        try (FileChunkMessagesStream stream = FileChunkMessagesStream.fromPath(CHUNK_SIZE, UUID.randomUUID(), pathToRead);
                ChunkedFileWriter writer = ChunkedFileWriter.open(pathToWrite)) {

            List<FileChunkMessage> chunks = new ArrayList<>();
            while (stream.hasNextMessage()) {
                chunks.add(stream.nextMessage());
            }

            // Write chunks in reverse order.
            for (int i = chunks.size() - 1; i >= 0; i--) {
                writer.write(chunks.get(i));
            }

            // We don't know the length of the file, so we need to write the file size after all the chunks are written.
            assertFalse(writer.isFinished());

            // Write the file size.
            writer.fileSize(length);

            // Now the writer is finished.
            assertTrue(writer.isFinished());

            // The file should be written.
            assertThat(pathToWrite, PathMatcher.hasSameContentAndName(pathToRead));
        }
    }
}

