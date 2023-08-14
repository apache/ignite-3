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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
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
    void write(int length) throws IOException {
        Path pathToRead = FileGenerator.randomFile(workDir, length);
        Path pathToWrite = writerDir.resolve(pathToRead.getFileName());
        try (FileChunkMessagesStream stream = FileChunkMessagesStream.fromPath(CHUNK_SIZE, UUID.randomUUID(), pathToRead);
                ChunkedFileWriter writer = ChunkedFileWriter.open(pathToWrite.toFile(), length)) {

            AtomicBoolean isComplete = new AtomicBoolean(false);
            while (stream.hasNextMessage()) {
                if (writer.write(stream.nextMessage())) {
                    assertTrue(isComplete.compareAndSet(false, true));
                }
            }

            // The file should be written.
            assertThat(pathToWrite, PathMatcher.hasSameContentAndName(pathToRead));
        }
    }
}

