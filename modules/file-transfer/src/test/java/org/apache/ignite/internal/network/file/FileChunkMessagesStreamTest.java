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

import static org.apache.ignite.internal.network.file.FileGenerator.randomFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.ignite.internal.network.file.messages.FileChunkMessage;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
class FileChunkMessagesStreamTest {
    private static final int CHUNK_SIZE = 1024;

    @WorkDirectory
    private Path workDir;

    @Test
    void negativeChunkSize() {
        UUID transferId = UUID.randomUUID();
        Path file = randomFile(workDir, 1024);
        int chunkSize = -1;

        assertThrows(IllegalArgumentException.class, () -> FileChunkMessagesStream.fromPath(chunkSize, transferId, file));
    }

    @Test
    void emptyFile() throws IOException {
        UUID transferId = UUID.randomUUID();
        Path file = randomFile(workDir, 0);

        try (FileChunkMessagesStream stream = FileChunkMessagesStream.fromPath(CHUNK_SIZE, transferId, file)) {
            assertFalse(stream.hasNextMessage());
            assertThrows(IllegalStateException.class, stream::nextMessage);
        }
    }

    @Test
    void fileWithSizeLessThanChunkSize() throws IOException {
        UUID transferId = UUID.randomUUID();
        int fileSize = CHUNK_SIZE - 1;
        Path file = randomFile(workDir, fileSize);

        try (FileChunkMessagesStream stream = FileChunkMessagesStream.fromPath(CHUNK_SIZE, transferId, file)) {

            assertTrue(stream.hasNextMessage());
            FileChunkMessage fileChunkMessage = stream.nextMessage();
            assertEquals(transferId, fileChunkMessage.transferId());
            assertEquals(0, fileChunkMessage.number());
            assertEquals(fileSize, fileChunkMessage.data().length);

            assertFalse(stream.hasNextMessage());
        }
    }

    @Test
    void fileWithSizeEqualToChunkSize() throws IOException {
        UUID transferId = UUID.randomUUID();
        int fileSize = CHUNK_SIZE;
        Path file = randomFile(workDir, fileSize);

        try (FileChunkMessagesStream stream = FileChunkMessagesStream.fromPath(CHUNK_SIZE, transferId, file)) {

            assertTrue(stream.hasNextMessage());
            FileChunkMessage fileChunkMessage = stream.nextMessage();
            assertEquals(transferId, fileChunkMessage.transferId());
            assertEquals(0, fileChunkMessage.number());
            assertEquals(fileSize, fileChunkMessage.data().length);

            assertFalse(stream.hasNextMessage());
        }
    }

    @Test
    void fileWithSizeGreaterThanChunkSize() throws IOException {
        UUID transferId = UUID.randomUUID();
        int fileSize = CHUNK_SIZE + 1;
        Path file = randomFile(workDir, fileSize);

        try (FileChunkMessagesStream stream = FileChunkMessagesStream.fromPath(CHUNK_SIZE, transferId, file)) {

            assertTrue(stream.hasNextMessage());
            FileChunkMessage fileChunkMessage = stream.nextMessage();
            assertEquals(transferId, fileChunkMessage.transferId());
            assertEquals(0, fileChunkMessage.number());
            assertEquals(CHUNK_SIZE, fileChunkMessage.data().length);

            assertTrue(stream.hasNextMessage());
            fileChunkMessage = stream.nextMessage();
            assertEquals(transferId, fileChunkMessage.transferId());
            assertEquals(1, fileChunkMessage.number());
            assertEquals(1, fileChunkMessage.data().length);

            assertFalse(stream.hasNextMessage());
        }
    }
}
