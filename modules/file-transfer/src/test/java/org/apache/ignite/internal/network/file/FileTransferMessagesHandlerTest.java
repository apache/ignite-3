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

import static org.apache.ignite.internal.network.file.FileAssertions.assertContentEquals;
import static org.apache.ignite.internal.network.file.FileGenerator.randomFile;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.FilesUtils.sortByNames;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.internal.network.file.messages.FileChunkMessage;
import org.apache.ignite.internal.network.file.messages.FileHeaderMessage;
import org.apache.ignite.internal.network.file.messages.FileTransferInfoMessage;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.NetworkMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(WorkDirectoryExtension.class)
class FileTransferMessagesHandlerTest {
    private static final int CHUNK_SIZE = 1024; // 1 KB
    @WorkDirectory
    private static Path worDir;

    private static Stream<List<File>> files() {
        return Stream.of(
                List.of(randomFile(worDir, CHUNK_SIZE)),
                List.of(randomFile(worDir, CHUNK_SIZE - 1)),
                List.of(randomFile(worDir, CHUNK_SIZE + 1)),
                List.of(randomFile(worDir, 0)),
                List.of(
                        randomFile(worDir, 0),
                        randomFile(worDir, CHUNK_SIZE),
                        randomFile(worDir, CHUNK_SIZE - 1),
                        randomFile(worDir, CHUNK_SIZE + 1)
                )
        );
    }

    @ParameterizedTest
    @MethodSource("files")
    void handleMessagesInDirectOrder(List<File> files) throws IOException {
        UUID transferId = UUID.randomUUID();
        FileTransferMessagesHandler handler = new FileTransferMessagesHandler(createHandlerWorkDir(transferId));
        try (FileTransferMessagesStream stream = new FileTransferMessagesStream(UUID.randomUUID(), files, CHUNK_SIZE)) {
            stream.forEach(it -> sendToHandler(handler, it));
        }

        assertThat(
                handler.result().thenAccept(receivedFiles -> {
                    assertContentEquals(sortByNames(files), sortByNames(receivedFiles));
                }),
                willCompleteSuccessfully()
        );
    }

    @ParameterizedTest
    @MethodSource("files")
    void handleMessagesInReverseOrder(List<File> files) throws IOException {
        UUID transferId = UUID.randomUUID();
        FileTransferMessagesHandler handler = new FileTransferMessagesHandler(createHandlerWorkDir(transferId));
        List<NetworkMessage> messages = new ArrayList<>();
        try (FileTransferMessagesStream stream = new FileTransferMessagesStream(UUID.randomUUID(), files, CHUNK_SIZE)) {
            stream.forEach(messages::add);
        }

        for (int i = messages.size() - 1; i >= 0; i--) {
            sendToHandler(handler, messages.get(i));
        }

        assertThat(
                handler.result().thenAccept(receivedFiles -> {
                    assertContentEquals(sortByNames(files), sortByNames(receivedFiles));
                }),
                willCompleteSuccessfully()
        );
    }

    @Test
    void handleConcurrentMessages() throws IOException {
        UUID transferId = UUID.randomUUID();
        File file1 = randomFile(worDir, CHUNK_SIZE * 1024);
        File file2 = randomFile(worDir, CHUNK_SIZE * 768);
        File file3 = randomFile(worDir, CHUNK_SIZE * 512);
        FileTransferMessagesHandler handler = new FileTransferMessagesHandler(createHandlerWorkDir(transferId));
        List<NetworkMessage> messages = new ArrayList<>();
        List<File> files = List.of(file1, file2, file3);
        try (FileTransferMessagesStream stream = new FileTransferMessagesStream(UUID.randomUUID(), files, CHUNK_SIZE)) {
            stream.forEach(messages::add);
        }

        messages.parallelStream().forEach(it -> sendToHandler(handler, it));

        assertThat(
                handler.result().thenAccept(receivedFiles -> {
                    assertContentEquals(sortByNames(files), sortByNames(receivedFiles));
                }),
                willCompleteSuccessfully()
        );
    }

    private static Path createHandlerWorkDir(UUID transferId) throws IOException {
        Path directory = Files.createTempDirectory(worDir, transferId.toString());
        directory.toFile().deleteOnExit();
        return directory;
    }

    private static void sendToHandler(FileTransferMessagesHandler handler, NetworkMessage message) {
        if (message instanceof FileHeaderMessage) {
            handler.handleFileHeader((FileHeaderMessage) message);
        } else if (message instanceof FileChunkMessage) {
            handler.handleFileChunk((FileChunkMessage) message);
        } else if (message instanceof FileTransferInfoMessage) {
            handler.handleFileTransferInfo((FileTransferInfoMessage) message);
        } else {
            throw new IllegalArgumentException("Unknown message type: " + message.getClass());
        }
    }
}
