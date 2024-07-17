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
import static org.apache.ignite.internal.network.file.PathAssertions.namesAndContentEquals;
import static org.apache.ignite.internal.network.file.messages.FileHeader.fromPaths;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.network.file.exception.FileTransferException;
import org.apache.ignite.internal.network.file.messages.FileTransferFactory;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
class FileReceiverTest {
    private static final int CHUNK_SIZE = 1024;

    @WorkDirectory
    private Path workDir;

    private final FileTransferFactory messageFactory = new FileTransferFactory();

    @Test
    void receiveSingleFile() throws IOException {
        // When a single file is sent.
        List<Path> filesToSend = List.of(randomFile(workDir, CHUNK_SIZE));
        FileReceiver receiver = new FileReceiver();
        UUID transferId = UUID.randomUUID();

        Path path = Files.createDirectory(workDir.resolve(transferId.toString()));

        TransferredFilesCollector collector = receiver.registerTransfer(
                "node2",
                transferId,
                fromPaths(messageFactory, filesToSend),
                path
        );

        sendFilesToReceiver(receiver, transferId, filesToSend);

        // Then the file is received.
        assertThat(
                collector.collectedFiles(),
                willBe(namesAndContentEquals(filesToSend))
        );
    }

    @Test
    void receiveMultipleFiles() throws IOException {
        // When multiple files are sent.
        List<Path> filesToSend = List.of(
                randomFile(workDir, CHUNK_SIZE),
                randomFile(workDir, CHUNK_SIZE * 2),
                randomFile(workDir, CHUNK_SIZE * 3)
        );
        FileReceiver receiver = new FileReceiver();
        UUID transferId = UUID.randomUUID();

        Path path = Files.createDirectory(workDir.resolve(transferId.toString()));

        TransferredFilesCollector collector = receiver.registerTransfer(
                "node2",
                transferId,
                fromPaths(messageFactory, filesToSend),
                path
        );

        sendFilesToReceiver(receiver, transferId, filesToSend);

        // Then the files are received.
        assertThat(
                collector.collectedFiles(),
                willBe(namesAndContentEquals(filesToSend))
        );
    }

    @Test
    void transfersCanceled() throws IOException {
        // When.
        FileReceiver receiver = new FileReceiver();

        // And the first file transfer is started.
        UUID transferId1 = UUID.randomUUID();
        List<Path> filesToSend1 = List.of(randomFile(workDir, CHUNK_SIZE * 2));

        Path path1 = Files.createDirectory(workDir.resolve(transferId1.toString()));

        TransferredFilesCollector collector1 = receiver.registerTransfer(
                "node2",
                transferId1,
                fromPaths(messageFactory, filesToSend1),
                path1
        );

        // And the second file transfer is registered.
        UUID transferId2 = UUID.randomUUID();

        Path path2 = Files.createDirectory(workDir.resolve(transferId2.toString()));

        TransferredFilesCollector collector2 = receiver.registerTransfer("node2", transferId2, List.of(), path2);

        // And the third file transfer from another node is started.
        UUID transferId3 = UUID.randomUUID();
        List<Path> filesToSend3 = List.of(randomFile(workDir, CHUNK_SIZE));

        Path path3 = Files.createDirectory(workDir.resolve(transferId3.toString()));

        TransferredFilesCollector collector3 = receiver.registerTransfer(
                "node3",
                transferId3,
                fromPaths(messageFactory, filesToSend3),
                path3
        );

        sendFilesToReceiver(receiver, transferId3, filesToSend3);

        // All transfers from node2 are canceled.
        receiver.cancelTransfersFromSender("node2");

        // Then the file transfer is canceled.
        assertThat(
                collector1.collectedFiles(),
                willThrow(FileTransferException.class)
        );

        // And the second file transfer is canceled.
        assertThat(
                collector2.collectedFiles(),
                willThrow(FileTransferException.class)
        );

        // And the third file transfer is not canceled.
        assertThat(
                collector3.collectedFiles(),
                willCompleteSuccessfully()
        );
    }

    private static void sendFilesToReceiver(FileReceiver receiver, UUID transferId, List<Path> files) {
        files.forEach(file -> {
            try (FileChunkMessagesStream stream = FileChunkMessagesStream.fromPath(CHUNK_SIZE, transferId, file)) {
                stream.forEach(receiver::receiveFileChunk);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
