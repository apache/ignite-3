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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutIn;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.FilesUtils.sortByNames;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.network.file.exception.FileTransferException;
import org.apache.ignite.internal.network.file.messages.FileChunkMessage;
import org.apache.ignite.internal.network.file.messages.FileHeaderMessage;
import org.apache.ignite.internal.network.file.messages.FileTransferInfoMessage;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.NetworkMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
class FileReceiverTest {
    private static final int CHUNK_SIZE = 1024;

    @WorkDirectory
    private Path workDir;

    @Test
    void receiveSingleFile() throws IOException {
        // when a single file is sent
        List<File> filesToSend = List.of(randomFile(workDir, CHUNK_SIZE));
        FileReceiver receiver = new FileReceiver("node1", 10);
        UUID transferId = UUID.randomUUID();

        Path path = Files.createDirectory(workDir.resolve(transferId.toString()));
        path.toFile().deleteOnExit();

        FileTransferMessagesHandler handler = receiver.registerTransfer("node2", transferId, path);
        try (FileTransferMessagesStream stream = new FileTransferMessagesStream(transferId, filesToSend, CHUNK_SIZE)) {
            stream.forEach(message -> sendMessageToReceiver(receiver, message));
        }

        // then the file is received
        assertThat(
                handler.result().thenAccept(files -> {
                    assertContentEquals(sortByNames(filesToSend), sortByNames(files));
                }),
                willCompleteSuccessfully()
        );
    }

    @Test
    void receiveMultipleFiles() throws IOException {
        // when multiple files are sent
        List<File> filesToSend = List.of(
                randomFile(workDir, CHUNK_SIZE),
                randomFile(workDir, CHUNK_SIZE * 2),
                randomFile(workDir, CHUNK_SIZE * 3)
        );
        FileReceiver receiver = new FileReceiver("node1", 10);
        UUID transferId = UUID.randomUUID();

        Path path = Files.createDirectory(workDir.resolve(transferId.toString()));
        path.toFile().deleteOnExit();

        FileTransferMessagesHandler handler = receiver.registerTransfer("node2", transferId, path);
        try (FileTransferMessagesStream stream = new FileTransferMessagesStream(transferId, filesToSend, CHUNK_SIZE)) {
            stream.forEach(message -> sendMessageToReceiver(receiver, message));
        }
        // then the files are received
        assertThat(
                handler.result().thenAccept(files -> {
                    assertContentEquals(sortByNames(filesToSend), sortByNames(files));
                }),
                willCompleteSuccessfully()
        );
    }

    @Test
    void transfersCanceled() throws IOException {
        // when
        FileReceiver receiver = new FileReceiver("node1", 10);

        // and the first file transfer is started
        UUID transferId1 = UUID.randomUUID();
        List<File> filesToSend1 = List.of(randomFile(workDir, CHUNK_SIZE));

        Path path1 = Files.createDirectory(workDir.resolve(transferId1.toString()));
        path1.toFile().deleteOnExit();

        FileTransferMessagesHandler handler1 = receiver.registerTransfer("node2", transferId1, path1);
        try (FileTransferMessagesStream stream = new FileTransferMessagesStream(transferId1, filesToSend1, CHUNK_SIZE)) {
            sendMessageToReceiver(receiver, stream.nextMessage());
        }

        // and the second file transfer is registered
        UUID transferId2 = UUID.randomUUID();

        Path path2 = Files.createDirectory(workDir.resolve(transferId2.toString()));
        path2.toFile().deleteOnExit();

        FileTransferMessagesHandler handler2 = receiver.registerTransfer("node2", transferId2, path2);

        // and the third file transfer from another node is started
        UUID transferId3 = UUID.randomUUID();
        List<File> filesToSend3 = List.of(randomFile(workDir, CHUNK_SIZE));

        Path path3 = Files.createDirectory(workDir.resolve(transferId3.toString()));
        path3.toFile().deleteOnExit();

        FileTransferMessagesHandler handler3 = receiver.registerTransfer("node3", transferId3, path3);
        try (FileTransferMessagesStream stream = new FileTransferMessagesStream(transferId3, filesToSend3, CHUNK_SIZE)) {
            sendMessageToReceiver(receiver, stream.nextMessage());
        }

        // all transfers from node2 are canceled
        receiver.cancelTransfersFromSender("node2");

        // then the file transfer is canceled
        assertThat(
                handler1.result(),
                willThrow(FileTransferException.class)
        );

        // and the second file transfer is canceled
        assertThat(
                handler2.result(),
                willThrow(FileTransferException.class)
        );

        // and the third file transfer is not canceled
        assertThat(
                handler3.result(),
                willTimeoutIn(250, TimeUnit.MILLISECONDS)
        );
    }

    private static void sendMessageToReceiver(FileReceiver receiver, NetworkMessage msg) {
        if (msg instanceof FileHeaderMessage) {
            receiver.receiveFileHeader((FileHeaderMessage) msg);
        } else if (msg instanceof FileTransferInfoMessage) {
            receiver.receiveFileTransferInfo((FileTransferInfoMessage) msg);
        } else if (msg instanceof FileChunkMessage) {
            receiver.receiveFileChunk((FileChunkMessage) msg);
        } else {
            throw new IllegalArgumentException("Unknown message type: " + msg);
        }
    }
}
