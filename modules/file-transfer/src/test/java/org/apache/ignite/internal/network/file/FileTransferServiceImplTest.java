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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.PathMatcher.isEmptyDirectory;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.network.file.messages.FileDownloadRequest;
import org.apache.ignite.internal.network.file.messages.FileHeader;
import org.apache.ignite.internal.network.file.messages.FileTransferFactory;
import org.apache.ignite.internal.network.file.messages.FileTransferMessageType;
import org.apache.ignite.internal.network.file.messages.FileUploadRequest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessageHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(WorkDirectoryExtension.class)
class FileTransferServiceImplTest {
    @WorkDirectory
    private Path workDir;

    private Path transferDir;

    @Mock
    private FileSender fileSender;

    @Mock
    private FileReceiver fileReceiver;

    @Spy
    private TestMessagingService messagingService = new TestMessagingService();

    @Spy
    private TestTopologyService topologyService = new TestTopologyService();

    private FileTransferServiceImpl fileTransferService;

    private final FileTransferFactory messageFactory = new FileTransferFactory();

    @BeforeEach
    void setUp() {
        transferDir = workDir.resolve("transfer");
        fileTransferService = new FileTransferServiceImpl(
                10_000,
                topologyService,
                messagingService,
                transferDir,
                fileSender,
                fileReceiver,
                Executors.newSingleThreadExecutor()
        );

        fileTransferService.start();
    }

    @AfterEach
    void tearDown() {
        fileTransferService.stop();
    }

    @Test
    void handlersAreAddedOnStart() {
        verify(messagingService).addMessageHandler(eq(FileTransferMessageType.class), any(NetworkMessageHandler.class));
        verify(topologyService).addEventHandler(any());
    }

    @Test
    void fileTransfersCanceledWhenSenderLeft() {
        topologyService.fairDisappearedEvent(new ClusterNodeImpl("node1", "sender", new NetworkAddress("localhost", 1234)));

        verify(fileReceiver).cancelTransfersFromSender("sender");
    }

    @Test
    void fileTransferIsCanceledWhenFailsToSendUploadResponse() {
        String targetConsistentId = "target";
        long correlationId = 1L;

        // Set messaging service to fail to send upload response.
        RuntimeException testException = new RuntimeException("Test exception");
        doReturn(failedFuture(testException))
                .when(messagingService).respond(eq(targetConsistentId), eq(Channel.FILE_TRANSFER_CHANNEL), any(), eq(correlationId));

        // Set file receiver to complete transfer registration.
        CompletableFuture<UUID> transferRegistered = new CompletableFuture<>();
        doAnswer(invocation -> {
            transferRegistered.complete(invocation.getArgument(1, UUID.class));
            return new CompletableFuture<>();
        }).when(fileReceiver).registerTransfer(eq(targetConsistentId), any(UUID.class), any(Path.class));

        // Set file receiver to complete transfer cancellation.
        CompletableFuture<UUID> transferCanceled = new CompletableFuture<>();
        doAnswer(invocation -> {
            transferCanceled.complete(invocation.getArgument(0, UUID.class));
            return null;
        }).when(fileReceiver).cancelTransfer(any(UUID.class), any(RuntimeException.class));

        Path path1 = FileGenerator.randomFile(workDir, 0);
        List<Path> paths = List.of(path1);

        FileUploadRequest uploadRequest = messageFactory.fileUploadRequest()
                .identifier(messageFactory.identifier().build())
                .headers(FileHeader.fromPaths(messageFactory, paths))
                .build();

        messagingService.fairMessage(uploadRequest, targetConsistentId, correlationId);

        // Check that transfer was registered and canceled.
        assertThat(transferRegistered, willCompleteSuccessfully());
        assertThat(transferCanceled, willBe(transferRegistered.join()));

        // Check that transfer directory is empty.
        await().until(() -> transferDir, isEmptyDirectory());
    }

    @Test
    void fileTransferIsCanceledWhenFailsToSendDownloadResponse() {
        String sourceConsistentId = "source";

        // Set messaging service to fail to send download request.
        RuntimeException testException = new RuntimeException("Test exception");
        doReturn(failedFuture(testException))
                .when(messagingService)
                .invoke(eq(sourceConsistentId), eq(Channel.FILE_TRANSFER_CHANNEL), any(FileDownloadRequest.class), any(Long.class));

        // Set file receiver to complete transfer registration.
        CompletableFuture<List<Path>> registeredTransfer = new CompletableFuture<>();
        CompletableFuture<UUID> transferId = new CompletableFuture<>();
        doAnswer(invocation -> {
            transferId.complete(invocation.getArgument(1, UUID.class));
            return registeredTransfer;
        }).when(fileReceiver).registerTransfer(eq(sourceConsistentId), any(UUID.class), any(Path.class));

        // Set file receiver to complete transfer cancellation.
        CompletableFuture<UUID> transferCanceled = new CompletableFuture<>();
        doAnswer(invocation -> {
            transferCanceled.complete(invocation.getArgument(0, UUID.class));
            registeredTransfer.completeExceptionally(invocation.getArgument(1, Exception.class));
            return null;
        }).when(fileReceiver).cancelTransfer(any(UUID.class), any(RuntimeException.class));

        CompletableFuture<List<Path>> downloaded = fileTransferService.download(sourceConsistentId, messageFactory.identifier().build(),
                workDir.resolve("download"));

        // Check that download future is completed exceptionally.
        assertThat(downloaded, willThrow(RuntimeException.class));

        // Check that transfer was registered and canceled.
        assertThat(transferId, willCompleteSuccessfully());
        assertThat(transferCanceled, willBe(transferId.join()));

        // Check that transfer directory is empty.
        await().until(() -> transferDir, isEmptyDirectory());
    }
}
