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

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.network.file.Channel.FILE_TRANSFER_CHANNEL;
import static org.apache.ignite.internal.network.file.messages.FileTransferError.fromThrowable;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.network.file.messages.FileDownloadRequest;
import org.apache.ignite.internal.network.file.messages.FileDownloadResponse;
import org.apache.ignite.internal.network.file.messages.FileHeader;
import org.apache.ignite.internal.network.file.messages.FileTransferErrorMessageImpl;
import org.apache.ignite.internal.network.file.messages.FileTransferFactory;
import org.apache.ignite.internal.network.file.messages.FileTransferInitMessage;
import org.apache.ignite.internal.network.file.messages.FileTransferInitResponse;
import org.apache.ignite.internal.network.file.messages.FileTransferMessageType;
import org.apache.ignite.internal.network.file.messages.Identifier;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(WorkDirectoryExtension.class)
class FileTransferServiceImplTest extends BaseIgniteAbstractTest {
    private static final String SOURCE_CONSISTENT_ID = "source";

    private static final String TARGET_CONSISTENT_ID = "target";

    private static final InternalClusterNode TARGET_NODE = new ClusterNodeImpl(
            randomUUID(),
            TARGET_CONSISTENT_ID,
            new NetworkAddress("target", 1234)
    );

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

        assertThat(fileTransferService.startAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() {
        assertThat(fileTransferService.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @Test
    void handlersAreAddedOnStart() {
        verify(messagingService).addMessageHandler(eq(FileTransferMessageType.class), any(NetworkMessageHandler.class));
        verify(topologyService).addEventHandler(any());
    }

    @Test
    void fileTransfersCanceledWhenSenderLeft() {
        topologyService.fireDisappearedEvent(new ClusterNodeImpl(randomUUID(), "sender", new NetworkAddress("localhost", 1234)));

        verify(fileReceiver).cancelTransfersFromSender("sender");
    }

    @Test
    void uploadFailsWhenInvokeReturnsResponseWithErrorOnFileTransferInitMessage() {
        // Set messaging service to fail to invoke.
        FileTransferInitResponse response = messageFactory.fileTransferInitResponse()
                .error(fromThrowable(messageFactory, new RuntimeException("Test exception")))
                .build();
        doReturn(completedFuture(response))
                .when(messagingService)
                .invoke(eq(TARGET_CONSISTENT_ID), eq(FILE_TRANSFER_CHANNEL), any(FileTransferInitMessage.class), anyLong());

        // Set file provider to provide a file.
        Path path1 = FileGenerator.randomFile(workDir, 100);
        fileTransferService.addFileProvider(Identifier.class, id -> completedFuture(List.of(path1)));

        // Upload file to target.
        CompletableFuture<Void> uploaded = fileTransferService.upload(TARGET_CONSISTENT_ID, messageFactory.identifier().build());

        // Check that upload failed.
        assertThat(uploaded, willThrow(RuntimeException.class, "Test exception"));
    }

    @Test
    void uploadFailsWhenSenderReturnsException() {
        // Set messaging service to fail to invoke.
        FileTransferInitResponse fileTransferInitResponse = messageFactory.fileTransferInitResponse().build();
        doReturn(completedFuture(fileTransferInitResponse))
                .when(messagingService)
                .invoke(eq(TARGET_CONSISTENT_ID), eq(FILE_TRANSFER_CHANNEL), any(FileTransferInitMessage.class), anyLong());

        doReturn(failedFuture(new RuntimeException("Test exception")))
                .when(fileSender)
                .send(eq(TARGET_CONSISTENT_ID), any(UUID.class), anyList());

        // Set file provider to provide a file.
        Path path1 = FileGenerator.randomFile(workDir, 100);
        fileTransferService.addFileProvider(Identifier.class, id -> completedFuture(List.of(path1)));

        // Upload file to target.
        CompletableFuture<Void> uploaded = fileTransferService.upload(TARGET_CONSISTENT_ID, messageFactory.identifier().build());

        // Check that upload failed.
        assertThat(uploaded, willThrow(RuntimeException.class, "Test exception"));

        // Check that error message was sent.
        await().untilAsserted(() -> {
            verify(messagingService, atLeastOnce())
                    .send(eq(TARGET_CONSISTENT_ID), eq(FILE_TRANSFER_CHANNEL), any(FileTransferErrorMessageImpl.class));
        });
    }

    @Test
    void uploadFailsWhenInvokeReturnsExceptionOnFileTransferInitResponse() {
        long correlationId = 1L;

        // Set messaging service to fail to send upload response.
        RuntimeException testException = new RuntimeException("Test exception");
        doReturn(failedFuture(testException))
                .when(messagingService)
                .respond(eq(TARGET_CONSISTENT_ID), eq(FILE_TRANSFER_CHANNEL), any(FileTransferInitResponse.class),
                        eq(correlationId));

        // Set file receiver to complete transfer registration.
        CompletableFuture<UUID> registeredTransferIdFuture = new CompletableFuture<>();
        CompletableFuture<List<Path>> transferredFilesFuture = new CompletableFuture<>();
        doAnswer(invocation -> {
            registeredTransferIdFuture.complete(invocation.getArgument(1, UUID.class));
            return transferredFilesFuture;
        }).when(fileReceiver).registerTransfer(eq(TARGET_CONSISTENT_ID), any(UUID.class), anyList(), any(Path.class));

        // Set file receiver to complete transfer cancellation.
        CompletableFuture<UUID> candeledTransferIdFuture = new CompletableFuture<>();
        doAnswer(invocation -> {
            candeledTransferIdFuture.complete(invocation.getArgument(0, UUID.class));
            transferredFilesFuture.completeExceptionally(invocation.getArgument(1, RuntimeException.class));
            return null;
        }).when(fileReceiver).cancelTransfer(any(UUID.class), any(RuntimeException.class));

        Path path1 = FileGenerator.randomFile(workDir, 0);
        List<Path> paths = List.of(path1);

        FileTransferInitMessage uploadRequest = messageFactory.fileTransferInitMessage()
                .transferId(randomUUID())
                .identifier(messageFactory.identifier().build())
                .headers(FileHeader.fromPaths(messageFactory, paths))
                .build();

        messagingService.fireMessage(uploadRequest, TARGET_NODE, correlationId);

        // Check that transfer was registered and canceled.
        assertThat(registeredTransferIdFuture, willCompleteSuccessfully());
        assertThat(candeledTransferIdFuture, willBe(registeredTransferIdFuture.join()));

        // Check that transfer directory is empty.
        await().untilAsserted(() -> assertThat(transferDir.toFile().listFiles(), emptyArray()));
    }

    @Test
    void downloadFailsWhenInvokeReturnsExceptionOnDownloadRequest() {
        // Set messaging service to fail to send download request.
        RuntimeException testException = new RuntimeException("Test exception");
        doReturn(failedFuture(testException))
                .when(messagingService)
                .invoke(eq(SOURCE_CONSISTENT_ID), eq(FILE_TRANSFER_CHANNEL), any(FileDownloadRequest.class), any(Long.class));

        CompletableFuture<List<Path>> downloaded = fileTransferService.download(SOURCE_CONSISTENT_ID, messageFactory.identifier().build(),
                workDir.resolve("download"));

        // Check that download future is completed exceptionally.
        assertThat(downloaded, willThrow(RuntimeException.class, "Test exception"));

        // Check that transfer directory is empty.
        await().untilAsserted(() -> assertThat(transferDir.toFile().listFiles(), nullValue()));
    }

    @Test
    void downloadFailsWhenInvokeReturnsResponseWithErrorOnDownloadRequest() {
        // Set messaging service to fail to send download request.
        FileDownloadResponse response = messageFactory.fileDownloadResponse()
                .error(fromThrowable(messageFactory, new RuntimeException("Test exception")))
                .build();
        doReturn(completedFuture(response))
                .when(messagingService)
                .invoke(eq(SOURCE_CONSISTENT_ID), eq(FILE_TRANSFER_CHANNEL), any(FileDownloadRequest.class), any(Long.class));

        CompletableFuture<List<Path>> downloaded = fileTransferService.download(SOURCE_CONSISTENT_ID, messageFactory.identifier().build(),
                workDir.resolve("download"));

        // Check that download future is completed exceptionally.
        assertThat(downloaded, willThrow(RuntimeException.class, "Test exception"));

        // Check that transfer directory is empty.
        await().untilAsserted(() -> assertThat(transferDir.toFile().listFiles(), nullValue()));
    }

    @Test
    void transferIsRegisteredBeforeResponseIsSent() {
        // Set file receiver to complete transfer registration.
        AtomicInteger transferLifecycleState = new AtomicInteger(0);
        doAnswer(invocation -> {
            // Update transfer lifecycle state - transfer is registered.
            transferLifecycleState.compareAndSet(0, 1);
            return (TransferredFilesCollector) CompletableFutures::emptyListCompletedFuture;
        }).when(fileReceiver).registerTransfer(anyString(), any(UUID.class), anyList(), any(Path.class));

        // Set messaging service to fail to send upload response if transfer is not registered.
        doAnswer(invocation -> {
            // Check lifecycle state - transfer is registered.
            transferLifecycleState.compareAndSet(1, 2);
            return nullCompletedFuture();
        }).when(messagingService).respond(anyString(), eq(FILE_TRANSFER_CHANNEL), any(FileTransferInitResponse.class), anyLong());

        // Create file transfer request.
        Path path1 = FileGenerator.randomFile(workDir, 0);
        List<Path> paths = List.of(path1);

        FileTransferInitMessage fileTransferInitMessage = messageFactory.fileTransferInitMessage()
                .transferId(randomUUID())
                .identifier(messageFactory.identifier().build())
                .headers(FileHeader.fromPaths(messageFactory, paths))
                .build();

        // Send file transfer request.
        messagingService.fireMessage(fileTransferInitMessage, TARGET_NODE, 1L);

        // Check that transfer was registered before response was sent.
        await().untilAtomic(transferLifecycleState, equalTo(2));
    }
}
