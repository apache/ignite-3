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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.configuration.FileTransferringConfiguration;
import org.apache.ignite.internal.network.file.messages.FileChunk;
import org.apache.ignite.internal.network.file.messages.FileDownloadRequest;
import org.apache.ignite.internal.network.file.messages.FileDownloadResponse;
import org.apache.ignite.internal.network.file.messages.FileHeader;
import org.apache.ignite.internal.network.file.messages.FileTransferError;
import org.apache.ignite.internal.network.file.messages.FileTransferErrorMessage;
import org.apache.ignite.internal.network.file.messages.FileTransferFactory;
import org.apache.ignite.internal.network.file.messages.FileTransferInfo;
import org.apache.ignite.internal.network.file.messages.FileTransferringMessageType;
import org.apache.ignite.internal.network.file.messages.FileUploadRequest;
import org.apache.ignite.internal.network.file.messages.Metadata;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.FilesUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ChannelType;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.annotations.Transferable;

/**
 * Implementation of {@link FileTransferService}.
 */
public class FileTransferServiceImpl implements FileTransferService {
    private static final IgniteLogger LOG = Loggers.forClass(FileTransferServiceImpl.class);

    private static final ChannelType FILE_TRANSFERRING_CHANNEL = ChannelType.register((short) 1, "FileTransferring");

    private static final long DOWNLOAD_RESPONSE_TIMEOUT = 10_000;

    /**
     * Cluster service.
     */
    private final MessagingService messagingService;

    private final ExecutorService executorService;

    private final Path tempDirectory;

    private final FileSender fileSender;

    private final FileReceiver fileReceiver;

    private final Map<Short, FileProvider<Metadata>> metadataToProvider = new ConcurrentHashMap<>();

    private final Map<Short, FileHandler<Metadata>> metadataToHandler = new ConcurrentHashMap<>();

    private final FileTransferFactory factory = new FileTransferFactory();

    /**
     * Constructor.
     *
     * @param messagingService Messaging service.
     * @param tempDirectory Temporary directory.
     */
    public FileTransferServiceImpl(
            String nodeName,
            MessagingService messagingService,
            FileTransferringConfiguration configuration,
            Path tempDirectory
    ) {
        this.tempDirectory = tempDirectory;
        this.messagingService = messagingService;
        this.executorService = new ThreadPoolExecutor(
                0,
                configuration.value().threadPoolSize(),
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                NamedThreadFactory.create(nodeName, "file-transfer", LOG)
        );
        this.fileSender = new FileSender(
                configuration.value().chunkSize(),
                new RateLimiter(configuration.value().maxConcurrentRequests()),
                (recipientConsistentId, message) -> messagingService.send(recipientConsistentId, FILE_TRANSFERRING_CHANNEL, message),
                executorService
        );
        this.fileReceiver = new FileReceiver(executorService);
    }

    @Override
    public void start() {
        messagingService.addMessageHandler(FileTransferringMessageType.class,
                (message, senderConsistentId, correlationId) -> {
                    if (message instanceof FileDownloadRequest) {
                        processDownloadRequest((FileDownloadRequest) message, senderConsistentId, correlationId);
                    } else if (message instanceof FileUploadRequest) {
                        processUploadRequest((FileUploadRequest) message);
                    } else if (message instanceof FileTransferErrorMessage) {
                        processFileTransferErrorMessage((FileTransferErrorMessage) message);
                    } else if (message instanceof FileTransferInfo) {
                        processFileTransferInfo((FileTransferInfo) message);
                    } else if (message instanceof FileHeader) {
                        processFileHeader((FileHeader) message);
                    } else if (message instanceof FileChunk) {
                        processFileChunk((FileChunk) message);
                    } else {
                        LOG.error("Unexpected message received: {}", message);
                    }
                });
    }

    private void processFileTransferErrorMessage(FileTransferErrorMessage message) {
        fileReceiver.receiveFileTransferErrorMessage(message);
    }

    private void processUploadRequest(FileUploadRequest message) {
        Path directory = createTransferDirectory(message.transferId());
        FileTransferMessagesHandler handler = fileReceiver.registerTransfer(message.transferId(), directory);
        handler.result().thenCompose(files -> handleUploadedFiles(message.metadata(), files))
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Failed to handle uploaded files. Transfer ID: {}", message.transferId(), e);
                    }
                    fileReceiver.deregisterTransfer(message.transferId());
                    deleteDirectoryIfExists(directory);
                });
    }

    private CompletableFuture<Void> handleUploadedFiles(Metadata metadata, List<File> files) {
        return metadataToHandler.get(metadata.messageType()).handleUpload(metadata, files)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Failed to handle file upload. Metadata: {}", metadata, e);
                    }
                });
    }

    private void processDownloadRequest(FileDownloadRequest message, String senderConsistentId, Long correlationId) {
        metadataToProvider.get(message.metadata().messageType()).files(message.metadata())
                .whenComplete((files, e) -> {
                    if (e != null) {
                        LOG.error("Failed to get files for download. Metadata: {}", message.metadata(), e);
                        FileDownloadResponse errorResponse = factory.fileDownloadResponse()
                                .error(toError(e))
                                .build();
                        messagingService.respond(senderConsistentId, FILE_TRANSFERRING_CHANNEL, errorResponse, correlationId);
                    } else {
                        FileDownloadResponse response = factory.fileDownloadResponse().build();
                        messagingService.respond(senderConsistentId, FILE_TRANSFERRING_CHANNEL, response, correlationId)
                                .thenCompose(v -> sendFiles(senderConsistentId, message.transferId(), files));
                    }
                });
    }

    private CompletableFuture<Void> sendFiles(String recipientConsistentId, UUID transferId, List<File> files) {
        return fileSender.send(recipientConsistentId, transferId, files)
                .<CompletableFuture<Void>>handle((v, e) -> {
                    if (e != null) {
                        LOG.error("Failed to send files to node: {}. Exception: {}", recipientConsistentId, e);
                        FileTransferErrorMessage message = factory.fileTransferErrorMessage()
                                .transferId(transferId)
                                .error(toError(e))
                                .build();
                        messagingService.send(recipientConsistentId, FILE_TRANSFERRING_CHANNEL, message);
                        return failedFuture(new FileTransferException("Failed to send files to node: " + recipientConsistentId, e));
                    } else {
                        return completedFuture(null);
                    }
                }).thenCompose(it -> it);
    }

    private void processFileTransferInfo(FileTransferInfo info) {
        fileReceiver.receiveFileTransferInfo(info);
    }

    private void processFileHeader(FileHeader header) {
        fileReceiver.receiveFileHeader(header);
    }

    private void processFileChunk(FileChunk fileChunk) {
        fileReceiver.receiveFileChunk(fileChunk);
    }

    @Override
    public void stop() {
        IgniteUtils.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS);
    }

    @Override
    public <M extends Metadata> void addFileProvider(
            Class<M> metadata,
            FileProvider<M> provider
    ) {
        metadataToProvider.put(
                metadata.getAnnotation(Transferable.class).value(),
                (FileProvider<Metadata>) provider
        );
    }

    @Override
    public <M extends Metadata> void addFileHandler(
            Class<M> metadata,
            FileHandler<M> handler
    ) {
        metadataToHandler.put(
                metadata.getAnnotation(Transferable.class).value(),
                (FileHandler<Metadata>) handler
        );
    }

    @Override
    public CompletableFuture<List<File>> download(String nodeConsistentId, Metadata metadata) {
        UUID transferId = UUID.randomUUID();
        FileDownloadRequest message = factory.fileDownloadRequest()
                .transferId(transferId)
                .metadata(metadata)
                .build();

        return completedFuture(createTransferDirectory(transferId))
                .thenApply(directory -> fileReceiver.registerTransfer(transferId, directory))
                .thenCompose(
                        handler -> messagingService.invoke(nodeConsistentId, FILE_TRANSFERRING_CHANNEL, message, DOWNLOAD_RESPONSE_TIMEOUT)
                                .thenApply(FileDownloadResponse.class::cast)
                                .thenCompose(response -> {
                                    if (response.error() != null) {
                                        return failedFuture(new FileTransferException(response.error().message()));
                                    }
                                    return handler.result();
                                }));
    }

    @Override
    public CompletableFuture<Void> upload(String nodeConsistentId, Metadata metadata) {
        return metadataToProvider.get(metadata.messageType()).files(metadata)
                .thenCompose(files -> {
                    UUID transferId = UUID.randomUUID();
                    FileUploadRequest message = factory.fileUploadRequest()
                            .transferId(transferId)
                            .metadata(metadata)
                            .build();

                    if (files.isEmpty()) {
                        return failedFuture(new FileTransferException("No files to upload"));
                    } else {
                        return messagingService.send(nodeConsistentId, FILE_TRANSFERRING_CHANNEL, message)
                                .thenCompose(v -> sendFiles(nodeConsistentId, transferId, files));
                    }
                });
    }

    private Path createTransferDirectory(UUID transferId) {
        try {
            return Files.createDirectories(tempDirectory.resolve(transferId.toString()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void deleteDirectoryIfExists(Path directory) {
        try {
            FilesUtils.deleteDirectoryIfExists(directory);
        } catch (IOException e) {
            LOG.error("Failed to delete directory: {}. Exception: {}", directory, e);
        }
    }

    private FileTransferError toError(Throwable throwable) {
        return factory.fileTransferError()
                .message(unwrapFileTransferException(throwable).getMessage())
                .build();
    }

    private Throwable unwrapFileTransferException(Throwable throwable) {
        Throwable unwrapped = ExceptionUtils.unwrapCause(throwable);
        if (unwrapped instanceof FileTransferException) {
            return unwrapped.getCause();
        } else {
            return unwrapped;
        }
    }
}
