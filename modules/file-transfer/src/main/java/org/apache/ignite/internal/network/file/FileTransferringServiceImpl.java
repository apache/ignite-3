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
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.configuration.FileTransferringConfiguration;
import org.apache.ignite.internal.network.file.messages.FileChunk;
import org.apache.ignite.internal.network.file.messages.FileDownloadRequest;
import org.apache.ignite.internal.network.file.messages.FileDownloadResponse;
import org.apache.ignite.internal.network.file.messages.FileHeader;
import org.apache.ignite.internal.network.file.messages.FileTransferFactory;
import org.apache.ignite.internal.network.file.messages.FileTransferInfo;
import org.apache.ignite.internal.network.file.messages.FileTransferringMessageType;
import org.apache.ignite.internal.network.file.messages.FileUploadRequest;
import org.apache.ignite.internal.network.file.messages.TransferMetadata;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.FilesUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ChannelType;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.annotations.Transferable;

/**
 * Implementation of {@link FileTransferringService}.
 */
public class FileTransferringServiceImpl implements FileTransferringService {
    private static final IgniteLogger LOG = Loggers.forClass(FileTransferringServiceImpl.class);

    private static final ChannelType FILE_TRANSFERRING_CHANNEL = ChannelType.register((short) 1, "FileTransferring");

    /**
     * Cluster service.
     */
    private final MessagingService messagingService;

    private final ExecutorService executorService;

    private final FileSender fileSender;

    private final FileReceiver fileReceiver;

    private final Map<Short, FileProvider<TransferMetadata>> metadataToProvider = new ConcurrentHashMap<>();

    private final Map<Short, FileHandler<TransferMetadata>> metadataToHandler = new ConcurrentHashMap<>();

    private final FileTransferFactory factory = new FileTransferFactory();

    /**
     * Constructor.
     *
     * @param messagingService Messaging service.
     * @param tempDirectory Temporary directory.
     */
    public FileTransferringServiceImpl(
            String nodeName,
            MessagingService messagingService,
            FileTransferringConfiguration configuration,
            Path tempDirectory
    ) {
        this.messagingService = messagingService;
        this.executorService = Executors.newFixedThreadPool(
                configuration.value().threadPoolSize(),
                NamedThreadFactory.create(nodeName, "file-transfer", LOG)
        );
        this.fileSender = new FileSender(
                configuration.value().chunkSize(),
                new RateLimiter(configuration.value().maxConcurrentRequests()),
                (recipientConsistentId, message) -> messagingService.send(recipientConsistentId, FILE_TRANSFERRING_CHANNEL, message),
                executorService
        );
        this.fileReceiver = new FileReceiver(tempDirectory, executorService);
    }

    @Override
    public void start() {
        messagingService.addMessageHandler(FileTransferringMessageType.class,
                (message, senderConsistentId, correlationId) -> {
                    if (message instanceof FileDownloadRequest) {
                        processDownloadRequest((FileDownloadRequest) message, senderConsistentId, correlationId);
                    } else if (message instanceof FileUploadRequest) {
                        processUploadRequest((FileUploadRequest) message);
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

    private void processUploadRequest(FileUploadRequest message) {
        fileReceiver.registerTransfer(message.transferId())
                .thenCompose(FileTransferringMessagesHandler::result)
                .thenCompose(path -> {
                    return metadataToHandler.get(message.metadata().messageType()).handleUpload(message.metadata(), path)
                            .whenComplete((v, e) -> {
                                if (e != null) {
                                    LOG.error("Failed to handle file upload. Metadata: {}", message.metadata(), e);
                                }

                                deleteDirectoryIfExists(path);
                            });
                });
    }

    private void processDownloadRequest(FileDownloadRequest message, String senderConsistentId, Long correlationId) {
        metadataToProvider.get(message.metadata().messageType()).files(message.metadata())
                .handle((files, e) -> {
                    if (e != null) {
                        LOG.error("Failed to get files for download. Metadata: {}", message.metadata(), e);
                        // todo send error response
                        return completedFuture(null);
                    }

                    FileDownloadResponse response = factory.fileDownloadResponse().build();
                    return messagingService.respond(senderConsistentId, FILE_TRANSFERRING_CHANNEL, response, correlationId)
                            .thenCompose(v -> sendFiles(senderConsistentId, message.transferId(), files));
                })
                .thenCompose(it -> it);
    }

    private CompletableFuture<Void> sendFiles(String recipientConsistentId, UUID transferId, List<File> files) {
        return fileSender.send(recipientConsistentId, transferId, files)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Failed to send files to node: {}. Exception: {}", recipientConsistentId, e);
                        // todo send error response
                    }
                });
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
    public void stop() throws Exception {
        IgniteUtils.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS);
        IgniteUtils.closeAllManually(Stream.of(fileSender, fileReceiver));
    }

    @Override
    public <M extends TransferMetadata> void addFileProvider(
            Class<M> metadata,
            FileProvider<M> provider
    ) {
        metadataToProvider.put(
                metadata.getAnnotation(Transferable.class).value(),
                (FileProvider<TransferMetadata>) provider
        );
    }

    @Override
    public <M extends TransferMetadata> void addFileHandler(
            Class<M> metadata,
            FileHandler<M> handler
    ) {
        metadataToHandler.put(
                metadata.getAnnotation(Transferable.class).value(),
                (FileHandler<TransferMetadata>) handler
        );
    }

    @Override
    public CompletableFuture<Path> download(String nodeConsistentId, TransferMetadata transferMetadata) {
        UUID transferId = UUID.randomUUID();
        FileDownloadRequest message = factory.fileDownloadRequest()
                .transferId(transferId)
                .metadata(transferMetadata)
                .build();

        return fileReceiver.registerTransfer(transferId)
                .thenCompose(handler -> messagingService.invoke(nodeConsistentId, FILE_TRANSFERRING_CHANNEL, message, Long.MAX_VALUE)
                        .thenApply(FileDownloadResponse.class::cast)
                        .thenCompose(response -> {
                            if (response.error() != null) {
                                // todo handle error
                                return failedFuture(new RuntimeException(response.error().errorMessage()));
                            }
                            return handler.result();
                        }));
    }

    @Override
    public CompletableFuture<Void> upload(String nodeConsistentId, TransferMetadata transferMetadata) {
        return metadataToProvider.get(transferMetadata.messageType()).files(transferMetadata)
                .thenCompose(files -> {
                    UUID transferId = UUID.randomUUID();
                    FileUploadRequest message = factory.fileUploadRequest()
                            .transferId(transferId)
                            .metadata(transferMetadata)
                            .build();

                    if (files.isEmpty()) {
                        return failedFuture(new FileTransferException("No files to upload"));
                    } else {
                        return messagingService.send(nodeConsistentId, FILE_TRANSFERRING_CHANNEL, message)
                                .thenCompose(v -> sendFiles(nodeConsistentId, transferId, files));
                    }
                });
    }

    private static void deleteDirectoryIfExists(Path directory) {
        try {
            FilesUtils.deleteDirectoryIfExists(directory);
        } catch (IOException e) {
            LOG.error("Failed to delete directory: {}. Exception: {}", directory, e);
        }
    }
}
