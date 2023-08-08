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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.configuration.FileTransferConfiguration;
import org.apache.ignite.internal.network.file.messages.FileChunkMessage;
import org.apache.ignite.internal.network.file.messages.FileDownloadRequest;
import org.apache.ignite.internal.network.file.messages.FileDownloadResponse;
import org.apache.ignite.internal.network.file.messages.FileHeaderMessage;
import org.apache.ignite.internal.network.file.messages.FileTransferErrorMessage;
import org.apache.ignite.internal.network.file.messages.FileTransferFactory;
import org.apache.ignite.internal.network.file.messages.FileTransferInfoMessage;
import org.apache.ignite.internal.network.file.messages.FileTransferMessageType;
import org.apache.ignite.internal.network.file.messages.FileUploadRequest;
import org.apache.ignite.internal.network.file.messages.FileUploadResponse;
import org.apache.ignite.internal.network.file.messages.Metadata;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.FilesUtils;
import org.apache.ignite.network.ChannelType;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.network.annotations.Transferable;
import org.jetbrains.annotations.TestOnly;

/**
 * Implementation of {@link FileTransferService}.
 */
public class FileTransferServiceImpl implements FileTransferService {
    private static final IgniteLogger LOG = Loggers.forClass(FileTransferServiceImpl.class);

    private static final ChannelType FILE_TRANSFERRING_CHANNEL = ChannelType.register((short) 1, "FileTransferring");

    private static final long RESPONSE_TIMEOUT = 10_000;

    private final TopologyService topologyService;
    /**
     * Cluster service.
     */
    private final MessagingService messagingService;

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
            TopologyService topologyService,
            MessagingService messagingService,
            FileTransferConfiguration configuration,
            Path tempDirectory
    ) {
        this(
                topologyService,
                messagingService,
                tempDirectory,
                new FileSender(
                        nodeName,
                        configuration.value().senderThreadPoolSize(),
                        configuration.value().chunkSize(),
                        new RateLimiter(configuration.value().maxConcurrentRequests()),
                        (recipientConsistentId, message) -> messagingService.send(recipientConsistentId, FILE_TRANSFERRING_CHANNEL,
                                message)
                ),
                new FileReceiver(nodeName, configuration.value().receiverThreadPoolSize())
        );
    }

    @TestOnly
    FileTransferServiceImpl(
            TopologyService topologyService,
            MessagingService messagingService,
            Path tempDirectory,
            FileSender fileSender,
            FileReceiver fileReceiver
    ) {
        this.topologyService = topologyService;
        this.messagingService = messagingService;
        this.tempDirectory = tempDirectory;
        this.fileSender = fileSender;
        this.fileReceiver = fileReceiver;
    }

    @Override
    public void start() {
        topologyService.addEventHandler(new TopologyEventHandler() {
            @Override
            public void onDisappeared(ClusterNode member) {
                fileReceiver.cancelTransfersFromSender(member.id());
            }
        });

        messagingService.addMessageHandler(FileTransferMessageType.class,
                (message, senderConsistentId, correlationId) -> {
                    if (message instanceof FileDownloadRequest) {
                        processDownloadRequest((FileDownloadRequest) message, senderConsistentId, correlationId);
                    } else if (message instanceof FileUploadRequest) {
                        processUploadRequest((FileUploadRequest) message, senderConsistentId, correlationId);
                    } else if (message instanceof FileTransferErrorMessage) {
                        processFileTransferErrorMessage((FileTransferErrorMessage) message);
                    } else if (message instanceof FileTransferInfoMessage) {
                        processFileTransferInfo((FileTransferInfoMessage) message);
                    } else if (message instanceof FileHeaderMessage) {
                        processFileHeader((FileHeaderMessage) message);
                    } else if (message instanceof FileChunkMessage) {
                        processFileChunk((FileChunkMessage) message);
                    } else {
                        LOG.error("Unexpected message received: {}", message);
                    }
                });
    }

    private void processFileTransferErrorMessage(FileTransferErrorMessage message) {
        fileReceiver.receiveFileTransferErrorMessage(message);
    }

    private void processUploadRequest(FileUploadRequest message, String senderConsistentId, long correlationId) {
        UUID transferId = UUID.randomUUID();
        createTransferDirectory(transferId)
                .whenComplete((directory, throwable) -> {
                    FileTransferMessagesHandler handler = fileReceiver.registerTransfer(
                            senderConsistentId,
                            transferId,
                            directory
                    );
                    handler.result().thenCompose(files -> handleUploadedFiles(message.metadata(), files))
                            .whenComplete((v, e) -> {
                                if (e != null) {
                                    LOG.error("Failed to handle uploaded files. Transfer ID: {}", transferId, e);
                                }
                                deleteDirectoryIfExists(directory);
                            });
                })
                .thenCompose(ignored -> messagingService.respond(
                        senderConsistentId,
                        FILE_TRANSFERRING_CHANNEL,
                        factory.fileUploadResponse()
                                .transferId(transferId)
                                .build(),
                        correlationId
                ));
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
                        FileDownloadResponse response = factory.fileDownloadResponse()
                                .error(unwrapFileTransferException(e).getMessage())
                                .build();
                        messagingService.respond(senderConsistentId, FILE_TRANSFERRING_CHANNEL, response, correlationId);
                    } else if (files.isEmpty()) {
                        LOG.warn("No files to download. Metadata: {}", message.metadata());
                        FileDownloadResponse response = factory.fileDownloadResponse()
                                .error("No files to download")
                                .build();
                        messagingService.respond(senderConsistentId, FILE_TRANSFERRING_CHANNEL, response, correlationId);
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
                        LOG.error("Failed to send files to node: {}, transfer id: {}. Exception: {}",
                                recipientConsistentId,
                                transferId,
                                e
                        );
                        FileTransferErrorMessage message = factory.fileTransferErrorMessage()
                                .transferId(transferId)
                                .error(unwrapFileTransferException(e).getMessage())
                                .build();
                        messagingService.send(recipientConsistentId, FILE_TRANSFERRING_CHANNEL, message);
                        return failedFuture(new FileTransferException("Failed to send files to node: " + recipientConsistentId, e));
                    } else {
                        return completedFuture(null);
                    }
                }).thenCompose(it -> it);
    }

    private void processFileTransferInfo(FileTransferInfoMessage info) {
        fileReceiver.receiveFileTransferInfo(info);
    }

    private void processFileHeader(FileHeaderMessage header) {
        fileReceiver.receiveFileHeader(header);
    }

    private void processFileChunk(FileChunkMessage fileChunk) {
        fileReceiver.receiveFileChunk(fileChunk);
    }

    @Override
    public void stop() {
        fileSender.stop();
        fileReceiver.stop();
    }

    @Override
    public <M extends Metadata> void addFileProvider(
            Class<M> metadata,
            FileProvider<M> provider
    ) {
        metadataToProvider.compute(
                getMessageType(metadata),
                (k, v) -> {
                    if (v != null) {
                        throw new IllegalArgumentException("File provider for metadata " + metadata.getName() + " already exists");
                    }
                    return (FileProvider<Metadata>) provider;
                }
        );
    }

    @Override
    public <M extends Metadata> void addFileHandler(
            Class<M> metadata,
            FileHandler<M> handler
    ) {
        metadataToHandler.compute(
                getMessageType(metadata),
                (k, v) -> {
                    if (v != null) {
                        throw new IllegalArgumentException("File handler for metadata " + metadata.getName() + " already exists");
                    }
                    return (FileHandler<Metadata>) handler;
                }
        );
    }

    private short getMessageType(Class<?> metadata) {
        Transferable annotation = metadata.getAnnotation(Transferable.class);
        if (annotation == null) {
            throw new IllegalArgumentException("Class " + metadata.getName() + " is not annotated with @Transferable");
        }
        return annotation.value();
    }

    @Override
    public CompletableFuture<List<File>> download(String nodeConsistentId, Metadata metadata) {
        UUID transferId = UUID.randomUUID();
        FileDownloadRequest message = factory.fileDownloadRequest()
                .transferId(transferId)
                .metadata(metadata)
                .build();

        return createTransferDirectory(transferId)
                .thenApply(directory -> fileReceiver.registerTransfer(nodeConsistentId, transferId, directory))
                .thenCompose(
                        handler -> messagingService.invoke(nodeConsistentId, FILE_TRANSFERRING_CHANNEL, message, RESPONSE_TIMEOUT)
                                .thenApply(FileDownloadResponse.class::cast)
                                .thenCompose(response -> {
                                    if (response.error() != null) {
                                        return failedFuture(new FileTransferException(response.error()));
                                    }
                                    return handler.result();
                                }));
    }

    @Override
    public CompletableFuture<Void> upload(String nodeConsistentId, Metadata metadata) {
        return metadataToProvider.get(metadata.messageType()).files(metadata)
                .thenCompose(files -> {
                    FileUploadRequest message = factory.fileUploadRequest()
                            .metadata(metadata)
                            .build();

                    if (files.isEmpty()) {
                        return failedFuture(new FileTransferException("No files to upload"));
                    } else {
                        return messagingService.invoke(nodeConsistentId, FILE_TRANSFERRING_CHANNEL, message, RESPONSE_TIMEOUT)
                                .thenApply(FileUploadResponse.class::cast)
                                .thenCompose(response -> sendFiles(nodeConsistentId, response.transferId(), files));
                    }
                });
    }

    private CompletableFuture<Path> createTransferDirectory(UUID transferId) {
        try {
            return completedFuture(Files.createDirectories(tempDirectory.resolve(transferId.toString())));
        } catch (IOException e) {
            return failedFuture(e);
        }
    }

    private static void deleteDirectoryIfExists(Path directory) {
        try {
            FilesUtils.deleteDirectoryIfExists(directory);
        } catch (IOException e) {
            LOG.error("Failed to delete directory: {}. Exception: {}", directory, e);
        }
    }

    private static Throwable unwrapFileTransferException(Throwable throwable) {
        Throwable unwrapped = ExceptionUtils.unwrapCause(throwable);
        if (unwrapped instanceof FileTransferException) {
            return unwrapped.getCause() == null ? unwrapped : unwrapped.getCause();
        } else {
            return unwrapped;
        }
    }
}
