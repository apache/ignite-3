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
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.ignite.internal.network.file.MessagesUtils.getHeaders;

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
import org.apache.ignite.internal.network.file.exception.FileTransferException;
import org.apache.ignite.internal.network.file.messages.FileChunkMessage;
import org.apache.ignite.internal.network.file.messages.FileDownloadRequest;
import org.apache.ignite.internal.network.file.messages.FileDownloadResponse;
import org.apache.ignite.internal.network.file.messages.FileTransferError;
import org.apache.ignite.internal.network.file.messages.FileTransferErrorMessage;
import org.apache.ignite.internal.network.file.messages.FileTransferFactory;
import org.apache.ignite.internal.network.file.messages.FileTransferMessageType;
import org.apache.ignite.internal.network.file.messages.FileUploadRequest;
import org.apache.ignite.internal.network.file.messages.FileUploadResponse;
import org.apache.ignite.internal.network.file.messages.Identifier;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ChannelType;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.network.annotations.Transferable;

/**
 * Implementation of {@link FileTransferService}.
 */
public class FileTransferServiceImpl implements FileTransferService {
    private static final IgniteLogger LOG = Loggers.forClass(FileTransferServiceImpl.class);

    private static final ChannelType FILE_TRANSFERRING_CHANNEL = ChannelType.register((short) 1, "FileTransferring");

    private static final long RESPONSE_TIMEOUT = 10_000;

    /**
     * Topology service.
     */
    private final TopologyService topologyService;

    /**
     * Cluster service.
     */
    private final MessagingService messagingService;

    /**
     * Temporary directory for saving files.
     */
    private final Path tempDirectory;

    /**
     * File sender.
     */
    private final FileSender fileSender;

    /**
     * File receiver.
     */
    private final FileReceiver fileReceiver;

    /**
     * Map of file providers.
     */
    private final Map<Short, FileProvider<Identifier>> metadataToProvider = new ConcurrentHashMap<>();

    /**
     * Map of file handlers.
     */
    private final Map<Short, FileConsumer<Identifier>> metadataToHandler = new ConcurrentHashMap<>();

    /**
     * File transfer factory.
     */
    private final FileTransferFactory messageFactory = new FileTransferFactory();

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param topologyService Topology service.
     * @param messagingService Messaging service.
     * @param configuration File transfer configuration.
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
                        configuration.value().chunkSize(),
                        configuration.value().senderThreadPoolSize(),
                        new RateLimiterImpl(configuration.value().maxConcurrentRequests()),
                        (recipientConsistentId, message) -> messagingService.send(recipientConsistentId, FILE_TRANSFERRING_CHANNEL,
                                message)
                ),
                new FileReceiver(nodeName, configuration.value().receiverThreadPoolSize())
        );
    }

    /**
     * Constructor.
     *
     * @param topologyService Topology service.
     * @param messagingService Messaging service.
     * @param tempDirectory Temporary directory.
     * @param fileSender File sender.
     * @param fileReceiver File receiver.
     */
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

        Path directory = createTransferDirectory(transferId);

        CompletableFuture<List<Path>> uploadedFiles = fileReceiver.registerTransfer(
                senderConsistentId,
                transferId,
                directory
        );

        fileReceiver.receiveFileHeaders(transferId, message.headers());

        FileUploadResponse response = messageFactory.fileUploadResponse()
                .transferId(transferId)
                .build();
        Identifier identifier = message.identifier();

        messagingService.respond(senderConsistentId, FILE_TRANSFERRING_CHANNEL, response, correlationId)
                .thenCompose(ignored -> {
                    return uploadedFiles.thenCompose(files -> getFileConsumer(identifier).consume(identifier, files));
                })
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error(
                                "Failed to handle file upload. Transfer ID: {}. Metadata: {}",
                                e,
                                transferId,
                                identifier
                        );
                    }
                    IgniteUtils.deleteIfExists(directory);
                });
    }

    private void processDownloadRequest(FileDownloadRequest message, String senderConsistentId, Long correlationId) {
        getFileProvider(message.identifier()).files(message.identifier())
                .whenComplete((files, e) -> {
                    if (e != null) {
                        LOG.error("Failed to get files for download. Metadata: {}", message.identifier(), e);
                        FileDownloadResponse response = messageFactory.fileDownloadResponse()
                                .error(buildError(e))
                                .build();
                        messagingService.respond(senderConsistentId, FILE_TRANSFERRING_CHANNEL, response, correlationId);
                    } else if (files.isEmpty()) {
                        LOG.warn("No files to download. Metadata: {}", message.identifier());
                        FileDownloadResponse response = messageFactory.fileDownloadResponse()
                                .error(buildError(new FileTransferException("No files to download")))
                                .build();
                        messagingService.respond(senderConsistentId, FILE_TRANSFERRING_CHANNEL, response, correlationId);
                    } else {
                        FileDownloadResponse response = messageFactory.fileDownloadResponse()
                                .headers(getHeaders(messageFactory, files))
                                .build();
                        messagingService.respond(senderConsistentId, FILE_TRANSFERRING_CHANNEL, response, correlationId)
                                .thenCompose(v -> sendFiles(senderConsistentId, message.transferId(), files));
                    }
                });
    }

    private CompletableFuture<Void> sendFiles(String recipientConsistentId, UUID transferId, List<Path> paths) {
        return fileSender.send(recipientConsistentId, transferId, paths)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Failed to send files to node: {}, transfer id: {}",
                                e,
                                recipientConsistentId,
                                transferId
                        );
                        FileTransferErrorMessage message = messageFactory.fileTransferErrorMessage()
                                .transferId(transferId)
                                .error(buildError(e))
                                .build();
                        messagingService.send(recipientConsistentId, FILE_TRANSFERRING_CHANNEL, message);
                    }
                });
    }

    private void processFileChunk(FileChunkMessage fileChunk) {
        fileReceiver.receiveFileChunk(fileChunk);
    }

    @Override
    public void stop() throws Exception {
        IgniteUtils.closeAllManually(fileSender, fileReceiver);
    }

    @Override
    public <M extends Identifier> void addFileProvider(
            Class<M> metadata,
            FileProvider<M> provider
    ) {
        metadataToProvider.compute(
                getMessageType(metadata),
                (k, v) -> {
                    if (v != null) {
                        throw new IllegalArgumentException("File provider for metadata " + metadata.getName() + " already exists");
                    }
                    return (FileProvider<Identifier>) provider;
                }
        );
    }

    @Override
    public <M extends Identifier> void addFileConsumer(
            Class<M> metadata,
            FileConsumer<M> handler
    ) {
        metadataToHandler.compute(
                getMessageType(metadata),
                (k, v) -> {
                    if (v != null) {
                        throw new IllegalArgumentException("File handler for metadata " + metadata.getName() + " already exists");
                    }
                    return (FileConsumer<Identifier>) handler;
                }
        );
    }

    @Override
    public CompletableFuture<List<Path>> download(String sourceNodeConsistentId, Identifier identifier) {
        UUID transferId = UUID.randomUUID();
        FileDownloadRequest downloadRequest = messageFactory.fileDownloadRequest()
                .transferId(transferId)
                .identifier(identifier)
                .build();

        return supplyAsync(() -> createTransferDirectory(transferId))
                .thenApply(directory -> {
                    return fileReceiver.registerTransfer(
                            sourceNodeConsistentId,
                            transferId,
                            directory
                    );
                }).thenCompose(downloadedFiles -> {
                    return messagingService.invoke(sourceNodeConsistentId, FILE_TRANSFERRING_CHANNEL, downloadRequest, RESPONSE_TIMEOUT)
                            .thenApply(FileDownloadResponse.class::cast)
                            .thenCompose(response -> {
                                if (response.error() != null) {
                                    return failedFuture(new FileTransferException(response.error().message()));
                                } else {
                                    return fileReceiver.receiveFileHeaders(transferId, response.headers())
                                            .thenCompose(ignored -> downloadedFiles);
                                }
                            });
                });
    }

    @Override
    public CompletableFuture<Void> upload(String targetNodeConsistentId, Identifier identifier) {
        return getFileProvider(identifier).files(identifier)
                .thenCompose(files -> {
                    FileUploadRequest message = messageFactory.fileUploadRequest()
                            .identifier(identifier)
                            .headers(getHeaders(messageFactory, files))
                            .build();

                    if (files.isEmpty()) {
                        return failedFuture(new FileTransferException("No files to upload"));
                    } else {
                        return messagingService.invoke(targetNodeConsistentId, FILE_TRANSFERRING_CHANNEL, message, RESPONSE_TIMEOUT)
                                .thenApply(FileUploadResponse.class::cast)
                                .thenCompose(response -> sendFiles(targetNodeConsistentId, response.transferId(), files));
                    }
                });
    }

    private static short getMessageType(Class<?> metadata) {
        Transferable annotation = metadata.getAnnotation(Transferable.class);
        if (annotation == null) {
            throw new IllegalArgumentException("Class " + metadata.getName() + " is not annotated with @Transferable");
        }
        return annotation.value();
    }

    private <M extends Identifier> FileProvider<M> getFileProvider(M metadata) {
        FileProvider<Identifier> provider = metadataToProvider.get(metadata.messageType());
        if (provider == null) {
            throw new IllegalArgumentException("File provider for metadata " + metadata.getClass().getName() + " not found");
        } else {
            return (FileProvider<M>) provider;
        }
    }

    private <M extends Identifier> FileConsumer<M> getFileConsumer(M metadata) {
        FileConsumer<Identifier> consumer = metadataToHandler.get(metadata.messageType());
        if (consumer == null) {
            throw new IllegalArgumentException("File consumer for metadata " + metadata.getClass().getName() + " not found");
        } else {
            return (FileConsumer<M>) consumer;
        }
    }

    private Path createTransferDirectory(UUID transferId) {
        try {
            return Files.createDirectories(tempDirectory.resolve(transferId.toString()));
        } catch (IOException e) {
            throw new FileTransferException("Failed to create transfer directory. Transfer id: " + transferId, e);
        }
    }

    private FileTransferError buildError(Throwable throwable) {
        return messageFactory.fileTransferError()
                .message(unwrapFileTransferException(throwable).getMessage())
                .build();
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
