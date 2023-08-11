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
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.ignite.internal.network.file.Channel.FILE_TRANSFER_CHANNEL;
import static org.apache.ignite.internal.network.file.messages.FileHeader.fromPaths;
import static org.apache.ignite.internal.network.file.messages.FileTransferError.fromThrowable;
import static org.apache.ignite.internal.network.file.messages.FileTransferError.toException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.configuration.FileTransferConfiguration;
import org.apache.ignite.internal.network.file.exception.FileTransferException;
import org.apache.ignite.internal.network.file.messages.FileChunkAckMessage;
import org.apache.ignite.internal.network.file.messages.FileChunkMessage;
import org.apache.ignite.internal.network.file.messages.FileDownloadRequest;
import org.apache.ignite.internal.network.file.messages.FileDownloadResponse;
import org.apache.ignite.internal.network.file.messages.FileTransferCancelMessage;
import org.apache.ignite.internal.network.file.messages.FileTransferFactory;
import org.apache.ignite.internal.network.file.messages.FileTransferMessageType;
import org.apache.ignite.internal.network.file.messages.FileUploadRequest;
import org.apache.ignite.internal.network.file.messages.FileUploadResponse;
import org.apache.ignite.internal.network.file.messages.Identifier;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
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

    /**
     * Response timeout.
     */
    private final long responseTimeout;

    /**
     * Topology service.
     */
    private final TopologyService topologyService;

    /**
     * Cluster service.
     */
    private final MessagingService messagingService;

    /**
     * Transfer directory. All files will be saved here before being moved to their final location.
     */
    private final Path transferDirectory;

    /**
     * File sender.
     */
    private final FileSender fileSender;

    /**
     * File receiver.
     */
    private final FileReceiver fileReceiver;

    /**
     * Executor service.
     */
    private final ExecutorService executorService;

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
     * Factory method. Creates a new instance of {@link FileTransferServiceImpl}.
     *
     * @param nodeName Node name.
     * @param topologyService Topology service.
     * @param messagingService Messaging service.
     * @param configuration File transfer configuration.
     * @param transferDirectory Transfer directory. All files will be saved here before being moved to their final location.
     */
    FileTransferServiceImpl(
            String nodeName,
            TopologyService topologyService,
            MessagingService messagingService,
            FileTransferConfiguration configuration,
            Path transferDirectory
    ) {
        this(
                topologyService,
                messagingService,
                configuration,
                transferDirectory,
                new ThreadPoolExecutor(
                        0,
                        configuration.value().threadPoolSize(),
                        0L, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        NamedThreadFactory.create(nodeName, "file-transfer", LOG)
                )
        );
    }

    private FileTransferServiceImpl(
            TopologyService topologyService,
            MessagingService messagingService,
            FileTransferConfiguration configuration,
            Path transferDirectory,
            ExecutorService executorService
    ) {
        this(
                configuration.value().responseTimeout(),
                topologyService,
                messagingService,
                transferDirectory,
                new FileSender(
                        configuration.value().chunkSize(),
                        new Semaphore(configuration.value().maxConcurrentRequests()),
                        configuration.value().responseTimeout(),
                        messagingService,
                        executorService
                ),
                new FileReceiver(),
                executorService
        );
    }

    /**
     * Constructor.
     *
     * @param responseTimeout Response timeout.
     * @param topologyService Topology service.
     * @param messagingService Messaging service.
     * @param transferDirectory Transfer directory. All files will be saved here before being moved to their final location.
     * @param fileSender File sender.
     * @param fileReceiver File receiver.
     * @param executorService Executor service.
     */
    FileTransferServiceImpl(
            long responseTimeout,
            TopologyService topologyService,
            MessagingService messagingService,
            Path transferDirectory,
            FileSender fileSender,
            FileReceiver fileReceiver,
            ExecutorService executorService
    ) {
        this.responseTimeout = responseTimeout;
        this.topologyService = topologyService;
        this.messagingService = messagingService;
        this.transferDirectory = transferDirectory;
        this.fileSender = fileSender;
        this.fileReceiver = fileReceiver;
        this.executorService = executorService;
    }

    @Override
    public void start() {
        topologyService.addEventHandler(new TopologyEventHandler() {
            @Override
            public void onDisappeared(ClusterNode member) {
                fileReceiver.cancelTransfersFromSender(member.name());
            }
        });

        messagingService.addMessageHandler(FileTransferMessageType.class,
                (message, senderConsistentId, correlationId) -> {
                    if (message instanceof FileDownloadRequest) {
                        processDownloadRequest((FileDownloadRequest) message, senderConsistentId, correlationId);
                    } else if (message instanceof FileUploadRequest) {
                        processUploadRequest((FileUploadRequest) message, senderConsistentId, correlationId);
                    } else if (message instanceof FileChunkMessage) {
                        processFileChunkMessage((FileChunkMessage) message, senderConsistentId, correlationId);
                    } else if (message instanceof FileTransferCancelMessage) {
                        processFileTransferCancelMessage((FileTransferCancelMessage) message);
                    } else {
                        LOG.error("Unexpected message received: {}", message);
                    }
                });
    }

    @Override
    public void stop() {
        IgniteUtils.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS);
    }

    private void processUploadRequest(FileUploadRequest message, String senderConsistentId, long correlationId) {
        UUID transferId = UUID.randomUUID();

        Identifier identifier = message.identifier();

        CompletableFuture<Path> directoryFuture = supplyAsync(() -> createTransferDirectory(transferId), executorService);

        CompletableFuture<List<Path>> uploadedFiles = directoryFuture.thenCompose(directory -> fileReceiver.registerTransfer(
                senderConsistentId,
                transferId,
                directory
        )).whenComplete((v, e) -> {
            if (e != null) {
                LOG.error("Failed to register file transfer. Transfer ID: {}. Metadata: {}", e, transferId, identifier);
            }
        });

        runAsync(() -> fileReceiver.receiveFileHeaders(transferId, message.headers()), executorService)
                .handle((v, e) -> {
                    FileUploadResponse response = messageFactory.fileUploadResponse()
                            .transferId(transferId)
                            .error(e != null
                                    ? fromThrowable(messageFactory, new FileTransferException("Failed to receive file headers", e))
                                    : null)
                            .build();

                    return messagingService.respond(senderConsistentId, FILE_TRANSFER_CHANNEL, response, correlationId);
                })
                .thenCompose(Function.identity())
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Failed to send file upload response. Transfer ID: {}. Metadata: {}", e, transferId, identifier);
                        fileReceiver.cancelTransfer(transferId, e);
                    }
                })
                .thenCompose(ignored -> uploadedFiles)
                .thenCompose(files -> getFileConsumer(identifier).consume(identifier, files))
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error(
                                "Failed to handle file upload. Transfer ID: {}. Metadata: {}",
                                e,
                                transferId,
                                identifier
                        );
                    }

                    directoryFuture.thenAccept(IgniteUtils::deleteIfExists);
                });
    }

    private void processDownloadRequest(FileDownloadRequest message, String senderConsistentId, Long correlationId) {
        supplyAsync(() -> getFileProvider(message.identifier()), executorService)
                .thenCompose(provider -> provider.files(message.identifier()))
                .whenComplete((files, e) -> {
                    if (e != null) {
                        LOG.error("Failed to get files for download. Metadata: {}", e, message.identifier());

                        FileDownloadResponse response = messageFactory.fileDownloadResponse()
                                .error(fromThrowable(messageFactory, e))
                                .build();

                        messagingService.respond(senderConsistentId, FILE_TRANSFER_CHANNEL, response, correlationId);
                    } else if (files.isEmpty()) {
                        LOG.warn("No files to download. Metadata: {}", message.identifier());

                        FileDownloadResponse response = messageFactory.fileDownloadResponse()
                                .error(fromThrowable(messageFactory, new FileTransferException("No files to download")))
                                .build();

                        messagingService.respond(senderConsistentId, FILE_TRANSFER_CHANNEL, response, correlationId);
                    } else {
                        FileDownloadResponse response = messageFactory.fileDownloadResponse()
                                .headers(fromPaths(messageFactory, files))
                                .build();

                        messagingService.respond(senderConsistentId, FILE_TRANSFER_CHANNEL, response, correlationId)
                                .thenComposeAsync(v -> sendFiles(senderConsistentId, message.transferId(), files), executorService)
                                .whenComplete((v, e2) -> {
                                    if (e2 != null) {
                                        LOG.error(
                                                "Failed to send files. Transfer ID: {}. Metadata: {}",
                                                e2,
                                                message.transferId(),
                                                message.identifier()
                                        );
                                    }
                                });
                    }
                });
    }

    private void processFileChunkMessage(FileChunkMessage message, String senderConsistentId, long correlationId) {
        runAsync(() -> fileReceiver.receiveFileChunk(message), executorService)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Failed to receive file chunk. Transfer ID: {}", e, message.transferId());
                    }

                    FileChunkAckMessage ack = messageFactory.fileChunkAckMessage()
                            .error(e != null ? fromThrowable(messageFactory, e) : null)
                            .build();

                    messagingService.respond(senderConsistentId, FILE_TRANSFER_CHANNEL, ack, correlationId);
                });
    }

    private void processFileTransferCancelMessage(FileTransferCancelMessage message) {
        LOG.error("Received file transfer cancel message. Transfer will be cancelled. Transfer ID: {}. Error: {}",
                message.transferId(),
                message.error()
        );
        runAsync(
                () -> fileReceiver.cancelTransfer(message.transferId(), toException(message.error())),
                executorService
        );
    }


    private CompletableFuture<Void> sendFiles(String targetNodeConsistentId, UUID transferId, List<Path> paths) {
        return fileSender.send(targetNodeConsistentId, transferId, paths)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Failed to send files to node: {}, transfer id: {}",
                                e,
                                targetNodeConsistentId,
                                transferId
                        );

                        FileTransferCancelMessage message = messageFactory.fileTransferCancelMessage()
                                .transferId(transferId)
                                .error(fromThrowable(messageFactory, e))
                                .build();

                        messagingService.send(targetNodeConsistentId, FILE_TRANSFER_CHANNEL, message);
                    }
                });
    }

    @Override
    public <M extends Identifier> void addFileProvider(
            Class<M> identifier,
            FileProvider<M> provider
    ) {
        metadataToProvider.compute(
                getMessageType(identifier),
                (k, v) -> {
                    if (v != null) {
                        throw new IllegalArgumentException("File provider for metadata " + identifier.getName() + " already exists");
                    } else {
                        return (FileProvider<Identifier>) provider;
                    }
                }
        );
    }

    @Override
    public <M extends Identifier> void addFileConsumer(
            Class<M> identifier,
            FileConsumer<M> consumer
    ) {
        metadataToHandler.compute(
                getMessageType(identifier),
                (k, v) -> {
                    if (v != null) {
                        throw new IllegalArgumentException("File handler for metadata " + identifier.getName() + " already exists");
                    } else {
                        return (FileConsumer<Identifier>) consumer;
                    }
                }
        );
    }

    @Override
    public CompletableFuture<List<Path>> download(String sourceNodeConsistentId, Identifier identifier, Path targetDir) {
        UUID transferId = UUID.randomUUID();
        FileDownloadRequest downloadRequest = messageFactory.fileDownloadRequest()
                .transferId(transferId)
                .identifier(identifier)
                .build();

        try {
            Path directory = createTransferDirectory(transferId);

            CompletableFuture<List<Path>> downloadedFiles = fileReceiver.registerTransfer(
                            sourceNodeConsistentId,
                            transferId,
                            directory
                    )
                    .thenApply(ignored -> {
                        try {
                            IgniteUtils.deleteIfExists(targetDir);

                            Files.move(directory, targetDir, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

                            try (Stream<Path> stream = Files.list(targetDir)) {
                                return stream.collect(Collectors.toList());
                            }
                        } catch (IOException e) {
                            throw new FileTransferException("Failed to move downloaded files to target directory", e);
                        }
                    })
                    .whenComplete((v, e) -> {
                        if (e != null) {
                            LOG.error("Failed to download files. Identifier: {}", identifier, e);
                        }

                        IgniteUtils.deleteIfExists(directory);
                    });

            return messagingService.invoke(sourceNodeConsistentId, FILE_TRANSFER_CHANNEL, downloadRequest, responseTimeout)
                    .thenApply(FileDownloadResponse.class::cast)
                    .whenCompleteAsync((response, e) -> {
                        if (e != null) {
                            fileReceiver.cancelTransfer(transferId, e);
                        } else if (response.error() != null) {
                            fileReceiver.cancelTransfer(transferId, toException(response.error()));
                        } else {
                            fileReceiver.receiveFileHeaders(transferId, response.headers());
                        }
                    }, executorService)
                    .thenCompose(v -> downloadedFiles);
        } catch (Exception e) {
            return failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> upload(String targetNodeConsistentId, Identifier identifier) {
        return getFileProvider(identifier).files(identifier)
                .thenCompose(files -> {
                    FileUploadRequest message = messageFactory.fileUploadRequest()
                            .identifier(identifier)
                            .headers(fromPaths(messageFactory, files))
                            .build();

                    if (files.isEmpty()) {
                        return failedFuture(new FileTransferException("No files to upload"));
                    } else {
                        return messagingService.invoke(targetNodeConsistentId, FILE_TRANSFER_CHANNEL, message, responseTimeout)
                                .thenApply(FileUploadResponse.class::cast)
                                .thenComposeAsync(response -> {
                                    if (response.error() != null) {
                                        return failedFuture(
                                                new FileTransferException("Failed to upload files: " + response.error().message())
                                        );
                                    } else {
                                        return sendFiles(targetNodeConsistentId, response.transferId(), files);
                                    }
                                }, executorService);
                    }
                });
    }

    private static short getMessageType(Class<?> metadata) {
        Transferable annotation = metadata.getAnnotation(Transferable.class);
        if (annotation == null) {
            throw new IllegalArgumentException("Class " + metadata.getName() + " is not annotated with @Transferable");
        } else {
            return annotation.value();
        }
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
            return Files.createDirectories(transferDirectory.resolve(transferId.toString()));
        } catch (IOException e) {
            throw new FileTransferException("Failed to create transfer directory. Transfer id: " + transferId, e);
        }
    }
}
