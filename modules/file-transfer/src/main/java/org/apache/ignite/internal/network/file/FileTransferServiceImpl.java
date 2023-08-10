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
import static org.apache.ignite.internal.network.file.Channel.FILE_TRANSFER_CHANNEL;
import static org.apache.ignite.internal.network.file.messages.FileHeader.fromPaths;

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
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.ExceptionUtils;
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
    private final int reponseTimeout;

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
                configuration.value().responseTimeout(),
                topologyService,
                messagingService,
                tempDirectory,
                new FileSender(
                        configuration.value().chunkSize(),
                        new Semaphore(configuration.value().maxConcurrentRequests()),
                        messagingService
                ),
                new FileReceiver(),
                new ThreadPoolExecutor(
                        0,
                        configuration.value().threadPoolSize(),
                        0L, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        NamedThreadFactory.create(nodeName, "file-sender", LOG)
                )
        );
    }

    /**
     * Constructor.
     *
     * @param responseTimeout Response timeout.
     * @param topologyService Topology service.
     * @param messagingService Messaging service.
     * @param tempDirectory Temporary directory.
     * @param fileSender File sender.
     * @param fileReceiver File receiver.
     */
    FileTransferServiceImpl(
            int responseTimeout,
            TopologyService topologyService,
            MessagingService messagingService,
            Path tempDirectory,
            FileSender fileSender,
            FileReceiver fileReceiver,
            ExecutorService executorService
    ) {
        this.reponseTimeout = responseTimeout;
        this.topologyService = topologyService;
        this.messagingService = messagingService;
        this.tempDirectory = tempDirectory;
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
                    } else if (message instanceof FileTransferErrorMessage) {
                        processFileTransferErrorMessage((FileTransferErrorMessage) message);
                    } else if (message instanceof FileChunkMessage) {
                        processFileChunkMessage((FileChunkMessage) message);
                    } else {
                        LOG.error("Unexpected message received: {}", message);
                    }
                });
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

        messagingService.respond(senderConsistentId, FILE_TRANSFER_CHANNEL, response, correlationId)
                .thenComposeAsync(ignored -> {
                    return uploadedFiles.thenCompose(files -> getFileConsumer(identifier)
                                    .consume(identifier, files))
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
                }, executorService);
    }

    private void processDownloadRequest(FileDownloadRequest message, String senderConsistentId, Long correlationId) {
        getFileProvider(message.identifier()).files(message.identifier())
                .whenComplete((files, e) -> {
                    if (e != null) {
                        LOG.error("Failed to get files for download. Metadata: {}", message.identifier(), e);

                        FileDownloadResponse response = messageFactory.fileDownloadResponse()
                                .error(buildError(e))
                                .build();

                        messagingService.respond(senderConsistentId, FILE_TRANSFER_CHANNEL, response, correlationId);
                    } else if (files.isEmpty()) {
                        LOG.warn("No files to download. Metadata: {}", message.identifier());

                        FileDownloadResponse response = messageFactory.fileDownloadResponse()
                                .error(buildError(new FileTransferException("No files to download")))
                                .build();

                        messagingService.respond(senderConsistentId, FILE_TRANSFER_CHANNEL, response, correlationId);
                    } else {
                        FileDownloadResponse response = messageFactory.fileDownloadResponse()
                                .headers(fromPaths(messageFactory, files))
                                .build();

                        messagingService.respond(senderConsistentId, FILE_TRANSFER_CHANNEL, response, correlationId)
                                .thenCompose(v -> sendFiles(senderConsistentId, message.transferId(), files));
                    }
                });
    }

    private void processFileChunkMessage(FileChunkMessage message) {
        runAsync(() -> fileReceiver.receiveFileChunk(message));
    }

    private void processFileTransferErrorMessage(FileTransferErrorMessage message) {
        fileReceiver.receiveFileTransferError(message.transferId(), message.error());
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

                        FileTransferErrorMessage message = messageFactory.fileTransferErrorMessage()
                                .transferId(transferId)
                                .error(buildError(e))
                                .build();

                        messagingService.send(targetNodeConsistentId, FILE_TRANSFER_CHANNEL, message);
                    }
                });
    }

    @Override
    public void stop() {
        IgniteUtils.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS);
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

            return messagingService.invoke(sourceNodeConsistentId, FILE_TRANSFER_CHANNEL, downloadRequest, reponseTimeout)
                    .thenApply(FileDownloadResponse.class::cast)
                    .thenComposeAsync(response -> {
                        if (response.error() != null) {
                            fileReceiver.receiveFileTransferError(transferId, response.error());
                        } else {
                            fileReceiver.receiveFileHeaders(transferId, response.headers());
                        }

                        return downloadedFiles;
                    }, executorService);
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
                        return messagingService.invoke(targetNodeConsistentId, FILE_TRANSFER_CHANNEL, message, reponseTimeout)
                                .thenApply(FileUploadResponse.class::cast)
                                .thenCompose(response -> sendFiles(targetNodeConsistentId, response.transferId(), files));
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
            return Files.createDirectories(tempDirectory.resolve(transferId.toString()));
        } catch (IOException e) {
            throw new FileTransferException("Failed to create transfer directory. Transfer id: " + transferId, e);
        }
    }

    private FileTransferError buildError(Throwable throwable) {
        return messageFactory.fileTransferError()
                .message(ExceptionUtils.unwrapCause(throwable).getMessage())
                .build();
    }
}
