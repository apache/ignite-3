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
import org.apache.ignite.internal.network.file.messages.FileChunkMessage;
import org.apache.ignite.internal.network.file.messages.FileChunkResponse;
import org.apache.ignite.internal.network.file.messages.FileDownloadRequest;
import org.apache.ignite.internal.network.file.messages.FileDownloadResponse;
import org.apache.ignite.internal.network.file.messages.FileTransferErrorMessage;
import org.apache.ignite.internal.network.file.messages.FileTransferFactory;
import org.apache.ignite.internal.network.file.messages.FileTransferInitMessage;
import org.apache.ignite.internal.network.file.messages.FileTransferInitResponse;
import org.apache.ignite.internal.network.file.messages.FileTransferMessageType;
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
    private final Map<Short, FileProvider<Identifier>> identifierToProvider = new ConcurrentHashMap<>();

    /**
     * Map of file consumers.
     */
    private final Map<Short, FileConsumer<Identifier>> identifierToConsumer = new ConcurrentHashMap<>();

    /**
     * Map of download requests consumers.
     */
    private final Map<UUID, DownloadRequestConsumer> transferIdToDownloadConsumer = new ConcurrentHashMap<>();

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

    /**
     * Constructor.
     *
     * @param topologyService Topology service.
     * @param messagingService Messaging service.
     * @param configuration File transfer configuration.
     * @param transferDirectory Transfer directory. All files will be saved here before being moved to their final location.
     * @param executorService Executor service.
     */
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
                    } else if (message instanceof FileTransferInitMessage) {
                        processFileTransferInitMessage((FileTransferInitMessage) message, senderConsistentId, correlationId);
                    } else if (message instanceof FileChunkMessage) {
                        processFileChunkMessage((FileChunkMessage) message, senderConsistentId, correlationId);
                    } else if (message instanceof FileTransferErrorMessage) {
                        processFileTransferErrorMessage((FileTransferErrorMessage) message);
                    } else {
                        LOG.error("Unexpected message received: {}", message);
                    }
                });
    }

    @Override
    public void stop() {
        IgniteUtils.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS);
    }

    private void processFileTransferInitMessage(FileTransferInitMessage message, String senderConsistentId, long correlationId) {
        UUID transferId = message.transferId();

        Identifier identifier = message.identifier();

        CompletableFuture<Path> directoryFuture = supplyAsync(() -> createTransferDirectory(transferId), executorService);

        CompletableFuture<List<Path>> uploadedFiles = directoryFuture.thenCompose(directory -> fileReceiver.registerTransfer(
                senderConsistentId,
                transferId,
                message.headers(),
                directory
        )).whenComplete((v, e) -> {
            if (e != null) {
                LOG.error("Failed to register file transfer [transferId={}, identifier={}]", e, transferId, identifier);
            }
        });

        // Check that directory was created successfully and send response to the sender.
        directoryFuture.handle((directory, e) -> {
            FileTransferInitResponse response = messageFactory.fileTransferInitResponse()
                    .error(e != null ? fromThrowable(messageFactory, e) : null)
                    .build();
            return messagingService.respond(senderConsistentId, FILE_TRANSFER_CHANNEL, response, correlationId);
        })
                // Wait for the response to be sent.
                .thenCompose(Function.identity())
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Failed to send file transfer response [transferId={}, identifier={}]", e, transferId, identifier);
                        fileReceiver.cancelTransfer(transferId, e);
                    }
                })
                // Wait for the files to be uploaded.
                .thenComposeAsync(ignored -> uploadedFiles, executorService)
                // Provide the files to the consumer.
                .<CompletableFuture<Void>>handleAsync((files, e) -> {
                    // Remove the consumer from the map if there was an error and it was a download request.
                    if (e != null && transferIdToDownloadConsumer.containsKey(transferId)) {
                        transferIdToDownloadConsumer.get(transferId).onError(e);
                        return failedFuture(e);
                    } else {
                        // Check if there is a consumer for this transfer ID (download request) or use the default one.
                        FileConsumer<Identifier> consumer = transferIdToDownloadConsumer.containsKey(transferId)
                                ? transferIdToDownloadConsumer.get(transferId)
                                : getFileConsumer(identifier);
                        return consumer.consume(identifier, files);
                    }
                }, executorService)
                // Wait for the consumer to finish.
                .thenComposeAsync(Function.identity(), executorService)
                .whenCompleteAsync((v, e) -> {
                    if (e != null) {
                        LOG.error("Failed to process file transfer [transferId={}, identifier={}]",
                                e,
                                transferId,
                                identifier
                        );
                    }

                    directoryFuture.thenAccept(IgniteUtils::deleteIfExists);
                }, executorService);
    }

    private void processDownloadRequest(FileDownloadRequest message, String senderConsistentId, Long correlationId) {
        supplyAsync(() -> getFileProvider(message.identifier()), executorService)
                .thenCompose(provider -> provider.files(message.identifier()))
                .whenComplete((files, e) -> {
                    if (e != null) {
                        LOG.error("Failed to get files for download [transferId={}, identifier={}]",
                                e,
                                message.transferId(),
                                message.identifier()
                        );

                        FileDownloadResponse response = messageFactory.fileDownloadResponse()
                                .error(fromThrowable(messageFactory, e))
                                .build();

                        messagingService.respond(senderConsistentId, FILE_TRANSFER_CHANNEL, response, correlationId);
                    } else if (files.isEmpty()) {
                        LOG.warn("No files to download [transferId={}, identifier={}]", message.transferId(), message.identifier());

                        FileDownloadResponse response = messageFactory.fileDownloadResponse()
                                .error(fromThrowable(messageFactory, new FileTransferException("No files to download")))
                                .build();

                        messagingService.respond(senderConsistentId, FILE_TRANSFER_CHANNEL, response, correlationId);
                    } else {
                        FileDownloadResponse response = messageFactory.fileDownloadResponse()
                                .build();

                        messagingService.respond(senderConsistentId, FILE_TRANSFER_CHANNEL, response, correlationId)
                                .thenComposeAsync(v -> {
                                    return transferFilesToNode(senderConsistentId, message.transferId(), message.identifier(), files);
                                }, executorService);
                    }
                });
    }

    private void processFileChunkMessage(FileChunkMessage message, String senderConsistentId, long correlationId) {
        runAsync(() -> fileReceiver.receiveFileChunk(message), executorService)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Failed to process file chunk [transferId={}]", e, message.transferId());
                    }

                    FileChunkResponse ack = messageFactory.fileChunkResponse()
                            .error(e != null ? fromThrowable(messageFactory, e) : null)
                            .build();

                    messagingService.respond(senderConsistentId, FILE_TRANSFER_CHANNEL, ack, correlationId);
                });
    }

    private void processFileTransferErrorMessage(FileTransferErrorMessage message) {
        LOG.error("Received file transfer error message. Transfer will be cancelled [transferId={}, error={}",
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
                        LOG.error("Failed to send files to node [nodeConsistentId={}, transferId={}]",
                                e,
                                targetNodeConsistentId,
                                transferId
                        );

                        FileTransferErrorMessage message = messageFactory.fileTransferErrorMessage()
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
        identifierToProvider.compute(
                getMessageType(identifier),
                (k, v) -> {
                    if (v != null) {
                        throw new IllegalArgumentException("File provider for identifier " + identifier.getName() + " already exists");
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
        identifierToConsumer.compute(
                getMessageType(identifier),
                (k, v) -> {
                    if (v != null) {
                        throw new IllegalArgumentException("File handler for identifier " + identifier.getName() + " already exists");
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

        CompletableFuture<List<Path>> downloadedFiles = new CompletableFuture<List<Path>>()
                .whenComplete((v, e) -> transferIdToDownloadConsumer.remove(transferId));

        transferIdToDownloadConsumer.put(transferId, new DownloadRequestConsumer(downloadedFiles, targetDir));

        messagingService.invoke(sourceNodeConsistentId, FILE_TRANSFER_CHANNEL, downloadRequest, responseTimeout)
                .thenApply(FileDownloadResponse.class::cast)
                .whenComplete((response, e) -> {
                    if (e != null) {
                        downloadedFiles.completeExceptionally(e);
                    } else if (response.error() != null) {
                        downloadedFiles.completeExceptionally(toException(response.error()));
                    }
                });

        return downloadedFiles;
    }

    @Override
    public CompletableFuture<Void> upload(String targetNodeConsistentId, Identifier identifier) {
        return getFileProvider(identifier).files(identifier)
                .thenCompose(files -> transferFilesToNode(targetNodeConsistentId, UUID.randomUUID(), identifier, files));
    }

    private CompletableFuture<Void> transferFilesToNode(
            String targetNodeConsistentId,
            UUID transferId,
            Identifier identifier,
            List<Path> paths
    ) {
        if (paths.isEmpty()) {
            return failedFuture(new FileTransferException("No files to upload"));
        }

        FileTransferInitMessage message = messageFactory.fileTransferInitMessage()
                .transferId(transferId)
                .identifier(identifier)
                .headers(fromPaths(messageFactory, paths))
                .build();

        return messagingService.invoke(targetNodeConsistentId, FILE_TRANSFER_CHANNEL, message, responseTimeout)
                .thenApply(FileTransferInitResponse.class::cast)
                .thenComposeAsync(response -> {
                    if (response.error() != null) {
                        return failedFuture(
                                new FileTransferException("Failed to upload files: " + response.error().message())
                        );
                    } else {
                        return sendFiles(targetNodeConsistentId, transferId, paths);
                    }
                }, executorService)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Failed to transfer files to node [nodeConsistentId={}, transferId={}]",
                                e,
                                targetNodeConsistentId,
                                transferId
                        );
                    }
                });
    }

    private static short getMessageType(Class<?> identifier) {
        Transferable annotation = identifier.getAnnotation(Transferable.class);
        if (annotation == null) {
            throw new IllegalArgumentException("Class " + identifier.getName() + " is not annotated with @Transferable");
        } else {
            return annotation.value();
        }
    }

    private <M extends Identifier> FileProvider<M> getFileProvider(M identifier) {
        FileProvider<Identifier> provider = identifierToProvider.get(identifier.messageType());
        if (provider == null) {
            throw new IllegalArgumentException("File provider for identifier " + identifier.getClass().getName() + " not found");
        } else {
            return (FileProvider<M>) provider;
        }
    }

    private <M extends Identifier> FileConsumer<M> getFileConsumer(M identifier) {
        FileConsumer<Identifier> consumer = identifierToConsumer.get(identifier.messageType());
        if (consumer == null) {
            throw new IllegalArgumentException("File consumer for identifier " + identifier.getClass().getName() + " not found");
        } else {
            return (FileConsumer<M>) consumer;
        }
    }

    private Path createTransferDirectory(UUID transferId) {
        try {
            return Files.createDirectories(transferDirectory.resolve(transferId.toString()));
        } catch (IOException e) {
            throw new FileTransferException("Failed to create the transfer directory with transferId: " + transferId, e);
        }
    }

    /**
     * Consumer for file download request. It moves downloaded files to target directory and completes future with list of downloaded
     * files.
     */
    private static class DownloadRequestConsumer implements FileConsumer<Identifier> {
        private final CompletableFuture<List<Path>> downloadedFiles;
        private final Path targetDir;

        /**
         * Constructor.
         *
         * @param downloadedFiles Future to complete with downloaded files.
         * @param targetDir       Target directory to move downloaded files.
         */
        private DownloadRequestConsumer(CompletableFuture<List<Path>> downloadedFiles, Path targetDir) {
            this.downloadedFiles = downloadedFiles;
            this.targetDir = targetDir;
        }

        @Override
        public CompletableFuture<Void> consume(Identifier identifier, List<Path> uploadedFiles) {
            IgniteUtils.deleteIfExists(targetDir);

            if (!uploadedFiles.isEmpty()) {
                Path directory = uploadedFiles.get(0).getParent();
                try {
                    Files.move(directory, targetDir, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

                    try (Stream<Path> stream = Files.list(targetDir)) {
                        downloadedFiles.complete(stream.collect(Collectors.toList()));
                    }
                } catch (IOException e) {
                    downloadedFiles.completeExceptionally(e);
                    return failedFuture(e);
                }
            }

            return completedFuture(null);
        }

        /**
         * Completes future with error.
         *
         * @param e Error.
         */
        private void onError(Throwable e) {
            downloadedFiles.completeExceptionally(e);
        }
    }
}
