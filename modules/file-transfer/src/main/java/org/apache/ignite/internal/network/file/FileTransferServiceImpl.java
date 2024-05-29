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
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.io.IOException;
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
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.annotations.Transferable;
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
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.TopologyService;

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
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        topologyService.addEventHandler(new TopologyEventHandler() {
            @Override
            public void onDisappeared(ClusterNode member) {
                fileReceiver.cancelTransfersFromSender(member.name());
            }
        });

        messagingService.addMessageHandler(FileTransferMessageType.class,
                (message, sender, correlationId) -> {
                    String senderConsistentId = sender.name();

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

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        IgniteUtils.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS);

        return nullCompletedFuture();
    }

    /**
     * Processes {@link FileTransferInitMessage} message. Creates transfer directory, registers transfer, sends response to the sender and
     * passes the transferred files to the consumer.
     *
     * @param message File transfer init message.
     * @param senderConsistentId Sender consistent ID.
     * @param correlationId Correlation ID.
     */
    private void processFileTransferInitMessage(FileTransferInitMessage message, String senderConsistentId, long correlationId) {
        UUID transferId = message.transferId();

        Identifier identifier = message.identifier();

        // Create transfer directory.
        CompletableFuture<Path> directoryFuture = supplyAsync(() -> createTransferDirectory(transferId), executorService)
                .whenComplete((directory, e) -> {
                    if (e != null) {
                        LOG.error("Failed to create transfer directory [transferId={}, identifier={}]", e, transferId, identifier);
                    }
                });

        // We have to register transfer before sending response to avoid the case
        // when chunks are received when FileTransferMessagesHandler is not yet registered.
        CompletableFuture<TransferredFilesCollector> collectorFuture = directoryFuture.thenApply(
                directory -> fileReceiver.registerTransfer(senderConsistentId, transferId, message.headers(), directory)
        );

        // Send response to the sender.
        collectorFuture.handle((collector, throwable) -> {
            return messageFactory.fileTransferInitResponse()
                    .error(throwable != null ? fromThrowable(messageFactory, throwable) : null)
                    .build();
        })
                .thenCompose(response -> messagingService.respond(senderConsistentId, FILE_TRANSFER_CHANNEL, response, correlationId))
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Failed to send file transfer response [transferId={}, identifier={}]", e, transferId,
                                identifier);
                        fileReceiver.cancelTransfer(transferId, e);
                    }
                });

        // Pass transferred files to the consumer.
        collectorFuture.thenCompose(TransferredFilesCollector::collectedFiles)
                .whenComplete((files, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Failed to collect transferred files [transferId={}, identifier={}]",
                                throwable,
                                transferId,
                                identifier
                        );

                        transferIdToDownloadConsumer.computeIfPresent(transferId, (k, v) -> {
                            v.onError(throwable);
                            return null;
                        });
                    }
                })
                .thenCompose(files -> {
                    return getFileConsumer(transferId, identifier).consume(identifier, files)
                            .whenComplete((v, e) -> {
                                if (e != null) {
                                    LOG.error("Failed to process file transfer [transferId={}, identifier={}]",
                                            e,
                                            transferId,
                                            identifier
                                    );
                                }
                            });
                })
                .whenComplete((v, e) -> {
                    transferIdToDownloadConsumer.remove(transferId);
                    directoryFuture.thenAccept(IgniteUtils::deleteIfExists);
                });
    }

    /**
     * Processes the download request. Gets the files from the provider and starts the transfer. If there is an error or there are no files
     * to transfer, then sends the response to the sender.
     *
     * @param message Download request.
     * @param senderConsistentId Sender consistent ID.
     * @param correlationId Correlation ID.
     */
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

    /**
     * Processes {@link FileChunkMessage} message. Passes the chunk to the receiver and sends the response to the sender.
     *
     * @param message File chunk message.
     * @param senderConsistentId Sender consistent ID.
     * @param correlationId Correlation ID.
     */
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

    /**
     * Processes {@link FileTransferErrorMessage} message. Cancels the corresponding transfer.
     *
     * @param message File transfer error message.
     */
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

    /**
     * Transfers files to the node with the given consistent id. Sends a {@link FileTransferInitMessage} to the node and then sends the
     * files. If the node responds with an error, the returned future will be completed exceptionally.
     *
     * @param targetNodeConsistentId The consistent id of the node to transfer the files to.
     * @param transferId The id of the transfer.
     * @param identifier The identifier of the files.
     * @param paths The paths of the files to transfer.
     * @return A future that will be completed when the transfer is complete.
     */
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

    /**
     * Returns the file consumer for the given transfer ID or identifier. If there is no consumer for the transfer ID, the consumer for the
     * identifier is returned.
     *
     * @param transferId The transfer ID.
     * @param identifier The identifier.
     * @return The file consumer.
     */
    private <M extends Identifier> FileConsumer<M> getFileConsumer(UUID transferId, M identifier) {
        return transferIdToDownloadConsumer.containsKey(transferId)
                ? (FileConsumer<M>) transferIdToDownloadConsumer.get(transferId)
                : getFileConsumer(identifier);
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
            return java.nio.file.Files.createDirectories(transferDirectory.resolve(transferId.toString()));
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
                    java.nio.file.Files.move(directory, targetDir, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

                    try (Stream<Path> stream = java.nio.file.Files.list(targetDir)) {
                        downloadedFiles.complete(stream.collect(Collectors.toList()));
                    }
                } catch (IOException e) {
                    downloadedFiles.completeExceptionally(e);
                    return failedFuture(e);
                }
            }

            return nullCompletedFuture();
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
