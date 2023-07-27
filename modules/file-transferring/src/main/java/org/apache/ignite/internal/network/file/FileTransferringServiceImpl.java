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
import java.util.stream.Stream;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.file.messages.ChunkedFile;
import org.apache.ignite.internal.network.file.messages.FileDownloadRequest;
import org.apache.ignite.internal.network.file.messages.FileDownloadRequestImpl;
import org.apache.ignite.internal.network.file.messages.FileDownloadResponse;
import org.apache.ignite.internal.network.file.messages.FileDownloadResponseImpl;
import org.apache.ignite.internal.network.file.messages.FileHeader;
import org.apache.ignite.internal.network.file.messages.FileTransferInfo;
import org.apache.ignite.internal.network.file.messages.FileTransferringMessageType;
import org.apache.ignite.internal.network.file.messages.FileUploadRequest;
import org.apache.ignite.internal.network.file.messages.FileUploadRequestImpl;
import org.apache.ignite.internal.network.file.messages.TransferMetadata;
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

    private static final int CHUNK_SIZE = 1024 * 1024;
    private static final int CONCURRENT_REQUESTS = 10;
    private static final ChannelType FILE_TRANSFERRING_CHANNEL = ChannelType.register((short) 1, "FileTransferring");

    /**
     * Cluster service.
     */
    private final MessagingService messagingService;

    private final Path tempDirectory;

    private final FileSender fileSender;

    private final Map<UUID, FileReceiver> transferIdToReceiver = new ConcurrentHashMap<>();
    private final Map<Short, FileProvider<TransferMetadata>> metadataToProvider = new ConcurrentHashMap<>();
    private final Map<Short, FileHandler<TransferMetadata>> metadataToHandler = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param messagingService Messaging service.
     * @param tempDirectory Temporary directory.
     */
    public FileTransferringServiceImpl(MessagingService messagingService, Path tempDirectory) {
        this.messagingService = messagingService;
        this.tempDirectory = tempDirectory;
        this.fileSender = new FileSender(CHUNK_SIZE, new RateLimiter(CONCURRENT_REQUESTS), (recipientConsistentId, message) -> {
            return messagingService.send(recipientConsistentId, FILE_TRANSFERRING_CHANNEL, message);
        });
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
                    } else if (message instanceof ChunkedFile) {
                        processChunkedFile((ChunkedFile) message);
                    }
                });
    }

    private void processUploadRequest(FileUploadRequest message) {
        createFileReceiver(message.transferId())
                .thenCompose(receiver -> handleFileTransferring(message.transferId(), receiver))
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

                    if (files.isEmpty()) {
                        LOG.error("No files found for download. Metadata: {}", message.metadata());
                        // todo send error response
                        return completedFuture(null);
                    }

                    FileDownloadResponse response = FileDownloadResponseImpl.builder()
                            .build();

                    return messagingService.respond(senderConsistentId, FILE_TRANSFERRING_CHANNEL, response, correlationId)
                            .thenCompose(v -> send(senderConsistentId, message.transferId(), files));
                })
                .thenCompose(it -> it);
    }

    private CompletableFuture<Void> send(String recipientConsistentId, UUID transferId, List<File> files) {
        return fileSender.send(recipientConsistentId, transferId, files)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Failed to send files to node: {}. Exception: {}", recipientConsistentId, e);
                        // todo send error response
                    }
                });
    }

    private void processFileTransferInfo(FileTransferInfo info) {
        FileReceiver receiver = transferIdToReceiver.get(info.transferId());
        if (receiver == null) {
            LOG.warn("File receiver is not found for file transfer info: {}", info);
        } else {
            receiver.receive(info);
        }
    }

    private void processFileHeader(FileHeader header) {
        FileReceiver receiver = transferIdToReceiver.get(header.transferId());
        if (receiver == null) {
            LOG.warn("File receiver is not found for file header: {}", header);
        } else {
            receiver.receive(header);
        }
    }

    private void processChunkedFile(ChunkedFile chunkedFile) {
        FileReceiver receiver = transferIdToReceiver.get(chunkedFile.transferId());
        if (receiver == null) {
            LOG.warn("File receiver is not found for chunked file: {}", chunkedFile);
        } else {
            receiver.receive(chunkedFile);
        }
    }

    private CompletableFuture<FileReceiver> createFileReceiver(UUID transferId) {
        try {
            Path directory = Files.createDirectory(tempDirectory.resolve(transferId.toString()));
            FileReceiver receiver = new FileReceiver(directory);
            transferIdToReceiver.put(transferId, receiver);
            return completedFuture(receiver);
        } catch (IOException e) {
            return failedFuture(e);
        }
    }

    private CompletableFuture<Path> handleFileTransferring(UUID transferId, FileReceiver fileReceiver) {
        return fileReceiver.result()
                .whenComplete((v, e) -> {
                    transferIdToReceiver.remove(transferId);
                    try {
                        fileReceiver.close();
                    } catch (Exception ex) {
                        LOG.error("Failed to close file receiver: {}. Exception: {}", fileReceiver, ex);
                    }
                    if (e != null) {
                        LOG.error("Failed to receive file. Id: {}. Exception: {}", transferId, e);
                        deleteDirectoryIfExists(fileReceiver.dir());
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

    @Override
    public void stop() throws Exception {
        IgniteUtils.closeAllManually(Stream.concat(
                transferIdToReceiver.values().stream(),
                Stream.of(fileSender)
        ));
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
    public CompletableFuture<Path> download(String node, TransferMetadata transferMetadata) {
        UUID transferId = UUID.randomUUID();
        FileDownloadRequest message = FileDownloadRequestImpl.builder()
                .transferId(transferId)
                .metadata(transferMetadata)
                .build();

        return createFileReceiver(message.transferId())
                .thenCompose(fileReceiver -> messagingService.invoke(node, FILE_TRANSFERRING_CHANNEL, message, Long.MAX_VALUE)
                        .thenApply(FileDownloadResponse.class::cast)
                        .thenCompose(response -> {
                            if (response.error() != null) {
                                // todo handle error
                                return failedFuture(new RuntimeException(response.error().errorMessage()));
                            }
                            return handleFileTransferring(transferId, fileReceiver);
                        }));
    }

    @Override
    public CompletableFuture<Void> upload(String node, TransferMetadata transferMetadata) {
        return metadataToProvider.get(transferMetadata.messageType()).files(transferMetadata)
                .thenCompose(files -> {
                    UUID transferId = UUID.randomUUID();
                    FileUploadRequest message = FileUploadRequestImpl.builder()
                            .transferId(transferId)
                            .metadata(transferMetadata)
                            .build();

                    return messagingService.send(node, FILE_TRANSFERRING_CHANNEL, message)
                            .thenCompose(v -> send(node, transferId, files));
                });
    }
}
