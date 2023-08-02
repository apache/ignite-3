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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.file.messages.FileChunk;
import org.apache.ignite.internal.network.file.messages.FileHeader;
import org.apache.ignite.internal.network.file.messages.FileTransferInfo;
import org.apache.ignite.internal.util.FilesUtils;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * File receiver.
 */
class FileReceiver implements ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(FileReceiver.class);

    private final Path tempDirectory;

    private final ExecutorService executorService;

    private final Map<UUID, FileTransferringMessagesHandler> transferIdToHandler = new ConcurrentHashMap<>();

    FileReceiver(Path tempDirectory, ExecutorService executorService) {
        this.tempDirectory = tempDirectory;
        this.executorService = executorService;
    }

    private CompletableFuture<FileTransferringMessagesHandler> createHandler(UUID transferId) {
        try {
            Path directory = Files.createDirectory(tempDirectory.resolve(transferId.toString()));
            FileTransferringMessagesHandler receiver = new FileTransferringMessagesHandler(directory);
            return completedFuture(receiver);
        } catch (IOException e) {
            return failedFuture(e);
        }
    }

    CompletableFuture<FileTransferringMessagesHandler> registerTransfer(UUID transferId) {
        return createHandler(transferId)
                .thenApply(handler -> {
                    transferIdToHandler.put(transferId, handler);
                    handler.result()
                            .whenComplete((path, throwable) -> {
                                transferIdToHandler.remove(transferId);
                                try {
                                    handler.close();
                                } catch (Exception ex) {
                                    LOG.error("Failed to close handler. Exception: {}", ex);
                                }
                                if (throwable != null) {
                                    LOG.error("Failed to receive file. Id: {}. Exception: {}", transferId, throwable);
                                    deleteDirectoryIfExists(handler.dir());
                                }
                            });
                    return handler;
                });
    }

    CompletableFuture<Void> receiveFileTransferInfo(FileTransferInfo info) {
        return CompletableFuture.runAsync(() -> receiveFileTransferInfo0(info), executorService)
                .whenComplete((v, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Failed to receive file transfer info. Id: {}. Exception: {}", info.transferId(), throwable);
                    }
                });
    }

    private void receiveFileTransferInfo0(FileTransferInfo info) {
        FileTransferringMessagesHandler handler = transferIdToHandler.get(info.transferId());
        if (handler == null) {
            throw new FileTransferException("Handler is not found for unknown transferId: " + info.transferId());
        } else {
            handler.receiveFileTransferInfo(info);
        }
    }

    CompletableFuture<Void> receiveFileHeader(FileHeader header) {
        return CompletableFuture.runAsync(() -> receiveFileHeader0(header), executorService)
                .whenComplete((v, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Failed to receive file header. Id: {}. Exception: {}", header.transferId(), throwable);
                    }
                });
    }

    private void receiveFileHeader0(FileHeader header) {
        FileTransferringMessagesHandler handler = transferIdToHandler.get(header.transferId());
        if (handler == null) {
            throw new FileTransferException("Handler is not found for unknown transferId: " + header.transferId());
        } else {
            handler.receiveFileHeader(header);
        }
    }

    CompletableFuture<Void> receiveFileChunk(FileChunk chunk) {
        return CompletableFuture.runAsync(() -> receiveFileChunk0(chunk), executorService)
                .whenComplete((v, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Failed to receive file chunk. Id: {}. Exception: {}", chunk.transferId(), throwable);
                    }
                });
    }

    private void receiveFileChunk0(FileChunk chunk) {
        FileTransferringMessagesHandler handler = transferIdToHandler.get(chunk.transferId());
        if (handler == null) {
            throw new FileTransferException("Handler is not found for unknown transferId: " + chunk.transferId());
        } else {
            handler.receiveFileChunk(chunk);
        }
    }

    private static void deleteDirectoryIfExists(Path directory) {
        try {
            FilesUtils.deleteDirectoryIfExists(directory);
        } catch (IOException e) {
            LOG.error("Failed to delete directory: {}. Exception: {}", directory, e);
        }
    }

    @Override
    public void close() throws Exception {
        IgniteUtils.closeAllManually(transferIdToHandler.values());
    }
}
