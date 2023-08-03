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

import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.file.messages.FileChunk;
import org.apache.ignite.internal.network.file.messages.FileHeader;
import org.apache.ignite.internal.network.file.messages.FileTransferErrorMessage;
import org.apache.ignite.internal.network.file.messages.FileTransferInfo;

/**
 * File receiver.
 */
class FileReceiver {
    private static final IgniteLogger LOG = Loggers.forClass(FileReceiver.class);

    private final ExecutorService executorService;

    private final Map<UUID, FileTransferMessagesHandler> transferIdToHandler = new ConcurrentHashMap<>();

    FileReceiver(ExecutorService executorService) {
        this.executorService = executorService;
    }

    FileTransferMessagesHandler registerTransfer(UUID transferId, Path handlerDir) {
        FileTransferMessagesHandler handler = new FileTransferMessagesHandler(handlerDir);
        transferIdToHandler.put(transferId, handler);
        return handler;
    }

    void deregisterTransfer(UUID transferId) {
        transferIdToHandler.remove(transferId);
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
        FileTransferMessagesHandler handler = transferIdToHandler.get(info.transferId());
        if (handler == null) {
            throw new FileTransferException("Handler is not found for unknown transferId: " + info.transferId());
        } else {
            handler.handleFileTransferInfo(info);
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
        FileTransferMessagesHandler handler = transferIdToHandler.get(header.transferId());
        if (handler == null) {
            throw new FileTransferException("Handler is not found for unknown transferId: " + header.transferId());
        } else {
            handler.handleFileHeader(header);
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
        FileTransferMessagesHandler handler = transferIdToHandler.get(chunk.transferId());
        if (handler == null) {
            throw new FileTransferException("Handler is not found for unknown transferId: " + chunk.transferId());
        } else {
            handler.handleFileChunk(chunk);
        }
    }

    CompletableFuture<Void> receiveFileTransferErrorMessage(FileTransferErrorMessage errorMessage) {
        return CompletableFuture.runAsync(() -> receiveFileTransferErrorMessage0(errorMessage), executorService)
                .whenComplete((v, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Failed to receive file transfer error message. Id: {}. Exception: {}",
                                errorMessage.transferId(),
                                throwable
                        );
                    }
                });
    }

    private void receiveFileTransferErrorMessage0(FileTransferErrorMessage errorMessage) {
        FileTransferMessagesHandler handler = transferIdToHandler.get(errorMessage.transferId());
        if (handler == null) {
            throw new FileTransferException("Handler is not found for unknown transferId: " + errorMessage.transferId());
        } else {
            handler.handleFileTransferError(new FileTransferException(errorMessage.error().message()));
        }

    }
}
