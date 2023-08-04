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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.file.messages.FileChunkMessage;
import org.apache.ignite.internal.network.file.messages.FileHeaderMessage;
import org.apache.ignite.internal.network.file.messages.FileTransferErrorMessage;
import org.apache.ignite.internal.network.file.messages.FileTransferInfoMessage;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.RefCountedObjectPool;

/**
 * File receiver.
 */
class FileReceiver {
    private static final IgniteLogger LOG = Loggers.forClass(FileReceiver.class);

    private final ExecutorService executorService;

    private final Map<UUID, FileTransferMessagesHandler> transferIdToHandler = new ConcurrentHashMap<>();

    private final Map<String, Set<UUID>> senderConsistentIdToTransferIds = new ConcurrentHashMap<>();

    private final RefCountedObjectPool<String, ReentrantLock> senderConsistentIdToLock = new RefCountedObjectPool<>();

    FileReceiver(String nodeName, int threadPoolSize) {
        this.executorService = new ThreadPoolExecutor(
                0,
                threadPoolSize,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                NamedThreadFactory.create(nodeName, "file-receiver", LOG)
        );
    }

    FileTransferMessagesHandler registerTransfer(String senderConsistentId, UUID transferId, Path handlerDir) {
        ReentrantLock lock = senderConsistentIdToLock.acquire(senderConsistentId, ignored -> new ReentrantLock());
        lock.lock();
        try {
            FileTransferMessagesHandler handler = new FileTransferMessagesHandler(handlerDir);
            transferIdToHandler.put(transferId, handler);
            senderConsistentIdToTransferIds.compute(senderConsistentId, (k, v) -> {
                if (v == null) {
                    v = new HashSet<>();
                }
                v.add(transferId);
                return v;
            });
            handler.result().whenComplete((files, throwable) -> deregisterTransfer(senderConsistentId, transferId));
            return handler;
        } finally {
            lock.unlock();
        }
    }

    private void deregisterTransfer(String senderConsistentId, UUID transferId) {
        ReentrantLock lock = senderConsistentIdToLock.acquire(senderConsistentId, ignored -> new ReentrantLock());
        lock.lock();
        try {
            transferIdToHandler.remove(transferId);
            Set<UUID> uuids = senderConsistentIdToTransferIds.get(senderConsistentId);
            uuids.remove(transferId);
            if (uuids.isEmpty()) {
                senderConsistentIdToTransferIds.remove(senderConsistentId);
            }
        } finally {
            lock.unlock();
        }
    }

    void cancelTransfersFromSender(String senderConsistentId) {
        ReentrantLock lock = senderConsistentIdToLock.acquire(senderConsistentId, ignored -> new ReentrantLock());
        lock.lock();
        try {
            new HashSet<>(senderConsistentIdToTransferIds.get(senderConsistentId))
                    .stream()
                    .forEach(uuid -> {
                        transferIdToHandler.get(uuid).handleFileTransferError(new FileTransferException("Transfer was cancelled"));
                    });
        } finally {
            lock.unlock();
        }
    }

    CompletableFuture<Void> receiveFileTransferInfo(FileTransferInfoMessage info) {
        return CompletableFuture.runAsync(() -> receiveFileTransferInfo0(info), executorService)
                .whenComplete((v, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Failed to receive file transfer info. Id: {}. Exception: {}", info.transferId(), throwable);
                    }
                });
    }

    void stop() {
        IgniteUtils.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS);
    }

    private void receiveFileTransferInfo0(FileTransferInfoMessage info) {
        FileTransferMessagesHandler handler = transferIdToHandler.get(info.transferId());
        if (handler == null) {
            throw new FileTransferException("Handler is not found for unknown transferId: " + info.transferId());
        } else {
            handler.handleFileTransferInfo(info);
        }
    }

    CompletableFuture<Void> receiveFileHeader(FileHeaderMessage header) {
        return CompletableFuture.runAsync(() -> receiveFileHeader0(header), executorService)
                .whenComplete((v, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Failed to receive file header. Id: {}. Exception: {}", header.transferId(), throwable);
                    }
                });
    }

    private void receiveFileHeader0(FileHeaderMessage header) {
        FileTransferMessagesHandler handler = transferIdToHandler.get(header.transferId());
        if (handler == null) {
            throw new FileTransferException("Handler is not found for unknown transferId: " + header.transferId());
        } else {
            handler.handleFileHeader(header);
        }
    }

    CompletableFuture<Void> receiveFileChunk(FileChunkMessage chunk) {
        return CompletableFuture.runAsync(() -> receiveFileChunk0(chunk), executorService)
                .whenComplete((v, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Failed to receive file chunk. Id: {}. Exception: {}", chunk.transferId(), throwable);
                    }
                });
    }

    private void receiveFileChunk0(FileChunkMessage chunk) {
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
            handler.handleFileTransferError(new FileTransferException(errorMessage.error()));
        }
    }
}
