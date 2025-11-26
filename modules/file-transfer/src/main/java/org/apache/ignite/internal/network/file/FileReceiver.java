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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.apache.ignite.internal.network.file.exception.FileTransferException;
import org.apache.ignite.internal.network.file.messages.FileChunkMessage;
import org.apache.ignite.internal.network.file.messages.FileHeader;
import org.apache.ignite.internal.util.RefCountedObjectPool;

/**
 * File receiver.
 */
class FileReceiver {
    /**
     * Map from transfer id to file transfer handler.
     */
    private final Map<UUID, FileTransferMessagesHandler> transferIdToHandler = new ConcurrentHashMap<>();

    /**
     * Map from sender consistent id to transfer ids.
     */
    private final Map<String, Set<UUID>> senderConsistentIdToTransferIds = new ConcurrentHashMap<>();

    /**
     * Map from sender consistent id to lock.
     */
    private final RefCountedObjectPool<String, ReentrantLock> senderConsistentIdToLock = new RefCountedObjectPool<>();

    /**
     * Registers file transfer.
     *
     * @param senderConsistentId Sender consistent id.
     * @param transferId Transfer id.
     * @return Future that will be completed when file transfer is finished.
     */
    TransferredFilesCollector registerTransfer(
            String senderConsistentId,
            UUID transferId,
            List<FileHeader> headers,
            Path handlerDir
    ) {
        return doInLock(senderConsistentId, () -> registerTransfer0(senderConsistentId, transferId, headers, handlerDir));
    }

    private TransferredFilesCollector registerTransfer0(
            String senderConsistentId,
            UUID transferId,
            List<FileHeader> headers,
            Path handlerDir
    ) {
        FileTransferMessagesHandler handler = new FileTransferMessagesHandler(headers, handlerDir);

        transferIdToHandler.put(transferId, handler);

        senderConsistentIdToTransferIds.compute(senderConsistentId, (k, v) -> {
            if (v == null) {
                v = new HashSet<>();
            }
            v.add(transferId);
            return v;
        });

        // De-register transfer on completion.
        handler.collectedFiles().whenComplete((files, throwable) -> deregisterTransfer(senderConsistentId, transferId));

        return handler;
    }

    /**
     * De-registers file transfer.
     *
     * @param senderConsistentId Sender consistent id.
     * @param transferId Transfer id.
     */
    private void deregisterTransfer(String senderConsistentId, UUID transferId) {
        doInLock(senderConsistentId, () -> deregisterTransfer0(senderConsistentId, transferId));
    }

    private void deregisterTransfer0(String senderConsistentId, UUID transferId) {
        transferIdToHandler.remove(transferId);

        senderConsistentIdToTransferIds.compute(senderConsistentId, (k, v) -> {
            if (v != null) {
                v.remove(transferId);
                if (v.isEmpty()) {
                    return null;
                }
            }
            return v;
        });
    }

    /**
     * Cancels all transfers from sender.
     *
     * @param senderConsistentId Sender consistent id.
     */
    void cancelTransfersFromSender(String senderConsistentId) {
        doInLock(senderConsistentId, () -> cancelTransfersFromSender0(senderConsistentId));
    }

    private void cancelTransfersFromSender0(String senderConsistentId) {
        Set<UUID> uuids = senderConsistentIdToTransferIds.remove(senderConsistentId);
        if (uuids != null) {
            uuids.forEach(uuid -> {
                transferIdToHandler.get(uuid).handleFileTransferError(new FileTransferException("Transfer was cancelled"));
            });
        }
    }

    /**
     * Cancels transfer.
     *
     * @param transferId Transfer id.
     * @param error Error message.
     */
    void cancelTransfer(UUID transferId, Throwable error) {
        FileTransferMessagesHandler handler = transferIdToHandler.get(transferId);
        if (handler == null) {
            throw new FileTransferException("Handler is not found for unknown transferId: " + transferId);
        } else {
            handler.handleFileTransferError(error);
        }
    }

    /**
     * Receives file chunk.
     *
     * @param chunk File chunk.
     */
    void receiveFileChunk(FileChunkMessage chunk) {
        FileTransferMessagesHandler handler = transferIdToHandler.get(chunk.transferId());
        if (handler == null) {
            throw new FileTransferException("Handler is not found for unknown transferId: " + chunk.transferId());
        } else {
            handler.handleFileChunk(chunk);
        }
    }

    /**
     * Acquires lock for sender consistent id and executes supplier.
     *
     * @param senderConsistentId Sender consistent id.
     * @param supplier Supplier.
     */
    private <V> V doInLock(String senderConsistentId, Supplier<V> supplier) {
        Lock lock = senderConsistentIdToLock.acquire(senderConsistentId, ignored -> new ReentrantLock());
        lock.lock();
        try {
            return supplier.get();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Acquires lock for sender consistent id and executes runnable.
     *
     * @param senderConsistentId Sender consistent id.
     * @param runnable Runnable.
     */
    private void doInLock(String senderConsistentId, Runnable runnable) {
        Lock lock = senderConsistentIdToLock.acquire(senderConsistentId, ignored -> new ReentrantLock());
        lock.lock();
        try {
            runnable.run();
        } finally {
            lock.unlock();
        }
    }
}
