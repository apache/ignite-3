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
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.ignite.internal.network.file.Channel.FILE_TRANSFER_CHANNEL;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.network.file.exception.FileTransferException;
import org.apache.ignite.network.MessagingService;

/**
 * A class that sends files to a node. It uses a rate limiter to limit the bandwidth used. It also uses a thread pool to send the files in
 * parallel.
 */
class FileSender {
    private final int chunkSize;

    private final Semaphore rateLimiter;

    private final MessagingService messagingService;

    private final Queue<FileTransferRequest> requests = new ConcurrentLinkedQueue<>();

    private final Map<FileTransferRequest, CompletableFuture<Void>> results = new ConcurrentHashMap<>();

    FileSender(
            int chunkSize,
            Semaphore rateLimiter,
            MessagingService messagingService) {
        this.chunkSize = chunkSize;
        this.rateLimiter = rateLimiter;
        this.messagingService = messagingService;
    }

    CompletableFuture<Void> send(String targetNodeConsistentId, UUID transferId, List<Path> paths) {
        if (rateLimiter.tryAcquire()) {
            return send0(targetNodeConsistentId, transferId, paths)
                    .whenComplete((v, e) -> {
                        rateLimiter.release();
                        processQueue();
                    });
        } else {
            // If the rate limiter is full, we should place the request in a queue and wait for the rate limiter to be released.
            return submitRequestToQueue(targetNodeConsistentId, transferId, paths);
        }
    }

    private CompletableFuture<Void> send0(String targetNodeConsistentId, UUID transferId, List<Path> paths) {
        // It doesn't make sense to continue file transfer if there is a failure in one of the files.
        AtomicBoolean shouldBeCancelled = new AtomicBoolean(false);

        CompletableFuture<?>[] futures = paths.stream()
                .map(file -> sendFile(targetNodeConsistentId, transferId, file, shouldBeCancelled)
                        .whenComplete((v, e) -> {
                            if (e != null) {
                                shouldBeCancelled.set(true);
                            }
                        }))
                .toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(futures);
    }

    /**
     * Sends the file to the node with the given consistent id.
     *
     * @param receiverConsistentId The consistent id of the node to send the file to.
     * @param id The id of the file transfer.
     * @param path The path of the file to send.
     * @return A future that will be completed when the file is sent.
     */
    private CompletableFuture<Void> sendFile(String receiverConsistentId, UUID id, Path path, AtomicBoolean shouldBeCancelled) {
        if (path.toFile().length() == 0) {
            return completedFuture(null);
        } else {
            return supplyAsync(() -> {
                try {
                    return FileChunkMessagesStream.fromPath(chunkSize, id, path);
                } catch (IOException e) {
                    throw new FileTransferException("Failed to create a file transfer stream", e);
                }
            }).thenCompose(stream -> {
                return sendMessagesFromStream(receiverConsistentId, stream, shouldBeCancelled).thenApply(ignored -> stream);
            }).whenComplete((stream, e) -> {
                try {
                    stream.close();
                } catch (IOException ex) {
                    throw new FileTransferException("Failed to close the file transfer stream", ex);
                }
            }).thenApply(stream -> null);
        }
    }

    /**
     * Sends the next message in the stream. If there are no more messages, the future will be completed.
     *
     * @param receiverConsistentId The consistent id of the node to send the files to.
     * @param stream The stream of messages to send.
     * @return A future that will be completed when the next message is sent.
     */
    private CompletableFuture<Void> sendMessagesFromStream(
            String receiverConsistentId,
            FileChunkMessagesStream stream,
            AtomicBoolean shouldBeCancelled
    ) {
        try {
            if (stream.hasNextMessage() && !shouldBeCancelled.get()) {
                return messagingService.send(receiverConsistentId, FILE_TRANSFER_CHANNEL, stream.nextMessage())
                        .thenCompose(ignored -> sendMessagesFromStream(receiverConsistentId, stream, shouldBeCancelled));
            } else {
                return completedFuture(null);
            }
        } catch (IOException e) {
            return failedFuture(e);
        }
    }

    /**
     * Submits a file transfer request to the queue.
     */
    private CompletableFuture<Void> submitRequestToQueue(String targetNodeConsistentId, UUID transferId, List<Path> paths) {
        FileTransferRequest request = new FileTransferRequest(targetNodeConsistentId, transferId, paths);

        CompletableFuture<Void> result = new CompletableFuture<>();
        results.put(request, result);

        requests.add(request);

        return result;
    }

    /**
     * Processes the queue of file transfer requests. If the rate limiter is full, the request will not be processed.
     */
    private synchronized void processQueue() {
        FileTransferRequest request = requests.peek();

        if (request != null) {
            if (rateLimiter.tryAcquire()) {
                requests.remove();

                CompletableFuture<Void> result = results.remove(request);

                Objects.requireNonNull(result);

                send0(request.receiverConsistentId, request.transferId, request.paths)
                        .whenComplete((v, e) -> {
                            rateLimiter.release();

                            if (e != null) {
                                result.completeExceptionally(e);
                            } else {
                                result.complete(null);
                            }
                        });
            }
        }
    }

    private static class FileTransferRequest {
        private final String receiverConsistentId;
        private final UUID transferId;
        private final List<Path> paths;

        private FileTransferRequest(String receiverConsistentId, UUID transferId, List<Path> paths) {
            this.receiverConsistentId = receiverConsistentId;
            this.transferId = transferId;
            this.paths = paths;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FileTransferRequest that = (FileTransferRequest) o;
            return Objects.equals(receiverConsistentId, that.receiverConsistentId) && Objects.equals(transferId, that.transferId)
                    && Objects.equals(paths, that.paths);
        }

        @Override
        public int hashCode() {
            return Objects.hash(receiverConsistentId, transferId, paths);
        }
    }
}
