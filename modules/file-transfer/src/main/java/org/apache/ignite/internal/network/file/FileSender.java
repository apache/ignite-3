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

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.ignite.internal.network.file.Channel.FILE_TRANSFER_CHANNEL;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
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

    private final ExecutorService executorService;

    private final Queue<FileTransfer> requests = new ConcurrentLinkedQueue<>();

    FileSender(
            int chunkSize,
            Semaphore rateLimiter,
            MessagingService messagingService,
            ExecutorService executorService
    ) {
        this.chunkSize = chunkSize;
        this.rateLimiter = rateLimiter;
        this.messagingService = messagingService;
        this.executorService = executorService;
    }

    /**
     * Creates {@link FileTransfer} objects for each file and places them in the queue. The files will be sent when the rate limiter is
     * available.
     *
     * @param targetNodeConsistentId The consistent ID of the node to send the files to.
     * @param transferId The ID of the transfer.
     * @param paths The paths of the files to send.
     * @return A future that will be completed when the transfer is complete.
     */
    CompletableFuture<Void> send(String targetNodeConsistentId, UUID transferId, List<Path> paths) {
        // It doesn't make sense to continue file transfer if there is a failure in one of the files.
        AtomicBoolean shouldBeCancelled = new AtomicBoolean(false);

        List<FileTransfer> transfers = paths.stream()
                .map(path -> new FileTransfer(targetNodeConsistentId, transferId, path, shouldBeCancelled))
                .collect(Collectors.toList());

        CompletableFuture[] results = transfers.stream().map(it -> it.result)
                .map(it -> it.whenComplete((v, e) -> {
                    if (e != null) {
                        shouldBeCancelled.set(true);
                    }
                }))
                .toArray(CompletableFuture[]::new);

        transfers.forEach(this::processTransfer);

        return allOf(results);
    }

    /**
     * Processes the given transfer. If the rate limiter is not available, the transfer will be processed later.
     *
     * @param transfer The transfer to process.
     */
    private void processTransfer(FileTransfer transfer) {
        if (rateLimiter.tryAcquire()) {
            sendTransfer(transfer)
                    .thenCompose(v -> processNextTransfer())
                    .whenComplete((v, e) -> rateLimiter.release());
        } else {
            requests.add(transfer);
        }
    }

    /**
     * Process the next transfer in the queue. If the rate limiter is not available, the transfer will be processed later.
     *
     * @return A future that will be completed when the request is processed.
     */
    private CompletableFuture<Void> processNextTransfer() {
        return completedFuture(requests.poll())
                .thenComposeAsync(transfer -> {
                    if (transfer == null) {
                        return completedFuture(null);
                    } else {
                        return sendTransfer(transfer).thenCompose(v -> processNextTransfer());
                    }
                }, executorService);
    }

    /**
     * Sends the file to the node with the given consistent id.
     *
     * @param transfer The transfer to send.
     * @return A future that will be completed when the file is sent. The future will be completed successfully always, even if the file is
     *         not sent.
     */
    private CompletableFuture<Void> sendTransfer(FileTransfer transfer) {
        return sendFile(transfer.receiverConsistentId, transfer.transferId, transfer.path, transfer.shouldBeCancelled)
                .handle((v, e) -> {
                    if (e == null) {
                        transfer.result.complete(null);
                    } else {
                        transfer.result.completeExceptionally(e);
                    }
                    return null;
                });
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
            }, executorService)
                    .thenCompose(stream -> {
                        return sendMessagesFromStream(receiverConsistentId, stream, shouldBeCancelled)
                                .whenComplete((v, e) -> {
                                    try {
                                        stream.close();
                                    } catch (IOException ex) {
                                        throw new FileTransferException("Failed to close the file transfer stream", ex);
                                    }
                                });
                    });
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
                        .thenComposeAsync(
                                ignored -> sendMessagesFromStream(receiverConsistentId, stream, shouldBeCancelled),
                                executorService
                        );
            } else {
                return completedFuture(null);
            }
        } catch (IOException e) {
            return failedFuture(e);
        }
    }

    private static class FileTransfer {
        private final String receiverConsistentId;
        private final UUID transferId;
        private final Path path;
        private final AtomicBoolean shouldBeCancelled;
        private final CompletableFuture<Void> result = new CompletableFuture<>();

        private FileTransfer(String receiverConsistentId, UUID transferId, Path path, AtomicBoolean shouldBeCancelled) {
            this.receiverConsistentId = receiverConsistentId;
            this.transferId = transferId;
            this.path = path;
            this.shouldBeCancelled = shouldBeCancelled;
        }
    }
}
