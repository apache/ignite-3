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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.file.exception.FileTransferException;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.MessagingService;

/**
 * A class that sends files to a node. It uses a rate limiter to limit the bandwidth used. It also uses a thread pool to send the files in
 * parallel.
 */
class FileSender implements ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(FileSender.class);

    private final int chunkSize;

    private final RateLimiter rateLimiter;

    private final MessagingService messagingService;

    private final ExecutorService executorService;

    FileSender(
            String nodeName,
            int chunkSize,
            int threadPoolSize,
            RateLimiter rateLimiter,
            MessagingService messagingService) {
        this.chunkSize = chunkSize;
        this.rateLimiter = rateLimiter;
        this.executorService = new ThreadPoolExecutor(
                0,
                threadPoolSize,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                NamedThreadFactory.create(nodeName, "file-sender", LOG)
        );
        this.messagingService = messagingService;
    }

    /**
     * Sends the files to the node with the given consistent id.
     *
     * @param receiverConsistentId The consistent id of the node to send the files to.
     * @param id The id of the file transfer.
     * @param files The files to send.
     * @return A future that will be completed when the files are sent.
     */
    CompletableFuture<Void> send(String receiverConsistentId, UUID id, List<Path> files) {
        // It doesn't make sense to continue file transfer if there is a failure in one of the files.
        AtomicBoolean shouldBeCancelled = new AtomicBoolean(false);

        CompletableFuture<?>[] futures = files.stream()
                .map(file -> send(receiverConsistentId, id, file, shouldBeCancelled).whenComplete((v, e) -> {
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
    private CompletableFuture<Void> send(String receiverConsistentId, UUID id, Path path, AtomicBoolean shouldBeCancelled) {
        return supplyAsync(() -> {
            try {
                return FileTransferMessagesStream.fromPath(chunkSize, id, path);
            } catch (IOException e) {
                throw new FileTransferException("Failed to create a file transfer stream", e);
            }
        }, executorService)
                .thenComposeAsync(stream -> {
                    try {
                        if (!stream.hasNextMessage()) {
                            return completedFuture(null);
                        } else {

                            rateLimiter.acquire();

                            return send(receiverConsistentId, stream, shouldBeCancelled)
                                    .whenComplete((v, e) -> {
                                        try {
                                            stream.close();
                                        } catch (IOException ex) {
                                            throw new FileTransferException("Failed to close the file transfer stream", ex);
                                        } finally {
                                            rateLimiter.release();
                                        }
                                    });
                        }
                    } catch (IOException e) {
                        throw new FileTransferException("Failed to send a file: " + path, e);
                    } catch (InterruptedException e) {
                        throw new FileTransferException("Failed to acquire a permit from the rate limiter. Thread was interrupted.", e);
                    }
                }, executorService);
    }

    /**
     * Sends the next message in the stream. If there are no more messages, the future will be completed.
     *
     * @param receiverConsistentId The consistent id of the node to send the files to.
     * @param stream The stream of messages to send.
     * @return A future that will be completed when the next message is sent.
     */
    private CompletableFuture<Void> send(String receiverConsistentId, FileTransferMessagesStream stream, AtomicBoolean shouldBeCancelled) {
        try {
            if (!Thread.currentThread().isInterrupted() && !shouldBeCancelled.get() && stream.hasNextMessage()) {
                return messagingService.send(receiverConsistentId, FILE_TRANSFER_CHANNEL, stream.nextMessage())
                        .thenComposeAsync(it -> send(receiverConsistentId, stream, shouldBeCancelled), executorService);
            } else {
                return completedFuture(null);
            }
        } catch (IOException e) {
            return failedFuture(new FileTransferException("Failed to send files to node: " + receiverConsistentId, e));
        }
    }

    @Override
    public void close() {
        IgniteUtils.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS);
    }
}
