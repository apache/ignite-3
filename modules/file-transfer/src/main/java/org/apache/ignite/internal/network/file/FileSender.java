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

import static java.util.concurrent.Executors.newSingleThreadExecutor;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.network.NetworkMessage;

class FileSender implements ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(FileSender.class);
    private final int chunkSize;
    private final RateLimiter rateLimiter;
    private final BiFunction<String, NetworkMessage, CompletableFuture<Void>> send;

    private final Queue<FileTransferringRequest> filesToSend = new ConcurrentLinkedQueue<>();

    private final ExecutorService executorService = newSingleThreadExecutor(
            new NamedThreadFactory("FileSenderExecutor", LOG)
    );

    FileSender(
            int chunkSize,
            RateLimiter rateLimiter,
            BiFunction<String, NetworkMessage, CompletableFuture<Void>> send
    ) {
        this.send = send;
        this.chunkSize = chunkSize;
        this.rateLimiter = rateLimiter;
    }

    /**
     * Adds files to the queue to be sent to the receiver.
     */
    CompletableFuture<Void> send(String receiverConsistentId, UUID id, List<File> files) {
        FileTransferringRequest request = new FileTransferringRequest(receiverConsistentId, id, files, chunkSize);
        filesToSend.add(request);
        executorService.submit(this::processQueue);
        return request.result();
    }

    /**
     * Processes the queue of files to be sent.
     */
    private void processQueue() {
        while (!filesToSend.isEmpty()) {
            FileTransferringRequest request = filesToSend.peek();
            try {
                FileTransferringMessagesStream stream = request.messagesStream;
                while (stream.hasNextMessage() && rateLimiter.tryAcquire()) {
                    send.apply(request.receiverConsistentId, stream.nextMessage())
                            .whenComplete((res, e) -> {
                                if (e != null) {
                                    request.complete(e);
                                }
                                rateLimiter.release();
                            });
                }
                if (!stream.hasNextMessage()) {
                    request.complete();
                    filesToSend.remove();
                }
            } catch (Exception e) {
                request.complete(e);
            }
        }
    }

    @Override
    public void close() {
        executorService.shutdown();
        filesToSend.forEach(it -> it.complete(new InterruptedException("File sender is closed")));
    }

    private static class FileTransferringRequest {
        /**
         * Receiver consistent id.
         */
        private final String receiverConsistentId;

        /**
         * Messages stream.
         */
        private final FileTransferringMessagesStream messagesStream;

        /**
         * Result future. Will be completed when all messages are sent.
         */
        private final CompletableFuture<Void> result = new CompletableFuture<>();

        private FileTransferringRequest(String receiverConsistentId, UUID id, List<File> files, int chunkSize) {
            this.receiverConsistentId = receiverConsistentId;
            this.messagesStream = new FileTransferringMessagesStream(id, files, chunkSize);
        }

        CompletableFuture<Void> result() {
            return result;
        }

        void complete() {
            result.complete(null);
            try {
                messagesStream.close();
            } catch (IOException e) {
                LOG.error("Failed to close file messages stream", e);
            }
        }

        void complete(Throwable e) {
            result.completeExceptionally(e);
            try {
                messagesStream.close();
            } catch (IOException ex) {
                LOG.error("Failed to close file messages stream", ex);
            }
        }
    }
}
