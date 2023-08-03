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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.network.NetworkMessage;

class FileSender {
    private static final IgniteLogger LOG = Loggers.forClass(FileSender.class);

    private final int chunkSize;

    private final RateLimiter rateLimiter;

    private final BiFunction<String, NetworkMessage, CompletableFuture<Void>> send;

    private final ExecutorService executorService;

    FileSender(
            int chunkSize,
            RateLimiter rateLimiter,
            BiFunction<String, NetworkMessage, CompletableFuture<Void>> send,
            ExecutorService executorService) {
        this.send = send;
        this.chunkSize = chunkSize;
        this.rateLimiter = rateLimiter;
        this.executorService = executorService;
    }

    /**
     * Adds files to the queue to be sent to the receiver.
     */
    CompletableFuture<Void> send(String receiverConsistentId, UUID id, List<File> files) {
        return CompletableFuture.runAsync(() -> send0(receiverConsistentId, id, files), executorService);
    }

    private void send0(String receiverConsistentId, UUID id, List<File> files) {
        AtomicBoolean interrupted = new AtomicBoolean(false);
        try (FileTransferMessagesStream stream = new FileTransferMessagesStream(id, files, chunkSize)) {
            while (stream.hasNextMessage() && !interrupted.get()) {
                if (rateLimiter.tryAcquire()) {
                    send.apply(receiverConsistentId, stream.nextMessage())
                            .whenComplete((res, e) -> {
                                if (e != null) {
                                    LOG.error("Failed to send message to node: {}, transfer id: {}. Exception: {}",
                                            receiverConsistentId,
                                            id,
                                            e
                                    );
                                    interrupted.set(true);
                                }
                                rateLimiter.release();
                            });
                }
            }
        } catch (IOException e) {
            throw new FileTransferException("Failed to send files to node: " + receiverConsistentId + ", transfer id: " + id, e);
        }
    }
}
