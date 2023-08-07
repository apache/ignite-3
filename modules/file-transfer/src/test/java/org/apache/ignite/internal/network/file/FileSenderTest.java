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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.NetworkMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
class FileSenderTest {
    private static final int CHUNK_SIZE = 1024;

    @WorkDirectory
    private Path workDir;

    @Test
    void sendSingleFile() throws IOException {
        // when
        File randomFile = FileGenerator.randomFile(workDir, CHUNK_SIZE);
        UUID transferId = UUID.randomUUID();
        Set<NetworkMessage> sentMessages = new CopyOnWriteArraySet<>();
        FileSender sender = new FileSender(
                "node1",
                CHUNK_SIZE,
                4,
                new RateLimiter(4),
                (fileName, message) -> {
                    sentMessages.add(message);
                    return completedFuture(null);
                }
        );

        // then - no exception is thrown
        assertThat(
                sender.send("node2", transferId, List.of(randomFile)),
                willCompleteSuccessfully()
        );

        // and - all messages are sent
        Set<NetworkMessage> expectedMessages = new HashSet<>();
        try (FileTransferMessagesStream stream = new FileTransferMessagesStream(transferId, List.of(randomFile), CHUNK_SIZE)) {
            stream.forEach(expectedMessages::add);
        }

        assertEquals(expectedMessages, sentMessages);
    }

    @Test
    void sendMultipleFiles() throws IOException {
        // when
        List<File> randomFiles = List.of(
                FileGenerator.randomFile(workDir, CHUNK_SIZE),
                FileGenerator.randomFile(workDir, CHUNK_SIZE),
                FileGenerator.randomFile(workDir, CHUNK_SIZE)
        );
        UUID transferId = UUID.randomUUID();
        Set<NetworkMessage> sentMessages = new CopyOnWriteArraySet<>();
        FileSender sender = new FileSender(
                "node1",
                CHUNK_SIZE,
                4,
                new RateLimiter(4),
                (fileName, message) -> {
                    sentMessages.add(message);
                    return completedFuture(null);
                }
        );

        // then - no exception is thrown
        assertThat(
                sender.send("node2", transferId, randomFiles),
                willCompleteSuccessfully()
        );

        // and - all messages are sent
        Set<NetworkMessage> expectedMessages = new HashSet<>();
        try (FileTransferMessagesStream stream = new FileTransferMessagesStream(transferId, randomFiles, CHUNK_SIZE)) {
            stream.forEach(expectedMessages::add);
        }
        assertEquals(expectedMessages, sentMessages);
    }

    @Test
    void exceptionIsThrownIfFileTransferFailed() {
        // when
        File randomFile = FileGenerator.randomFile(workDir, CHUNK_SIZE);
        UUID transferId = UUID.randomUUID();
        AtomicInteger count = new AtomicInteger();
        FileSender sender = new FileSender(
                "node1",
                CHUNK_SIZE,
                1,
                new RateLimiter(4),
                (fileName, message) -> {
                    if (count.incrementAndGet() == 2) {
                        return failedFuture(new RuntimeException("Test exception"));
                    } else {
                        return completedFuture(null);
                    }
                }
        );

        // then - exception is thrown
        assertThat(
                sender.send("node2", transferId, List.of(randomFile)),
                willThrowWithCauseOrSuppressed(RuntimeException.class)
        );
    }

    @Test
    void maxConcurrentRequestsLimitIsNotExceeded() {
        // when
        List<File> randomFiles = List.of(
                FileGenerator.randomFile(workDir, CHUNK_SIZE),
                FileGenerator.randomFile(workDir, CHUNK_SIZE * 10),
                FileGenerator.randomFile(workDir, CHUNK_SIZE * 20)
        );
        UUID transferId = UUID.randomUUID();

        int maxConcurrentRequests = 2;
        AtomicInteger concurrentRequests = new AtomicInteger();
        FileSender sender = new FileSender(
                "node1",
                CHUNK_SIZE,
                10,
                new RateLimiter(maxConcurrentRequests),
                (fileName, message) -> {
                    int currentConcurrentRequests = concurrentRequests.incrementAndGet();
                    if (currentConcurrentRequests > maxConcurrentRequests) {
                        throw new RuntimeException("Max concurrent requests limit exceeded");
                    }

                    try {
                        // emulate long processing
                        TimeUnit.MILLISECONDS.sleep(10);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } finally {
                        concurrentRequests.decrementAndGet();
                    }
                    return completedFuture(null);
                }
        );

        // then - no exception is thrown
        assertThat(
                sender.send("node2", transferId, randomFiles),
                willCompleteSuccessfully()
        );
    }

    @Test
    void rateLimiterIsReleasedIfSendThrowsException() {
        // when
        File randomFile = FileGenerator.randomFile(workDir, CHUNK_SIZE);
        UUID transferId = UUID.randomUUID();
        RateLimiter rateLimiter = mock(RateLimiter.class);
        doReturn(true).when(rateLimiter).tryAcquire();
        FileSender sender = new FileSender(
                "node1",
                CHUNK_SIZE,
                1,
                rateLimiter,
                (fileName, message) -> {
                    throw new RuntimeException("Test exception");
                }
        );

        // then - exception is thrown and rate limiter is released
        assertThat(
                sender.send("node2", transferId, List.of(randomFile)),
                willThrowWithCauseOrSuppressed(RuntimeException.class)
        );

        verify(rateLimiter).release();
    }
}
