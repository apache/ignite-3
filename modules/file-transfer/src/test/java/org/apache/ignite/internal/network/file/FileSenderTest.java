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
import static org.apache.ignite.internal.network.file.messages.FileTransferError.fromThrowable;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.spy;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.file.messages.FileChunkMessage;
import org.apache.ignite.internal.network.file.messages.FileChunkResponse;
import org.apache.ignite.internal.network.file.messages.FileTransferFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(WorkDirectoryExtension.class)
class FileSenderTest extends BaseIgniteAbstractTest {
    private static final long RESPONSE_TIMEOUT = 1000;
    private static final int CHUNK_SIZE = 1024;

    @WorkDirectory
    private Path workDir;

    @Mock
    private MessagingService messagingService;

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private final FileTransferFactory messageFactory = new FileTransferFactory();

    @BeforeEach
    void setUp() {
        lenient().doReturn(completedFuture(messageFactory.fileChunkResponse().build()))
                .when(messagingService)
                .invoke(anyString(), eq(Channel.FILE_TRANSFER_CHANNEL), any(FileChunkMessage.class), eq(RESPONSE_TIMEOUT));
    }

    @AfterEach
    void tearDown() {
        IgniteUtils.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS);
    }

    @Test
    void sendSingleFile() {
        // When.
        Path randomFile = FileGenerator.randomFile(workDir, CHUNK_SIZE);
        UUID transferId = UUID.randomUUID();
        FileSender sender = new FileSender(
                CHUNK_SIZE,
                new Semaphore(4),
                RESPONSE_TIMEOUT,
                messagingService,
                executorService
        );

        // Then - no exception is thrown.
        assertThat(
                sender.send("node2", transferId, List.of(randomFile)),
                willCompleteSuccessfully()
        );
    }

    @Test
    void sendMultipleFiles() {
        // When.
        List<Path> randomFiles = List.of(
                FileGenerator.randomFile(workDir, CHUNK_SIZE),
                FileGenerator.randomFile(workDir, CHUNK_SIZE),
                FileGenerator.randomFile(workDir, CHUNK_SIZE)
        );
        UUID transferId = UUID.randomUUID();
        FileSender sender = new FileSender(
                CHUNK_SIZE,
                new Semaphore(4),
                RESPONSE_TIMEOUT,
                messagingService,
                executorService
        );

        // Then - no exception is thrown.
        assertThat(
                sender.send("node2", transferId, randomFiles),
                willCompleteSuccessfully()
        );
    }

    @Test
    void exceptionIsThrownWhenInvokeReturnException() {
        // Setup messaging service to fail on second file transfer.
        AtomicInteger count = new AtomicInteger();
        doAnswer(invocation -> {
            if (count.incrementAndGet() == 2) {
                return failedFuture(new RuntimeException("Test exception"));
            } else {
                return completedFuture(messageFactory.fileChunkResponse().build());
            }
        })
                .when(messagingService)
                .invoke(anyString(), eq(Channel.FILE_TRANSFER_CHANNEL), any(FileChunkMessage.class), eq(RESPONSE_TIMEOUT));

        // When.
        Path randomFile = FileGenerator.randomFile(workDir, CHUNK_SIZE * 5);
        UUID transferId = UUID.randomUUID();
        FileSender sender = new FileSender(
                CHUNK_SIZE,
                new Semaphore(4),
                RESPONSE_TIMEOUT,
                messagingService,
                executorService
        );

        // Then - exception is thrown.
        assertThat(
                sender.send("node2", transferId, List.of(randomFile)),
                willThrowWithCauseOrSuppressed(RuntimeException.class)
        );
    }

    @Test
    void exceptionIsThrownWhenInvokeReturnAckWithError() {
        // Setup messaging service to fail on second file transfer.
        AtomicInteger count = new AtomicInteger();
        doAnswer(invocation -> {
            if (count.incrementAndGet() == 2) {
                FileChunkResponse ackWithError = messageFactory.fileChunkResponse()
                        .error(fromThrowable(messageFactory, new RuntimeException("Test exception")))
                        .build();

                return completedFuture(ackWithError);
            } else {
                return completedFuture(messageFactory.fileChunkResponse().build());
            }
        })
                .when(messagingService)
                .invoke(anyString(), eq(Channel.FILE_TRANSFER_CHANNEL), any(FileChunkMessage.class), eq(RESPONSE_TIMEOUT));

        // When.
        Path randomFile = FileGenerator.randomFile(workDir, CHUNK_SIZE * 5);
        UUID transferId = UUID.randomUUID();
        FileSender sender = new FileSender(
                CHUNK_SIZE,
                new Semaphore(4),
                RESPONSE_TIMEOUT,
                messagingService,
                executorService
        );

        // Then - exception is thrown.
        assertThat(
                sender.send("node2", transferId, List.of(randomFile)),
                willThrowWithCauseOrSuppressed(RuntimeException.class)
        );
    }


    @Test
    void maxConcurrentRequestsLimitIsNotExceeded() {
        // Setup mock messaging service to emulate long processing and count concurrent requests.
        // Max concurrent requests limit is 5. If it is exceeded, exception is thrown.
        int maxConcurrentRequests = 5;
        AtomicInteger concurrentRequests = new AtomicInteger();

        doAnswer(invocation -> {
            int currentConcurrentRequests = concurrentRequests.incrementAndGet();
            if (currentConcurrentRequests > maxConcurrentRequests) {
                throw new RuntimeException("Max concurrent requests limit exceeded");
            }

            try {
                // Emulate long processing.
                TimeUnit.MILLISECONDS.sleep(5);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                concurrentRequests.decrementAndGet();
            }
            return completedFuture(messageFactory.fileChunkResponse().build());
        })
                .when(messagingService)
                .invoke(anyString(), eq(Channel.FILE_TRANSFER_CHANNEL), any(FileChunkMessage.class), eq(RESPONSE_TIMEOUT));

        // When.
        List<Path> randomFiles = List.of(
                FileGenerator.randomFile(workDir, CHUNK_SIZE),
                FileGenerator.randomFile(workDir, CHUNK_SIZE * 10),
                FileGenerator.randomFile(workDir, CHUNK_SIZE * 20)
        );
        UUID transferId = UUID.randomUUID();

        FileSender sender = new FileSender(
                CHUNK_SIZE,
                new Semaphore(maxConcurrentRequests),
                RESPONSE_TIMEOUT,
                messagingService,
                executorService
        );

        // Then - no exception is thrown.
        assertThat(
                sender.send("node2", transferId, randomFiles),
                willCompleteSuccessfully()
        );
    }

    @Test
    void rateLimiterIsReleasedIfSendThrowsException() {
        // Setup messaging service to fail on second file transfer.
        AtomicInteger count = new AtomicInteger();
        doAnswer(invocation -> {
            if (count.incrementAndGet() == 2) {
                return failedFuture(new RuntimeException("Test exception"));
            } else {
                return completedFuture(messageFactory.fileChunkResponse().build());
            }
        })
                .when(messagingService)
                .invoke(anyString(), eq(Channel.FILE_TRANSFER_CHANNEL), any(FileChunkMessage.class), eq(RESPONSE_TIMEOUT));

        // Setup rate limiter to count tryAcquire and release calls.
        Semaphore rateLimiter = spy(new Semaphore(4));
        AtomicInteger tryAcquireCount = new AtomicInteger();
        doAnswer(invocation -> {
            tryAcquireCount.incrementAndGet();
            return true;
        }).when(rateLimiter).tryAcquire();

        AtomicInteger releaseCount = new AtomicInteger();
        doAnswer(invocation -> {
            releaseCount.incrementAndGet();
            return null;
        }).when(rateLimiter).release();

        // When.
        Path randomFile = FileGenerator.randomFile(workDir, CHUNK_SIZE * 5);
        UUID transferId = UUID.randomUUID();
        FileSender sender = new FileSender(
                CHUNK_SIZE,
                rateLimiter,
                RESPONSE_TIMEOUT,
                messagingService,
                executorService
        );

        // Then - exception is thrown.
        assertThat(
                sender.send("node2", transferId, List.of(randomFile)),
                willThrowWithCauseOrSuppressed(RuntimeException.class)
        );

        // And - rate limiter is released the same number of times as it was acquired.
        assertThat(tryAcquireCount.get(), greaterThan(0));
        assertThat(releaseCount.get(), equalTo(tryAcquireCount.get()));
    }
}
