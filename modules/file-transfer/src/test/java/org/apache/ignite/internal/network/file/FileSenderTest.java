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
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.MessagingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.invocation.Invocation;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(WorkDirectoryExtension.class)
class FileSenderTest {
    private static final int CHUNK_SIZE = 1024;

    @WorkDirectory
    private Path workDir;

    @Mock
    private MessagingService messagingService;

    @BeforeEach
    void setUp() {
        doReturn(completedFuture(null)).when(messagingService).send(anyString(), any(), any());
    }

    @Test
    void sendSingleFile() {
        // When.
        Path randomFile = FileGenerator.randomFile(workDir, CHUNK_SIZE);
        UUID transferId = UUID.randomUUID();
        FileSender sender = new FileSender(
                "node1",
                CHUNK_SIZE,
                4,
                new RateLimiterImpl(4),
                messagingService
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
                "node1",
                CHUNK_SIZE,
                4,
                new RateLimiterImpl(4),
                messagingService
        );

        // Then - no exception is thrown.
        assertThat(
                sender.send("node2", transferId, randomFiles),
                willCompleteSuccessfully()
        );
    }

    @Test
    void exceptionIsThrownIfFileTransferFailed() {
        // Setup messaging service to fail on second file transfer.
        AtomicInteger count = new AtomicInteger();
        given(messagingService.send(anyString(), any(), any())).will(invocation -> {
            if (count.incrementAndGet() == 2) {
                return failedFuture(new RuntimeException("Test exception"));
            } else {
                return completedFuture(null);
            }

        });

        // When.
        Path randomFile = FileGenerator.randomFile(workDir, CHUNK_SIZE * 5);
        UUID transferId = UUID.randomUUID();
        FileSender sender = new FileSender(
                "node1",
                CHUNK_SIZE,
                1,
                new RateLimiterImpl(4),
                messagingService
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

        given(messagingService.send(anyString(), any(), any())).will(invocation -> {
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
            return completedFuture(null);
        });

        // When.
        List<Path> randomFiles = List.of(
                FileGenerator.randomFile(workDir, CHUNK_SIZE),
                FileGenerator.randomFile(workDir, CHUNK_SIZE * 10),
                FileGenerator.randomFile(workDir, CHUNK_SIZE * 20)
        );
        UUID transferId = UUID.randomUUID();

        FileSender sender = new FileSender(
                "node1",
                CHUNK_SIZE,
                10,
                new RateLimiterImpl(maxConcurrentRequests),
                messagingService
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
        given(messagingService.send(anyString(), any(), any())).will(invocation -> {
            if (count.incrementAndGet() == 2) {
                return failedFuture(new RuntimeException("Test exception"));
            } else {
                return completedFuture(null);
            }

        });

        // When.
        Path randomFile = FileGenerator.randomFile(workDir, CHUNK_SIZE * 5);
        UUID transferId = UUID.randomUUID();
        RateLimiter rateLimiter = mock(RateLimiter.class);
        FileSender sender = new FileSender(
                "node1",
                CHUNK_SIZE,
                1,
                rateLimiter,
                messagingService
        );

        // Then - exception is thrown.
        assertThat(
                sender.send("node2", transferId, List.of(randomFile)),
                willThrowWithCauseOrSuppressed(RuntimeException.class)
        );

        // And - rate limiter is released.
        Collection<Invocation> invocations = mockingDetails(rateLimiter).getInvocations();
        long expectedCount = invocations
                .stream()
                .filter(it -> it.getMethod().getName().equals("acquire"))
                .count();

        assertThat(expectedCount, greaterThan(0L));
        verify(rateLimiter, times((int) expectedCount)).release();
    }
}
