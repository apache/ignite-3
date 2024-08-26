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

package org.apache.ignite.internal.table.distributed.schema;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureCompletedMatcher.completedFuture;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ExecutorInclinedSchemaSyncServiceTest extends BaseIgniteAbstractTest {
    @Mock
    private SchemaSyncService schemaSyncService;

    private ExecutorService executorForDecorator;

    private ExecutorService anotherExecutor;

    private ExecutorInclinedSchemaSyncService decorator;

    private final HybridTimestamp timestamp = new HybridTimestamp(1, 1);

    @BeforeEach
    void createDecorator() {
        executorForDecorator = Executors.newSingleThreadExecutor(TestThread::new);
        anotherExecutor = Executors.newSingleThreadExecutor();

        decorator = new ExecutorInclinedSchemaSyncService(schemaSyncService, executorForDecorator);
    }

    @AfterEach
    void shutDown() {
        if (executorForDecorator != null) {
            IgniteUtils.shutdownAndAwaitTermination(executorForDecorator, 10, TimeUnit.SECONDS);
        }

        if (anotherExecutor != null) {
            IgniteUtils.shutdownAndAwaitTermination(anotherExecutor, 10, TimeUnit.SECONDS);
        }
    }

    @Test
    void delegatesWaitForMetadataCompleteness() {
        CompletableFuture<Void> originalFuture = new CompletableFuture<>();

        when(schemaSyncService.waitForMetadataCompleteness(timestamp)).thenReturn(originalFuture);

        CompletableFuture<Void> finalFuture = decorator.waitForMetadataCompleteness(timestamp);

        assertThat(finalFuture, not(completedFuture()));

        originalFuture.complete(null);

        assertThat(finalFuture, willCompleteSuccessfully());

        verify(schemaSyncService).waitForMetadataCompleteness(timestamp);
    }

    /**
     * Makes sure that, if the schema sync future is completed right from its creation, its dependant
     * is completed either in the supplied executor or the current thread.
     */
    @Test
    void completesFuturesInGivenExecutorOrCurrentThreadForCompletedFuture() {
        when(schemaSyncService.waitForMetadataCompleteness(timestamp)).thenReturn(completedFuture(null));

        AtomicReference<Thread> threadReference = new AtomicReference<>();

        CompletableFuture<Void> finalFuture = decorator.waitForMetadataCompleteness(timestamp)
                .whenComplete((res, ex) -> threadReference.set(Thread.currentThread()));

        assertThat(finalFuture, willCompleteSuccessfully());

        assertThat(
                threadReference.get(),
                either(instanceOf(TestThread.class))
                        .or(is(Thread.currentThread()))
        );
    }

    /**
     * Makes sure that, even if the schema sync future gets completed asynchronously in another thread,
     * its dependant is completed either in the supplied executor or the current thread.
     */
    @Test
    void completesFuturesInGivenExecutorAfterCompletionOfUpstreamInDifferentThread() {
        CompletableFuture<Void> originalFuture = new CompletableFuture<>();
        when(schemaSyncService.waitForMetadataCompleteness(timestamp)).thenReturn(originalFuture);

        AtomicReference<Thread> threadReference = new AtomicReference<>();

        CompletableFuture<Void> finalFuture = decorator.waitForMetadataCompleteness(timestamp)
                .whenComplete((res, ex) -> threadReference.set(Thread.currentThread()));

        anotherExecutor.submit(() -> originalFuture.complete(null));

        assertThat(finalFuture, willCompleteSuccessfully());

        assertThat(
                threadReference.get(),
                either(instanceOf(TestThread.class))
                        .or(is(Thread.currentThread()))
        );
    }

    @SuppressWarnings("ClassExplicitlyExtendsThread")
    private static class TestThread extends Thread {
        private TestThread(Runnable target) {
            super(target);
        }
    }
}
