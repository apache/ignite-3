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

package org.apache.ignite.internal.table.distributed.wrappers;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ExecutorInclinedPlacementDriverTest extends BaseIgniteAbstractTest {
    @Mock
    private PlacementDriver placementDriver;

    private ExecutorService executorForDecorator;

    private ExecutorService anotherExecutor;

    private ExecutorInclinedPlacementDriver decorator;

    private final Context context = new Context();

    @Mock
    private EventListener<PrimaryReplicaEventParameters> listener;

    @BeforeEach
    void createDecorator() {
        executorForDecorator = Executors.newSingleThreadExecutor(TestThread::new);
        anotherExecutor = Executors.newSingleThreadExecutor();

        decorator = new ExecutorInclinedPlacementDriver(placementDriver, executorForDecorator);
    }

    @AfterEach
    void shutDown() {
        if (executorForDecorator != null) {
            IgniteUtils.shutdownAndAwaitTermination(executorForDecorator, 10, SECONDS);
        }

        if (anotherExecutor != null) {
            IgniteUtils.shutdownAndAwaitTermination(anotherExecutor, 10, SECONDS);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @ParameterizedTest
    @EnumSource(AsyncOperation.class)
    void delegatesAsyncOperations(AsyncOperation operation) {
        CompletableFuture originalFuture = new CompletableFuture<>();

        when(operation.execute(placementDriver, context)).thenReturn(originalFuture);

        CompletableFuture<?> finalFuture = operation.execute(decorator, context);

        assertThat(finalFuture, not(completedFuture()));

        originalFuture.complete(null);

        assertThat(finalFuture, willCompleteSuccessfully());

        operation.execute(verify(placementDriver), context);
    }

    /**
     * Makes sure that, if the future is completed right from its creation, its dependant
     * is completed either in the supplied executor or the current thread.
     */
    @ParameterizedTest
    @EnumSource(AsyncOperation.class)
    void completesFuturesInGivenExecutorOrCurrentThreadForCompletedFuture(AsyncOperation operation) {
        when(operation.execute(placementDriver, context)).thenReturn(completedFuture(null));

        AtomicReference<Thread> threadReference = new AtomicReference<>();

        CompletableFuture<?> finalFuture = operation.execute(decorator, context)
                .whenComplete((res, ex) -> threadReference.set(Thread.currentThread()));

        assertThat(finalFuture, willCompleteSuccessfully());

        assertThat(
                threadReference.get(),
                either(instanceOf(TestThread.class))
                        .or(is(Thread.currentThread()))
        );
    }

    /**
     * Makes sure that, even if the future gets completed asynchronously in another thread,
     * its dependant is completed either in the supplied executor or the current thread.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @ParameterizedTest
    @EnumSource(AsyncOperation.class)
    void completesFuturesInGivenExecutorAfterCompletionOfUpstreamInDifferentThread(AsyncOperation operation) {
        CompletableFuture originalFuture = new CompletableFuture<>();
        when(operation.execute(placementDriver, context)).thenReturn(originalFuture);

        AtomicReference<Thread> threadReference = new AtomicReference<>();

        CompletableFuture<?> finalFuture = operation.execute(decorator, context)
                .whenComplete((res, ex) -> threadReference.set(Thread.currentThread()));

        anotherExecutor.submit(() -> originalFuture.complete(null));

        assertThat(finalFuture, willCompleteSuccessfully());

        assertThat(
                threadReference.get(),
                either(instanceOf(TestThread.class))
                        .or(is(Thread.currentThread()))
        );
    }

    @Test
    void delegatesListen() {
        decorator.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, listener);

        verify(placementDriver).listen(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, listener);
    }

    @Test
    void delegatesRemoveListener() {
        decorator.removeListener(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, listener);

        verify(placementDriver).removeListener(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, listener);
    }

    @SuppressWarnings("ClassExplicitlyExtendsThread")
    private static class TestThread extends Thread {
        private TestThread(Runnable target) {
            super(target);
        }
    }

    private static class Context {
        private final ReplicationGroupId groupId = new TestReplicationGroupId("test");

        private final HybridTimestamp timestamp = new HybridTimestamp(1, 1);
    }

    private enum AsyncOperation {
        AWAIT_PRIMARY_REPLICA {
            @Override
            CompletableFuture<?> execute(PlacementDriver placementDriver, Context context) {
                return placementDriver.awaitPrimaryReplicaForTable(context.groupId, context.timestamp, 1, SECONDS);
            }
        },
        GET_PRIMARY_REPLICA {
            @Override
            CompletableFuture<?> execute(PlacementDriver placementDriver, Context context) {
                return placementDriver.getPrimaryReplica(context.groupId, context.timestamp);
            }
        },
        PREVIOUS_PRIMARY_EXPIRED {
            @Override
            CompletableFuture<?> execute(PlacementDriver placementDriver, Context context) {
                return placementDriver.previousPrimaryExpired(context.groupId);
            }
        };

        abstract CompletableFuture<?> execute(PlacementDriver placementDriver, Context context);
    }
}
