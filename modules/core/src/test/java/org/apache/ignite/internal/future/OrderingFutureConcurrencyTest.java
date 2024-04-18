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

package org.apache.ignite.internal.future;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for concurrency aspects of {@link OrderingFuture}.
 */
class OrderingFutureConcurrencyTest {
    private static final RuntimeException cause = new RuntimeException();

    private final Object lockMonitor = new Object();

    @Test
    void concurrentAdditionOfWhenCompleteCallbacksIsCorrect() throws Exception {
        OrderingFuture<Integer> future = new OrderingFuture<>();

        AtomicInteger counter = new AtomicInteger();

        Runnable addIncrementerTask = () -> {
            for (int i = 0; i < 10_000; i++) {
                future.whenComplete((res, ex) -> counter.incrementAndGet());
            }
        };

        executeInParallel(addIncrementerTask, addIncrementerTask);

        future.complete(1);

        assertThat(counter.get(), is(20_000));
    }

    private static void executeInParallel(Runnable task1, Runnable task2) throws InterruptedException {
        Thread thread1 = new Thread(task1);
        Thread thread2 = new Thread(task2);

        thread1.start();
        thread2.start();

        try {
            thread1.join();
            thread2.join();
        } finally {
            thread1.interrupt();
            thread2.interrupt();
        }
    }

    @Test
    void concurrentAdditionOfThenComposeToCompletableCallbacksIsCorrect() throws Exception {
        OrderingFuture<Integer> future = new OrderingFuture<>();

        AtomicInteger counter = new AtomicInteger();

        Runnable addIncrementerTask = () -> {
            for (int i = 0; i < 10_000; i++) {
                future.thenComposeToCompletable(x -> {
                    counter.incrementAndGet();
                    return nullCompletedFuture();
                });
            }
        };

        executeInParallel(addIncrementerTask, addIncrementerTask);

        future.complete(1);

        assertThat(counter.get(), is(20_000));
    }

    @Test
    void concurrentAdditionOfThenComposeCallbacksIsCorrect() throws Exception {
        OrderingFuture<Integer> future = new OrderingFuture<>();

        AtomicInteger counter = new AtomicInteger();

        Runnable addIncrementerTask = () -> {
            for (int i = 0; i < 10_000; i++) {
                future.thenCompose(x -> {
                    counter.incrementAndGet();
                    return OrderingFuture.completedFuture(null);
                });
            }
        };

        executeInParallel(addIncrementerTask, addIncrementerTask);

        future.complete(1);

        assertThat(counter.get(), is(20_000));
    }

    // Normally, each test completes in 1.5 seconds. 60 is to accomodate for possible TC-side issues.
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @ParameterizedTest
    @MethodSource("completionsAndOperations")
    void noDeadlockBetweenCompletionAndOtherOperations(Completion completion, Operation operation) throws Exception {
        for (int i = 0; i < 10; i++) {
            OrderingFuture<Void> future = futureThatLocksMonitorViaCallbackOnCompletion();

            Runnable otherOperationTask = () -> {
                synchronized (lockMonitor) {
                    long started = System.nanoTime();
                    while (System.nanoTime() < started + TimeUnit.MILLISECONDS.toNanos(100)) {
                        operation.execute(future);
                    }
                }
            };

            executeInParallel(completion.completionTask(future), otherOperationTask);
        }
    }

    private static Stream<Arguments> completionsAndOperations() {
        List<Arguments> args = new ArrayList<>();

        for (Completion completion : Completion.values()) {
            for (Operation operation : Operation.values()) {
                args.add(Arguments.of(completion, operation));
            }
        }

        return args.stream();
    }

    private OrderingFuture<Void> futureThatLocksMonitorViaCallbackOnCompletion() {
        OrderingFuture<Void> future = new OrderingFuture<>();

        future.whenComplete((res, ex) -> {
            synchronized (lockMonitor) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        });

        return future;
    }

    private enum Completion {
        NORMAL {
            @Override
            Runnable completionTask(OrderingFuture<?> future) {
                return () -> future.complete(null);
            }
        },
        EXCEPTIONAL {
            @Override
            Runnable completionTask(OrderingFuture<?> future) {
                return () -> future.completeExceptionally(cause);
            }
        };

        abstract Runnable completionTask(OrderingFuture<?> future);
    }

    private enum Operation {
        COMPLETE_NORMALLY {
            @Override
            void execute(OrderingFuture<?> future) {
                future.complete(null);
            }
        },
        COMPLETE_EXCEPTIONALLY {
            @Override
            void execute(OrderingFuture<?> future) {
                future.completeExceptionally(cause);
            }
        },
        IS_COMPLETED_EXCEPTIONALLY {
            @Override
            void execute(OrderingFuture<?> future) {
                future.isCompletedExceptionally();
            }
        },
        WHEN_COMPLETE {
            @Override
            void execute(OrderingFuture<?> future) {
                future.whenComplete((res, ex) -> {});
            }
        },
        THEN_COMPOSE_TO_COMPLETABLE {
            @Override
            void execute(OrderingFuture<?> future) {
                future.thenComposeToCompletable(x -> nullCompletedFuture());
            }
        },
        THEN_COMPOSE {
            @Override
            void execute(OrderingFuture<?> future) {
                future.thenCompose(x -> OrderingFuture.completedFuture(null));
            }
        },
        GET_NOW {
            @Override
            void execute(OrderingFuture<?> future) {
                try {
                    future.getNow(null);
                } catch (Exception e) {
                    // ignored
                }
            }
        };

        abstract void execute(OrderingFuture<?> future);
    }
}
