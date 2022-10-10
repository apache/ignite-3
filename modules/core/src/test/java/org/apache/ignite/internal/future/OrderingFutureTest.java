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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * General tests for {@link OrderingFuture}.
 */
class OrderingFutureTest {
    private final RuntimeException cause = new RuntimeException("Oops");

    @Test
    void completedFutureCreatesCompletedFuture() {
        OrderingFuture<Integer> future = OrderingFuture.completedFuture(1);

        assertThat(future.getNow(999), is(1));
    }

    @Test
    void completedFutureIsNotFailed() {
        OrderingFuture<Integer> future = OrderingFuture.completedFuture(1);

        assertFalse(future.isCompletedExceptionally());
    }

    @Test
    void failedFutureCreatesFutureCompletedExceptionally() {
        OrderingFuture<Integer> future = OrderingFuture.failedFuture(cause);

        assertThatFutureIsCompletedWithOurException(future);
    }

    private void assertThatFutureIsCompletedWithOurException(OrderingFuture<Integer> future) {
        CompletionException ex = assertThrows(CompletionException.class, () -> future.getNow(999));

        assertThat(ex.getCause(), is(sameInstance(cause)));
    }

    private void assertThatFutureIsCompletedWithOurException(CompletableFuture<Integer> future) {
        CompletionException ex = assertThrows(CompletionException.class, () -> future.getNow(999));

        assertThat(ex.getCause(), is(sameInstance(cause)));
    }

    @Test
    void failedFutureIsCompletedExceptionally() {
        OrderingFuture<Integer> future = OrderingFuture.failedFuture(cause);

        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    void adaptingIncompleteFutureProducesIncompleteResult() {
        CompletableFuture<Integer> adaptee = new CompletableFuture<>();
        OrderingFuture<Integer> adaptor = OrderingFuture.adapt(adaptee);

        assertThat(adaptor.getNow(999), is(999));
    }

    @Test
    void normalCompletionIsPropagatedThrowAdapt() {
        CompletableFuture<Integer> adaptee = new CompletableFuture<>();
        OrderingFuture<Integer> adaptor = OrderingFuture.adapt(adaptee);

        adaptee.complete(1);

        assertThat(adaptor.getNow(999), is(1));
    }

    @Test
    void exceptionalCompletionIsPropagatedThroughAdapter() {
        CompletableFuture<Integer> adaptee = new CompletableFuture<>();
        OrderingFuture<Integer> adaptor = OrderingFuture.adapt(adaptee);

        adaptee.completeExceptionally(cause);

        assertThatFutureIsCompletedWithOurException(adaptor);
    }

    @Test
    void completeCompletesIncompleteFuture() {
        var future = new OrderingFuture<Integer>();

        future.complete(1);

        assertThat(future.getNow(999), is(1));
    }

    @Test
    void completeDoesNothingWithCompletedFuture() {
        OrderingFuture<Integer> future = OrderingFuture.completedFuture(1);

        future.complete(2);

        assertThat(future.getNow(999), is(1));
    }

    @Test
    void completeDoesDoesNotInvokeCallbacksSecondTimeOnCompletedFuture() {
        OrderingFuture<Integer> future = OrderingFuture.completedFuture(1);

        AtomicInteger completionCount = new AtomicInteger();

        future.whenComplete((res, ex) -> completionCount.incrementAndGet());

        future.complete(2);

        assertThat(completionCount.get(), is(1));
    }

    @Test
    void completeDoesNothingWithFailedFuture() {
        OrderingFuture<Integer> future = OrderingFuture.failedFuture(cause);

        future.complete(2);

        assertThatFutureIsCompletedWithOurException(future);
    }

    @Test
    void completeExceptionallyDoesDoesNotInvokeCallbacksSecondTimeOnCompletedFuture() {
        OrderingFuture<Integer> future = OrderingFuture.completedFuture(1);

        AtomicInteger completionCount = new AtomicInteger();

        future.whenComplete((res, ex) -> completionCount.incrementAndGet());

        future.completeExceptionally(cause);

        assertThat(completionCount.get(), is(1));
    }

    @Test
    void completeExceptionallyCompletesIncompleteFuture() {
        var future = new OrderingFuture<Integer>();

        future.completeExceptionally(cause);

        assertThatFutureIsCompletedWithOurException(future);
    }

    @Test
    void completeExceptionallyDoesNothingWithCompletedFuture() {
        OrderingFuture<Integer> future = OrderingFuture.completedFuture(1);

        future.completeExceptionally(cause);

        assertThat(future.getNow(999), is(1));
    }

    @Test
    void completeExceptionallyDoesNothingWithFailedFuture() {
        OrderingFuture<Integer> future = OrderingFuture.failedFuture(cause);

        future.completeExceptionally(new Exception("Another cause"));

        assertThatFutureIsCompletedWithOurException(future);
    }

    @Test
    void completionWithCompletionExceptionDoesNotDuplicateCompletionException() {
        OrderingFuture<Integer> future = new OrderingFuture<>();

        future.completeExceptionally(new CompletionException(cause));

        assertThatFutureIsCompletedWithOurException(future);
    }

    @Test
    void whenCompletePropagatesResultFromAlreadyCompletedFuture() {
        AtomicInteger container = new AtomicInteger();
        OrderingFuture<Integer> future = OrderingFuture.completedFuture(1);

        future.whenComplete((res, ex) -> container.set(res));

        assertThat(container.get(), is(1));
    }

    @Test
    void whenCompletePropagatesExceptionFromAlreadyFailedFuture() {
        AtomicReference<Throwable> container = new AtomicReference<>();
        OrderingFuture<Integer> future = OrderingFuture.failedFuture(cause);

        future.whenComplete((res, ex) -> container.set(ex));

        assertThat(container.get(), is(sameInstance(cause)));
    }

    @Test
    void whenCompletePropagatesResultFromFutureCompletion() {
        AtomicInteger container = new AtomicInteger();
        OrderingFuture<Integer> future = new OrderingFuture<>();

        future.whenComplete((res, ex) -> container.set(res));

        future.complete(1);

        assertThat(container.get(), is(1));
    }

    @Test
    void whenCompletePropagatesExceptionFromFutureCompletion() {
        AtomicReference<Throwable> container = new AtomicReference<>();
        OrderingFuture<Integer> future = new OrderingFuture<>();

        future.whenComplete((res, ex) -> container.set(ex));

        future.completeExceptionally(cause);

        assertThat(container.get(), is(sameInstance(cause)));
    }

    @Test
    void whenCompleteSwallowsExceptionThrownByActionOnAlreadyCompletedFuture() {
        OrderingFuture<Integer> future = OrderingFuture.completedFuture(1);

        assertDoesNotThrow(() -> future.whenComplete((res, ex) -> {
            throw cause;
        }));
    }

    @Test
    void whenCompleteSwallowsExceptionThrownByActionOnAlreadyFailedFuture() {
        OrderingFuture<Integer> future = OrderingFuture.failedFuture(cause);

        assertDoesNotThrow(() -> future.whenComplete((res, ex) -> {
            throw new RuntimeException("Another exception");
        }));
    }

    @Test
    void composeToCompletablePropagatesResultFromAlreadyCompletedFuture() {
        OrderingFuture<Integer> orderingFuture = OrderingFuture.completedFuture(3);

        CompletableFuture<Integer> completableFuture = orderingFuture.thenComposeToCompletable(
                x -> CompletableFuture.completedFuture(x * 5)
        );

        assertThat(completableFuture.getNow(999), is(15));
    }

    @Test
    void composeToCompletablePropagatesExceptionFromAlreadyFailedFuture() {
        OrderingFuture<Integer> orderingFuture = OrderingFuture.failedFuture(cause);

        CompletableFuture<Integer> completableFuture = orderingFuture.thenComposeToCompletable(CompletableFuture::completedFuture);

        assertThatFutureIsCompletedWithOurException(completableFuture);
    }

    @Test
    void composeToCompletableDoesNotInvokeActionOnAlreadyFailedFuture() {
        AtomicBoolean called = new AtomicBoolean(false);
        OrderingFuture<Integer> orderingFuture = OrderingFuture.failedFuture(cause);

        CompletableFuture<Integer> completableFuture = orderingFuture.thenComposeToCompletable(value -> {
            called.set(true);
            return CompletableFuture.completedFuture(value);
        });

        assertFalse(called.get());
        assertThatFutureIsCompletedWithOurException(completableFuture);
    }

    @Test
    void composeToCompletablePropagatesResultFromFutureCompletion() {
        OrderingFuture<Integer> orderingFuture = new OrderingFuture<>();

        CompletableFuture<Integer> completableFuture = orderingFuture.thenComposeToCompletable(
                x -> CompletableFuture.completedFuture(x * 5)
        );

        orderingFuture.complete(3);

        assertThat(completableFuture.getNow(999), is(15));
    }

    @Test
    void composeToCompletablePropagatesExceptionFromFutureCompletion() {
        OrderingFuture<Integer> orderingFuture = new OrderingFuture<>();

        CompletableFuture<Integer> completableFuture = orderingFuture.thenComposeToCompletable(CompletableFuture::completedFuture);

        orderingFuture.completeExceptionally(cause);

        assertThatFutureIsCompletedWithOurException(completableFuture);
    }

    @Test
    void composeToCompletableDoesNotInvokeActionOnExceptionalCompletion() {
        AtomicBoolean called = new AtomicBoolean(false);
        OrderingFuture<Integer> orderingFuture = new OrderingFuture<>();

        CompletableFuture<Integer> completableFuture = orderingFuture.thenComposeToCompletable(value -> {
            called.set(true);
            return CompletableFuture.completedFuture(value);
        });
        orderingFuture.completeExceptionally(cause);

        assertFalse(called.get());
        assertThatFutureIsCompletedWithOurException(completableFuture);
    }

    @Test
    void composeToCompletablePropagatesExceptionFromActionOnCompletedFuture() {
        OrderingFuture<Integer> orderingFuture = OrderingFuture.completedFuture(1);

        CompletableFuture<Integer> completableFuture = orderingFuture.thenComposeToCompletable(x -> {
            throw cause;
        });

        assertThatFutureIsCompletedWithOurException(completableFuture);
    }

    @Test
    void composeToCompletablePropagatesExceptionFromActionOnNormalCompletion() {
        OrderingFuture<Integer> orderingFuture = new OrderingFuture<>();

        CompletableFuture<Integer> completableFuture = orderingFuture.thenComposeToCompletable(x -> {
            throw cause;
        });

        orderingFuture.complete(1);

        assertThatFutureIsCompletedWithOurException(completableFuture);
    }

    @Test
    void getNowReturnsCompletionValueFromCompletedFuture() {
        OrderingFuture<Integer> future = OrderingFuture.completedFuture(1);

        assertThat(future.getNow(999), is(1));
    }

    @Test
    void getNowReturnsCompletionValueFromFutureCompletedManually() {
        OrderingFuture<Integer> future = new OrderingFuture<>();
        future.complete(1);

        assertThat(future.getNow(999), is(1));
    }

    @Test
    void getNowReturnsDefaultValueFromIncompleteFuture() {
        OrderingFuture<Integer> incompleteFuture = new OrderingFuture<>();

        assertThat(incompleteFuture.getNow(999), is(999));
    }

    @Test
    void getNowThrowsOnFailedFuture() {
        OrderingFuture<Integer> future = OrderingFuture.failedFuture(cause);

        CompletionException ex = assertThrows(CompletionException.class, () -> future.getNow(999));
        assertThat(ex.getCause(), is(sameInstance(cause)));
    }

    @Test
    void getNowThrowsOnFutureCompletedExceptionally() {
        OrderingFuture<Integer> future = new OrderingFuture<>();
        future.completeExceptionally(cause);

        CompletionException ex = assertThrows(CompletionException.class, () -> future.getNow(999));
        assertThat(ex.getCause(), is(sameInstance(cause)));
    }

    @Test
    void getWithTimeoutReturnsCompletionValueFromCompletedFuture() throws Exception {
        OrderingFuture<Integer> future = OrderingFuture.completedFuture(1);

        assertThat(future.get(1, TimeUnit.NANOSECONDS), is(1));
    }

    @Test
    void getWithTimeoutReturnsCompletionValueFromFutureCompletedManually() throws Exception {
        OrderingFuture<Integer> future = new OrderingFuture<>();
        future.complete(1);

        assertThat(future.get(1, TimeUnit.NANOSECONDS), is(1));
    }

    @Test
    void getWithTimeoutReturnsCompletionValueFromFutureCompletedFromDifferentThread() throws Exception {
        OrderingFuture<Integer> future = new OrderingFuture<>();

        new Thread(() -> future.complete(1)).start();

        assertThat(future.get(1, TimeUnit.SECONDS), is(1));
    }

    @Test
    void getWithTimeoutThrowsOnFailedFuture() {
        OrderingFuture<Integer> future = OrderingFuture.failedFuture(cause);

        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.NANOSECONDS));
        assertThat(ex.getCause(), is(sameInstance(cause)));
    }

    @Test
    void getWithTimeoutThrowsOnFutureCompletedExceptionally() {
        OrderingFuture<Integer> future = new OrderingFuture<>();

        future.completeExceptionally(cause);

        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.NANOSECONDS));
        assertThat(ex.getCause(), is(sameInstance(cause)));
    }

    @Test
    void getWithTimeoutThrowsOnFutureCompletedExceptionallyFromDifferentThread() {
        OrderingFuture<Integer> future = new OrderingFuture<>();

        new Thread(() -> future.completeExceptionally(cause)).start();

        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
        assertThat(ex.getCause(), is(sameInstance(cause)));
    }

    @Test
    void getWithTimeoutUnwrapsCompletionExceptionWhenThrowsExecutionException() {
        OrderingFuture<Void> future = OrderingFuture.failedFuture(new CompletionException(cause));

        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
        assertThat(ex.getCause(), is(sameInstance(cause)));
    }

    @Test
    void getWithTimeoutThrowsTimeoutExceptionWhenTimesOut() {
        OrderingFuture<Integer> incompleteFuture = new OrderingFuture<>();

        assertThrows(TimeoutException.class, () -> incompleteFuture.get(1, TimeUnit.NANOSECONDS));
    }

    @Test
    void getWithTimeoutThrowsTimeoutExceptionWhenTimeoutIsZero() {
        OrderingFuture<Integer> incompleteFuture = new OrderingFuture<>();

        assertThrows(TimeoutException.class, () -> incompleteFuture.get(0, TimeUnit.NANOSECONDS));
    }

    @Test
    void getWithTimeoutThrowsInterruptedExceptionOnInterruption() throws Exception {
        OrderingFuture<Integer> incompleteFuture = new OrderingFuture<>();

        CountDownLatch workerStartReached = new CountDownLatch(1);
        CountDownLatch workerEndReached = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean(false);
        AtomicReference<Throwable> exRef = new AtomicReference<>();

        Thread worker = new Thread(() -> {
            workerStartReached.countDown();

            try {
                incompleteFuture.get(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                interrupted.set(true);
            } catch (TimeoutException | ExecutionException e) {
                exRef.set(e);
            } finally {
                workerEndReached.countDown();
            }
        });
        worker.start();

        assertTrue(workerStartReached.await(1, TimeUnit.SECONDS));

        worker.interrupt();

        assertTrue(workerEndReached.await(1, TimeUnit.SECONDS));

        assertThat(interrupted.get(), is(true));
        assertThat(exRef.get(), is(nullValue()));
    }

    @Test
    void getWithTimeoutThrowsInterruptedExceptionIfThreadIsAlreadyInterruptedEvenWithZeroTimeout() {
        OrderingFuture<Integer> incompleteFuture = new OrderingFuture<>();

        Thread.currentThread().interrupt();

        assertThrows(InterruptedException.class, () -> incompleteFuture.get(0, TimeUnit.NANOSECONDS));
    }

    @Test
    void conversionOfIncompleteFutureToCompletableFutureProducesIncompleteResult() {
        OrderingFuture<Integer> orderingFuture = new OrderingFuture<>();

        CompletableFuture<Integer> completableFuture = orderingFuture.toCompletableFuture();

        assertThat(completableFuture.getNow(999), is(999));
    }

    @Test
    void normalCompletionIsPropagatedToCompletableFuture() {
        OrderingFuture<Integer> orderingFuture = new OrderingFuture<>();

        CompletableFuture<Integer> completableFuture = orderingFuture.toCompletableFuture();

        orderingFuture.complete(1);

        assertThat(completableFuture.getNow(999), is(1));
    }

    @Test
    void exceptionalCompletionIsPropagatedToCompletableFuture() {
        OrderingFuture<Integer> orderingFuture = new OrderingFuture<>();

        CompletableFuture<Integer> completableFuture = orderingFuture.toCompletableFuture();

        orderingFuture.completeExceptionally(cause);

        assertThatFutureIsCompletedWithOurException(completableFuture);
    }
}
