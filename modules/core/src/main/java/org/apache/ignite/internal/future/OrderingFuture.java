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

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

/**
 * A little analogue of {@link CompletableFuture} that has the following property: callbacks (like {@link #whenComplete(BiConsumer)}
 * and {@link #thenComposeToCompletable(Function)}) are invoked in the same order in which they were registered.
 *
 * <p>For completion methods ({@link #complete(Object)} and {@link #completeExceptionally(Throwable)} it is guaranteed that,
 * upon returning, the caller will see the completion values (for example, using {@link #getNow(Object)}, UNLESS the
 * completion method is interrupted.
 *
 * <p>Callbacks are invoked asynchronously relative to completion. This means that completer may exit the completion method
 * before the callbacks are invoked.
 *
 * @param <T> Type of payload.
 * @see CompletableFuture
 */
public class OrderingFuture<T> {
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<OrderingFuture, State> STATE = newUpdater(OrderingFuture.class, State.class, "state");

    /**
     * Stores all the state of this future: whether it is completed, normal completion result (if any), cause
     * of exceptional completion (if any), dependents. The State class and all of its components are immutable.
     * We change the state using compare-and-set approach, next state is built from previous one.
     */
    private volatile State<T> state = State.empty();

    /**
     * Used to make sure that at most one thread executes completion code.
     */
    private final AtomicBoolean completionStarted = new AtomicBoolean(false);

    /**
     * Used by {@link #get(long, TimeUnit)} to wait for the moment when completion values are available.
     */
    private final CountDownLatch completionValuesReadyLatch = new CountDownLatch(1);

    /**
     * Creates an incomplete future.
     */
    public OrderingFuture() {
    }

    /**
     * Creates a future that is alredy completed with the given value.
     *
     * @param result Value with which the future is completed.
     * @param <T> Payload type.
     * @return Completed future.
     */
    public static <T> OrderingFuture<T> completedFuture(@Nullable T result) {
        var future = new OrderingFuture<T>();
        future.complete(result);
        return future;
    }

    /**
     * Creates a future that is alredy completed exceptionally (i.e. failed) with the given exception.
     *
     * @param ex Exception with which the future is failed.
     * @param <T> Payload type.
     * @return Failed future.
     */
    public static <T> OrderingFuture<T> failedFuture(Throwable ex) {
        var future = new OrderingFuture<T>();
        future.completeExceptionally(ex);
        return future;
    }

    /**
     * Adapts a {@link CompletableFuture}. That is, creates an {@link OrderingFuture} that gets completed when the
     * original future is completed (and in the same way in which it gets completed).
     *
     * @param adaptee Future to adapt.
     * @param <T> Payload type.
     * @return Adapting future.
     */
    public static <T> OrderingFuture<T> adapt(CompletableFuture<T> adaptee) {
        var future = new OrderingFuture<T>();

        adaptee.whenComplete((res, ex) -> {
            if (ex != null) {
                future.completeExceptionally(ex);
            } else {
                future.complete(res);
            }
        });

        return future;
    }

    /**
     * Completes this future with the given result if it's not completed yet; otherwise has no effect.
     *
     * @param result Completion value (may be {@code null}).
     */
    public void complete(@Nullable T result) {
        completeInternal(result, null);
    }

    /**
     * Completes this future exceptionally with the given exception if it's not completed yet; otherwise has no effect.
     *
     * @param ex Exception.
     */
    public void completeExceptionally(Throwable ex) {
        completeInternal(null, ex);
    }

    private void completeInternal(@Nullable T result, @Nullable Throwable ex) {
        assert ex == null || result == null;

        if (!completionStarted.compareAndSet(false, true)) {
            // Someone has already started the completion. We must leave as the following code can produce duplicate
            // notifications of dependents if executed by more than one thread.

            // But let's wait for completion first as it would be strange if someone calls completion and then manages
            // to see that getNow() returns the fallback value.
            waitForCompletionValuesVisibility();

            return;
        }

        assert state.phase == Phase.INCOMPLETE;

        switchToNotifyingStage(result, ex);

        assert state.phase == Phase.NOTIFYING;

        completionValuesReadyLatch.countDown();

        completeNotificationStage(result, ex);

        assert state.phase == Phase.COMPLETED;
    }

    private void waitForCompletionValuesVisibility() {
        try {
            completionValuesReadyLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void switchToNotifyingStage(@Nullable T result, @Nullable Throwable ex) {
        while (true) {
            State<T> prevState = state;

            // We can only compete with threads adding dependents in this loop, so we don't need to check the phase, it's INCOMPLETE.

            State<T> newState = prevState.switchToNotifying(result, ex);

            if (replaceState(prevState, newState)) {
                break;
            }
        }
    }

    private void completeNotificationStage(@Nullable T result, @Nullable Throwable ex) {
        ListNode<T> lastNotifiedNode = null;

        while (true) {
            State<T> prevState = state;

            State<T> newState = prevState.switchToCompleted();

            // We produce side-effects inside the retry loop, but it's ok as the queue can only grow, the queue
            // state we see is always a prefix of a queue changed by a competitor (we only compete with operations
            // that enqueue elements to the queue as competition with other completers is ruled out with AtomicBoolean)
            // and we track what dependents have already been notified by us.
            notifyDependents(result, ex, prevState.dependentsQueueTail, lastNotifiedNode);
            lastNotifiedNode = prevState.dependentsQueueTail;

            if (replaceState(prevState, newState)) {
                break;
            }
        }
    }

    /**
     * Replaces state with compare-and-set semantics.
     *
     * @param prevState State that we expect to see.
     * @param newState  New state we want to set.
     * @return {@code true} if CAS was successful.
     */
    private boolean replaceState(State<T> prevState, State<T> newState) {
        return STATE.compareAndSet(this, prevState, newState);
    }

    /**
     * Notifies dependents about completion of this future. Does NOT notify notifiedDependents closest to the head of the queue.
     *
     * @param result           Normal completion result.
     * @param ex               Exceptional completion cause.
     * @param dependents       Dependents queue.
     * @param lastNotifiedNode Node that was notified last on preceding iterations of while loop.
     */
    private void notifyDependents(
            @Nullable T result,
            @Nullable Throwable ex,
            @Nullable ListNode<T> dependents,
            ListNode<T> lastNotifiedNode
    ) {
        if (dependents != null) {
            dependents.notifyHeadToTail(result, ex, lastNotifiedNode);
        }
    }

    /**
     * Returns {@code true} if this future is completed exceptionally, {@code false} if completed normally or not completed.
     *
     * @return {@code true} if this future is completed exceptionally, {@code false} if completed normally or not completed
     */
    public boolean isCompletedExceptionally() {
        return state.exception != null;
    }

    /**
     * Adds a callback that gets executed as soon as this future gets completed for any reason. The action will get both result
     * and exception; if the completion is normal, exception will be {@code null}, otherwise result will be {@code null}.
     * If it's already complete, the action is executed immediately.
     * Any exception produced by the action is swallowed.
     *
     * @param action Action to execute.
     */
    public void whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        WhenComplete<T> dependent = null;

        while (true) {
            State<T> prevState = state;

            if (prevState.completionQueueProcessed()) {
                acceptQuietly(action, prevState.result, prevState.exception);
                return;
            }

            if (dependent == null) {
                dependent = new WhenComplete<>(action);
            }
            State<T> newState = prevState.enqueueDependent(dependent);

            if (replaceState(prevState, newState)) {
                return;
            }
        }
    }

    private static <T> void acceptQuietly(BiConsumer<? super T, ? super Throwable> action, T result, Throwable ex) {
        try {
            action.accept(result, ex);
        } catch (Exception e) {
            // ignore
        }
    }

    /**
     * Creates a composition of this future with a function producing a {@link CompletableFuture}.
     *
     * @param mapper Mapper used to produce a {@link CompletableFuture} from this future result.
     * @param <U> Result future payload type.
     * @return Composition.
     * @see CompletableFuture#thenCompose(Function)
     */
    public <U> CompletableFuture<U> thenComposeToCompletable(Function<? super T, ? extends CompletableFuture<U>> mapper) {
        ThenComposeToCompletable<T, U> dependent = null;

        while (true) {
            State<T> prevState = state;

            if (prevState.completionQueueProcessed()) {
                if (prevState.exception != null) {
                    return CompletableFuture.failedFuture(wrapWithCompletionException(prevState.exception));
                } else {
                    return applyMapper(mapper, prevState.result);
                }
            }

            if (dependent == null) {
                dependent = new ThenComposeToCompletable<>(new CompletableFuture<>(), mapper);
            }
            State<T> newState = prevState.enqueueDependent(dependent);

            if (replaceState(prevState, newState)) {
                return dependent.resultFuture;
            }
        }
    }

    private static CompletionException wrapWithCompletionException(Throwable ex) {
        return ex instanceof CompletionException ? (CompletionException) ex : new CompletionException(ex);
    }

    private static <T, U> CompletableFuture<U> applyMapper(Function<? super T, ? extends CompletableFuture<U>> mapper, T result) {
        try {
            return mapper.apply(result);
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Returns the completion value, (if the future is completed normally), throws completion cause wrapped in
     * {@link CompletionException} (if the future is completed exceptionally), or returns the provided default value
     * if the future is not completed yet.
     *
     * @param valueIfAbsent Value to return if the future is not completed yet.
     * @return Completion value or default value.
     * @see CompletableFuture#getNow(Object)
     */
    public T getNow(T valueIfAbsent) {
        State<T> currentState = state;

        if (currentState.completionValuesAvailable()) {
            if (currentState.exception != null) {
                throw wrapWithCompletionException(currentState.exception);
            } else {
                return currentState.result;
            }
        } else {
            return valueIfAbsent;
        }
    }

    /**
     * Returns completion value or throws completion exception (wrapped in {@link ExecutionException}), waiting for
     * completion up to the specified amount of time, if not completed yet. If the time runs out while waiting,
     * throws {@link TimeoutException}.
     *
     * @param timeout Maximum amount of time to wait.
     * @param unit    Unit of time in which the timeout is given.
     * @return Completion value.
     * @throws InterruptedException Thrown if the current thread gets interrupted while waiting for completion.
     * @throws TimeoutException Thrown if the wait for completion times out.
     * @throws ExecutionException Thrown (with the original exception as a cause) if the future completes exceptionally.
     * @see CompletableFuture#get(long, TimeUnit)
     */
    public T get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException {
        boolean completedInTime = completionValuesReadyLatch.await(timeout, unit);
        if (!completedInTime) {
            throw new TimeoutException();
        }

        State<T> currentState = state;

        if (currentState.exception instanceof CancellationException) {
            throw (CancellationException) currentState.exception;
        } else if (currentState.exception != null) {
            throw exceptionForThrowingFromGet(currentState);
        } else {
            return currentState.result;
        }
    }

    private ExecutionException exceptionForThrowingFromGet(State<T> currentState) {
        Throwable unwrapped = currentState.exception;
        Throwable cause = unwrapped.getCause();
        if (cause != null) {
            unwrapped = cause;
        }

        return new ExecutionException(unwrapped);
    }

    /**
     * Returns a {@link CompletableFuture} that gets completed when this future gets completed (and in the same way).
     * The returned future does not provide any ordering guarantees that this future provides.
     *
     * @return An equivalent {@link CompletableFuture}.
     */
    public CompletableFuture<T> toCompletableFuture() {
        CompletableFuture<T> completableFuture = new CompletableFuture<>();

        this.whenComplete((res, ex) -> completeCompletableFuture(completableFuture, res, ex));

        return completableFuture;
    }

    private static <T> void completeCompletableFuture(CompletableFuture<T> future, T result, Throwable ex) {
        if (ex != null) {
            future.completeExceptionally(ex);
        } else {
            future.complete(result);
        }
    }

    /**
     * Dependent action that gets notified when this future is completed.
     *
     * @param <T> Payload type.
     */
    private interface DependentAction<T> {
        /**
         * Informs that dependent that the host future is completed.
         *
         * @param result Normal completion result ({@code null} if completed exceptionally, but might be {@code null} for normal completion.
         * @param ex     Exceptional completion cause ({@code null} if completed normally).
         * @param context Notification context used to cache CompletionException, if needed.
         */
        void onCompletion(T result, Throwable ex, NotificationContext context);
    }

    private static class WhenComplete<T> implements DependentAction<T> {
        private final BiConsumer<? super T, ? super Throwable> action;

        private WhenComplete(BiConsumer<? super T, ? super Throwable> action) {
            this.action = action;
        }

        @Override
        public void onCompletion(T result, Throwable ex, NotificationContext context) {
            acceptQuietly(action, result, ex);
        }
    }

    private static class ThenComposeToCompletable<T, U> implements DependentAction<T>, BiConsumer<U, Throwable> {
        private final CompletableFuture<U> resultFuture;
        private final Function<? super T, ? extends CompletableFuture<U>> mapper;

        private ThenComposeToCompletable(CompletableFuture<U> resultFuture, Function<? super T, ? extends CompletableFuture<U>> mapper) {
            this.resultFuture = resultFuture;
            this.mapper = mapper;
        }

        @Override
        public void onCompletion(T result, Throwable ex, NotificationContext context) {
            if (ex != null) {
                resultFuture.completeExceptionally(context.completionExceptionCaching(ex));
                return;
            }

            try {
                CompletableFuture<U> mapResult = mapper.apply(result);

                // Reusing this object as a BiConsumer instead of writing lambda to spare one allocation (might be
                // important if there is a huge amount of dependents).
                mapResult.whenComplete(this);
            } catch (Throwable e) {
                resultFuture.completeExceptionally(e);
            }
        }

        @Override
        public void accept(U mapRes, Throwable mapEx) {
            completeCompletableFuture(resultFuture, mapRes, mapEx);
        }
    }

    private static class State<T> {
        private static final State<?> INCOMPLETE_STATE = new State<>(Phase.INCOMPLETE, null, null, null);

        private final Phase phase;
        private final T result;
        private final Throwable exception;
        private final ListNode<T> dependentsQueueTail;

        private State(Phase phase, T result, Throwable exception, ListNode<T> dependentsQueueTail) {
            this.phase = phase;
            this.result = result;
            this.exception = exception;
            this.dependentsQueueTail = dependentsQueueTail;
        }

        @SuppressWarnings("unchecked")
        private static <T> State<T> empty() {
            return (State<T>) INCOMPLETE_STATE;
        }

        public boolean completionValuesAvailable() {
            return phase != Phase.INCOMPLETE;
        }

        public boolean completionQueueProcessed() {
            return phase == Phase.COMPLETED;
        }

        public State<T> switchToNotifying(T completionResult, Throwable completionCause) {
            return new State<>(Phase.NOTIFYING, completionResult, completionCause, dependentsQueueTail);
        }

        public State<T> switchToCompleted() {
            return new State<>(Phase.COMPLETED, result, exception, null);
        }

        public State<T> enqueueDependent(DependentAction<T> dependent) {
            return new State<>(phase, result, exception, new ListNode<>(dependent, dependentsQueueTail));
        }
    }

    private static class ListNode<T> {
        private final DependentAction<T> dependent;
        private final ListNode<T> prev;

        private ListNode(DependentAction<T> dependent, ListNode<T> prev) {
            this.dependent = dependent;
            this.prev = prev;
        }

        public void notifyHeadToTail(T result, Throwable exception, ListNode<T> lastNotifiedNode) {
            Deque<ListNode<T>> stack = new ArrayDeque<>();

            for (ListNode<T> node = this; node != null && node != lastNotifiedNode; node = node.prev) {
                stack.addFirst(node);
            }

            NotificationContext context = new NotificationContext();

            // Notify those dependents that are not notified yet.
            while (!stack.isEmpty()) {
                ListNode<T> node = stack.removeFirst();

                try {
                    node.dependent.onCompletion(result, exception, context);
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    private enum Phase {
        /**
         * Future is not complete, completion values are not known, callbacks are not invoked. Callbacks added in this phase
         * are enqueued for a later invocation.
         */
        INCOMPLETE,
        /**
         * Future is half-complete (completion values are available for the outside world), but callbacks are not yet invoked
         * (but they are probably being invoked right now). Callbacks added in this phase are enqueued for a later invocation.
         */
        NOTIFYING,
        /**
         * Future is fully completed: completion values are available, callbacks are invoked. In this phase, new callbacks
         * are invoked immediately instead of being enqueued.
         */
        COMPLETED
    }

    private static class NotificationContext {
        private CompletionException completionException;

        CompletionException completionExceptionCaching(Throwable cause) {
            if (completionException == null) {
                completionException = wrapWithCompletionException(cause);
            }

            return completionException;
        }
    }
}
