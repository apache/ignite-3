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

package org.apache.ignite.internal.manager;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.NodeStoppingException;

public class ComponentInflights {
    private enum State {
        CREATED, STARTING, RUNNING, STOPPING, STOPPED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);

    private final AtomicInteger inflights = new AtomicInteger(0);

    private final CompletableFuture<Void> stopFut = new CompletableFuture<>();

    public <T> CompletableFuture<T> startFutureAsync(Supplier<CompletableFuture<T>> function) {
        if (state.compareAndSet(State.CREATED, State.STARTING)) {
            return function.get().whenComplete((t, throwable) -> state.compareAndSet(State.STARTING, State.RUNNING));
        } else {
            State componentState = state.get();

            switch (componentState) {
                case STOPPED:
                case STOPPING:
                    return failedFuture(new NodeStoppingException("Component has already been stopped."));
                case STARTING:
                case RUNNING:
                    return nullCompletedFuture();
                default:
                    return failedFuture(new NodeStoppingException("Unexpected state: " + componentState));
            }
        }
    }

    public <T> CompletableFuture<T> register(Supplier<CompletableFuture<T>> function) {
        // First check if we are in CREATED or STARTING state.
        if (isNotStarted()) {
            // TODO: Alternatively we can wait for the start to complete and then execute.
            return failedFuture(new IllegalStateException("Component has not been started."));
        }

        // Increment inflight. Protects us from concurrency issues on stop().

        startInflight();

        // stop() checks inflights only after changing the state to STOPPING.
        // And here we check the state after incrementing the inflight counter.
        // If isRunning() is true, then stop() has not been called yet and it will wait for this inflight to finish.
        // If on the other hand, isRunning() is false, then stop() has already been called and we need to abort the operation.
        if (!isRunning()) {
            stopInflight();

            return failedFuture(new NodeStoppingException("Component has already been stopped."));
        }

        CompletableFuture<T> future = function.get();

        // A shortcut to avoid creating a new future.
        if (future.isDone()) {
            stopInflight();
        } else {
            future.whenComplete((t, throwable) -> stopInflight());
        }

        return future;
    }

    private void startInflight() {
        inflights.incrementAndGet();
    }

    private void stopInflight() {
        if (inflights.decrementAndGet() == 0 && state.get() == State.STOPPING) {
            stopFut.complete(null);
        }
    }

    private CompletableFuture<Void> waitNoInflights() {
        if (inflights.get() == 0) {
            stopFut.complete(null);
        }
        return stopFut;
    }

    public <T> CompletableFuture<T> stopFutureAsync(Supplier<CompletableFuture<T>> function) {
        // TODO: We need to wait for start() to complete and only then stop.
        if (state.compareAndSet(State.CREATED, State.STOPPING) || state.compareAndSet(State.STARTING, State.STOPPING)) {
            return function.get().whenComplete((t, throwable) -> state.compareAndSet(State.STOPPING, State.STOPPED));
        } else if (state.compareAndSet(State.RUNNING, State.STOPPING)) {
            return waitNoInflights()
                    .thenCompose(unused -> function.get())
                    .whenComplete((t, throwable) -> state.compareAndSet(State.STOPPING, State.STOPPED));
        } else {
            // The state is either STOPPING or STOPPED.
            return nullCompletedFuture();
        }
    }

    public boolean isStopped() {
        State componentState = state.get();

        return State.STOPPING == componentState || State.STOPPED == componentState;
    }

    public boolean isRunning() {
        return state.get() == State.RUNNING;
    }

    public boolean isNotStarted() {
        State componentState = state.get();

        return State.CREATED == componentState || State.STARTING == componentState;
    }
}
