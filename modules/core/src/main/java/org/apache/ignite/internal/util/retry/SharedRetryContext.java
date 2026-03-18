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

package org.apache.ignite.internal.util.retry;

import static java.util.Optional.ofNullable;
import static org.apache.ignite.internal.util.retry.TimeoutState.attempt;
import static org.apache.ignite.internal.util.retry.TimeoutState.timeout;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A retry context that tracks a single shared timeout state across all callers.
 *
 * <p>Unlike {@link KeyBasedRetryContext}, this context does not distinguish between
 * retry targets — all callers advance and observe the same {@link TimeoutState}.
 * This is appropriate when a single backoff sequence should govern all retries
 * regardless of which operation is being retried.
 *
 * <p>The state is initialized lazily on the first call to {@link #updateAndGetState()},
 * and can be reset to {@code null} via {@link #resetState()}, allowing the progression
 * to restart from scratch. {@link #getState()} returns an empty {@link Optional} before
 * the first call and after a reset.
 *
 * <p>Concurrent calls to {@link #updateAndGetState()} and {@link #resetState()} are safe.
 * The {@link AtomicReference} handles structural transitions ({@code null ↔ initialized}),
 * while the {@link TimeoutState}'s internal {@link AtomicLong} CAS handles concurrent
 * value updates without allocating new objects on the hot path.
 *
 * <p>This class is thread-safe.
 */
public class SharedRetryContext {
    /** Timeout used to initialize the state on the first call to {@link #updateAndGetState()}. */
    private final int initialTimeout;

    /** Strategy used to compute the next timeout from the current one on each advancement. */
    private final TimeoutStrategy timeoutStrategy;

    /**
     * Holds the single shared retry state. {@code null} before the first update and after reset.
     * The {@link TimeoutState} object itself is reused across updates to avoid allocation pressure.
     */
    private final AtomicReference<TimeoutState> timeoutState = new AtomicReference<>();

    /**
     * Creates a new context with the given initial timeout and strategy.
     *
     * @param initialTimeout timeout returned on the very first call to {@link #updateAndGetState()},
     *                       in milliseconds.
     * @param timeoutStrategy strategy used to compute subsequent timeout values.
     */
    public SharedRetryContext(int initialTimeout, TimeoutStrategy timeoutStrategy) {
        this.initialTimeout = initialTimeout;
        this.timeoutStrategy = timeoutStrategy;
    }

    /**
     * Returns the current shared {@link TimeoutState}, if initialized.
     *
     * <p>Returns an empty {@link Optional} if {@link #updateAndGetState()} has never been called,
     * or if {@link #resetState()} was called after the last update.
     *
     * @return current state, or empty if not yet initialized or already reset.
     */
    public Optional<TimeoutState> getState() {
        return ofNullable(timeoutState.get());
    }

    /**
     * Atomically advances the shared retry state and returns it.
     *
     * <p>On the first call (or after a reset), lazily initializes the {@link TimeoutState}
     * with {@link #initialTimeout} and returns it with attempt count {@code 1} and the
     * initial timeout. On subsequent calls, advances the timeout using
     * {@link TimeoutStrategy#next(int)} and increments the attempt count.
     *
     * <p>Initialization uses {@link AtomicReference#compareAndSet} to handle concurrent
     * first calls safely — only one thread creates the state object. Advancement uses
     * a CAS loop on the {@link TimeoutState}'s internal {@link AtomicLong}, so no new
     * objects are allocated on the hot path.
     *
     * <p>If {@link #resetState()} races with this method between initialization and
     * advancement, the outer loop retries transparently.
     *
     * @return the shared {@link TimeoutState} after advancement.
     */
    public TimeoutState updateAndGetState() {
        while (true) {
            if (timeoutState.get() == null) {
                timeoutState.compareAndSet(null, new TimeoutState(initialTimeout, 0));
            }

            TimeoutState state = timeoutState.get();
            if (state == null) {
                continue; // reset raced us, retry
            }

            long raw;
            int nextTimeout;
            do {
                raw = state.getRawState();
                nextTimeout = attempt(raw) == 0
                        ? initialTimeout
                        : timeoutStrategy.next(timeout(raw));
            } while (!state.update(raw, nextTimeout, attempt(raw) + 1));

            return state;
        }
    }

    /**
     * Resets the shared state to {@code null}, as if no retries had ever occurred.
     *
     * <p>The next call to {@link #updateAndGetState()} after a reset will re-initialize
     * the state starting from {@link #initialTimeout} with attempt count {@code 1}.
     */
    public void resetState() {
        timeoutState.set(null);
    }
}
