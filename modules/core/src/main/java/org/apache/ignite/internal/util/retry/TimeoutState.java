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

import static org.apache.ignite.internal.util.retry.TimeoutStrategy.DEFAULT_RETRY_INITIAL_TIMEOUT_MS;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Mutable holder for retry timeout and attempt count.
 *
 * <p>Both fields are packed into a single {@link AtomicLong} to allow a consistent
 * atomic read of the combined state via a single {@code get()}. The high 32 bits store
 * the timeout in milliseconds; the low 32 bits store the attempt count.
 *
 * <p>This class intentionally does not override {@link Object#equals(Object)} or
 * {@link Object#hashCode()}. Because the state is mutable, value-based equality
 * would break the contracts required by {@link java.util.HashMap} and similar
 * collections. Reference equality (the {@link Object} default) is correct here.
 *
 * <p>The static helper methods {@link #timeout(long)} and {@link #attempt(long)}
 * are package-private to allow callers that hold a raw snapshot to extract fields
 * without additional reads.
 */
public class TimeoutState {
    /**
     * Packed representation of timeout and attempt count.
     * High 32 bits: timeout (ms). Low 32 bits: attempt count.
     */
    private final AtomicLong state = new AtomicLong();

    /**
     * Creates a new {@code TimeoutState} with the default initial timeout and attempt count of {@code 0}.
     *
     * <p>Attempt count {@code 0} acts as a sentinel indicating the state has been initialized
     * but not yet advanced. The first call to {@link #update(TimeoutStrategy)} will set the
     * timeout to {@link TimeoutStrategy#DEFAULT_RETRY_INITIAL_TIMEOUT_MS} and increment the count to {@code 1}.
     */
    public TimeoutState() {
        this(DEFAULT_RETRY_INITIAL_TIMEOUT_MS, 0);
    }

    /**
     * Creates a new {@code TimeoutState} with the given initial timeout and attempt count.
     *
     * @param timeout initial timeout in milliseconds.
     * @param attempt attempt count. Use {@code 0} as a sentinel to indicate
     *                "initialized but not yet advanced" when lazy initialization is needed.
     */
    public TimeoutState(int timeout, int attempt) {
        state.set(pack(timeout, attempt));
    }

    /**
     * Returns the current retry timeout in milliseconds.
     *
     * <p>This is a single atomic read.
     *
     * @return current timeout in milliseconds.
     */
    public int getTimeout() {
        return timeout(state.get());
    }

    /**
     * Returns the current attempt count.
     *
     * <p>This is a single atomic read.
     *
     * @return current attempt count.
     */
    public int getAttempt() {
        return attempt(state.get());
    }

    /**
     * Advances the retry state using the given strategy.
     *
     * <p>If the current attempt count is {@code 0} (the initial sentinel), the timeout is reset
     * to {@link TimeoutStrategy#DEFAULT_RETRY_INITIAL_TIMEOUT_MS} and the attempt count is set to {@code 1}.
     * On subsequent calls, the timeout is computed by {@link TimeoutStrategy#next(int)} and the
     * attempt count is incremented.
     *
     * <p>This method is package-private because callers are responsible for external synchronization.
     * The only intended call site is inside {@link java.util.concurrent.ConcurrentHashMap#compute} in
     * {@link KeyBasedRetryContext#updateAndGetState}, which holds an exclusive per-key lock for the
     * duration of the lambda, so no concurrent access to the same instance is possible.
     *
     * @param timeoutStrategy strategy used to compute the next timeout value.
     */
    void update(TimeoutStrategy timeoutStrategy) {
        long raw = state.get();

        int nextTimeout = attempt(raw) == 0
                ? DEFAULT_RETRY_INITIAL_TIMEOUT_MS
                : timeoutStrategy.next(timeout(raw));

        state.set(pack(nextTimeout, attempt(raw) + 1));
    }

    /**
     * Packs timeout and attempt count into a single {@code long}.
     * Timeout occupies the high 32 bits; attempt occupies the low 32 bits.
     *
     * @param timeout timeout in milliseconds.
     * @param attempt attempt count.
     * @return packed {@code long} value.
     */
    static long pack(int timeout, int attempt) {
        return ((long) timeout << 32) | (attempt & 0xFFFFFFFFL);
    }

    /**
     * Extracts the timeout from a packed raw state value.
     *
     * @param packed raw state value produced by {@link #pack(int, int)} or read directly
     *               from the underlying {@link AtomicLong}.
     * @return timeout in milliseconds.
     */
    static int timeout(long packed) {
        return (int) (packed >>> 32);
    }

    /**
     * Extracts the attempt count from a packed raw state value.
     *
     * @param packed raw state value produced by {@link #pack(int, int)} or read directly
     *               from the underlying {@link AtomicLong}.
     * @return attempt count.
     */
    static int attempt(long packed) {
        return (int) packed;
    }
}
