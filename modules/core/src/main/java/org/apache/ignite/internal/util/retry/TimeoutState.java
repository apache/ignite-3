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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Mutable, thread-safe holder for retry timeout and attempt count.
 *
 * <p>Both fields are packed into a single {@link AtomicLong} to allow atomic
 * compare-and-set updates of the combined state. The high 32 bits store the
 * timeout in milliseconds; the low 32 bits store the attempt count.
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
     * Creates a new {@code TimeoutState} with the given initial timeout and attempt count.
     *
     * @param timeout        initial timeout in milliseconds.
     * @param initialAttempt initial attempt count. Use {@code 0} as a sentinel to indicate
     *                       "initialized but not yet advanced" when lazy initialization is needed.
     */
    public TimeoutState(int timeout, int initialAttempt) {
        state.set(pack(timeout, initialAttempt));
    }

    /**
     * Returns the raw packed {@code long} representing the current state.
     *
     * <p>Callers that need both timeout and attempt count atomically should read this
     * once and pass it to {@link #timeout(long)} and {@link #attempt(long)}, rather
     * than calling {@link #getTimeout()} and {@link #getAttempt()} separately.
     *
     * @return raw packed state value.
     */
    public long getRawState() {
        return state.get();
    }

    /**
     * Returns the current retry timeout in milliseconds.
     *
     * <p>This is a single atomic read. If both timeout and attempt are needed
     * consistently, use {@link #getRawState()} instead.
     *
     * @return current timeout in milliseconds.
     */
    public int getTimeout() {
        return timeout(state.get());
    }

    /**
     * Returns the current attempt count.
     *
     * <p>This is a single atomic read. If both timeout and attempt are needed
     * consistently, use {@link #getRawState()} instead.
     *
     * @return current attempt count.
     */
    public int getAttempt() {
        return attempt(state.get());
    }

    /**
     * Atomically updates this state if it still matches the expected raw snapshot.
     *
     * <p>This is a standard CAS (compare-and-set) operation. If the internal state
     * has changed since {@code currentState} was read, the update is rejected and
     * the caller is expected to retry by reading a fresh snapshot via {@link #getRawState()}.
     *
     * @param currentState expected current raw state, obtained from a prior {@link #getRawState()} call.
     * @param newTimeout   new timeout value in milliseconds.
     * @param newAttempt   new attempt count.
     * @return {@code true} if the update succeeded; {@code false} if the state was
     *         concurrently modified and the update was rejected.
     */
    public boolean update(long currentState, int newTimeout, int newAttempt) {
        return state.compareAndSet(currentState, pack(newTimeout, newAttempt));
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
     * @param packed raw state value obtained from {@link #getRawState()} or {@link #pack(int, int)}.
     * @return timeout in milliseconds.
     */
    static int timeout(long packed) {
        return (int) (packed >>> 32);
    }

    /**
     * Extracts the attempt count from a packed raw state value.
     *
     * @param packed raw state value obtained from {@link #getRawState()} or {@link #pack(int, int)}.
     * @return attempt count.
     */
    static int attempt(long packed) {
        return (int) packed;
    }
}
