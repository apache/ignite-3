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

import static java.util.Collections.unmodifiableMap;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static org.apache.ignite.internal.util.retry.TimeoutState.attempt;
import static org.apache.ignite.internal.util.retry.TimeoutState.timeout;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.jetbrains.annotations.TestOnly;

/**
 * A retry context that tracks timeout state independently per key.
 *
 * <p>Each key maps to its own {@link TimeoutState}, allowing separate backoff progression
 * for different retry targets — for example, different replication group IDs or transaction IDs.
 * State updates are performed atomically per key using {@link ConcurrentHashMap#compute}.
 *
 * <p>To prevent unbounded memory growth, the registry is capped at {@link #REGISTRY_SIZE_LIMIT}
 * entries. Once the limit is reached, untracked keys receive a fixed {@link #fallbackTimeoutState}
 * that always returns {@link TimeoutStrategy#maxTimeout()}. The limit is a soft cap and may be
 * slightly exceeded under concurrent insertions.
 *
 * <p>This class is thread-safe.
 */
public class KeyBasedRetryContext {
    /**
     * Maximum number of keys tracked in {@link #registry}.
     * Can be slightly exceeded under concurrent insertions. See class-level Javadoc.
     */
    private static final int REGISTRY_SIZE_LIMIT = 1_000;

    /**
     * Timeout used when creating a new {@link TimeoutState} for a key that has no prior state.
     * Also used as the reset value when a key's state is removed.
     */
    private final int initialTimeout;

    /** Strategy used to compute the next timeout from the current one on each advancement. */
    private final TimeoutStrategy timeoutStrategy;

    /**
     * Sentinel state returned for keys that cannot be tracked because the registry is full.
     * Initialized with {@link TimeoutStrategy#maxTimeout()} and attempt {@code -1} to distinguish
     * it from legitimately tracked states.
     */
    private final TimeoutState fallbackTimeoutState;

    /** Per-key timeout state registry. Keys are typically transaction IDs or replication group IDs. */
    private final ConcurrentHashMap<String, TimeoutState> registry = new ConcurrentHashMap<>();

    /**
     * Creates a new context with the given initial timeout and strategy.
     *
     * @param initialTimeout timeout used for the first retry attempt of any new key, in milliseconds.
     * @param timeoutStrategy strategy used to compute subsequent timeout values.
     */
    public KeyBasedRetryContext(int initialTimeout, TimeoutStrategy timeoutStrategy) {
        this.initialTimeout = initialTimeout;
        this.timeoutStrategy = timeoutStrategy;

        this.fallbackTimeoutState = new TimeoutState(timeoutStrategy.maxTimeout(), -1);
    }

    /**
     * Returns the current {@link TimeoutState} for the given key, if tracked.
     *
     * <p>Returns an empty {@link Optional} if the key has no recorded state yet.
     * If the registry is full and the key is not already tracked, returns
     * {@link Optional} containing the {@link #fallbackTimeoutState}.
     *
     * <p>This method does not insert the key into the registry.
     *
     * @param key the key to look up, typically a transaction ID or replication group ID.
     * @return current state for the key, fallback state if registry is full, or empty if not tracked.
     */
    public Optional<TimeoutState> getState(String key) {
        if (!registry.containsKey(key) && registry.size() >= REGISTRY_SIZE_LIMIT) {
            return of(fallbackTimeoutState);
        }

        return ofNullable(registry.get(key));
    }

    /**
     * Atomically advances the retry state for the given key and returns the updated state.
     *
     * <p>If the key has no prior state, a new {@link TimeoutState} is created with
     * {@link #initialTimeout} and attempt count {@code 1}. Otherwise, the timeout is
     * advanced using {@link TimeoutStrategy#next(int)} and the attempt count is incremented.
     *
     * <p>The update is performed inside {@link ConcurrentHashMap#compute}, which holds
     * an exclusive per-key lock for the duration of the lambda. The CAS on the
     * {@link TimeoutState}'s internal {@link AtomicLong} is therefore always expected
     * to succeed on the first attempt within the lambda.
     *
     * <p>If the registry is full and the key is not already tracked, returns
     * {@link #fallbackTimeoutState} without modifying the registry.
     *
     * @param key the key to advance state for, typically a transaction ID or replication group ID.
     * @return updated {@link TimeoutState} for the key, or {@link #fallbackTimeoutState}
     *         if the registry is full.
     */
    public TimeoutState updateAndGetState(String key) {
        if (!registry.containsKey(key) && registry.size() >= REGISTRY_SIZE_LIMIT) {
            return fallbackTimeoutState;
        }

        return registry.compute(key, (k, state) -> {
            if (state == null) {
                return new TimeoutState(initialTimeout, 1);
            }

            long currentState = state.getRawState();
            state.update(currentState, timeoutStrategy.next(timeout(currentState)), attempt(currentState) + 1);

            return state;
        });
    }

    /**
     * Removes the retry state for the given key, resetting it as if no retries had occurred.
     *
     * <p>The next call to {@link #updateAndGetState(String)} for this key after a reset
     * will create fresh state starting from {@link #initialTimeout}.
     *
     * @param key the key whose state should be removed.
     */
    public void resetState(String key) {
        registry.remove(key);
    }

    /**
     * Returns an unmodifiable snapshot of the current registry contents.
     *
     * <p>The snapshot is a point-in-time copy of the registry map. The returned
     * {@link TimeoutState} values are live references — their internal state may
     * continue to change concurrently after the snapshot is taken.
     *
     * <p>This method is intended for testing only and should not be used in production code.
     *
     * @return unmodifiable copy of the current key-to-state mappings.
     */
    @TestOnly
    public Map<String, TimeoutState> snapshot() {
        return unmodifiableMap(new HashMap<>(registry));
    }
}
