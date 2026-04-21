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
import static org.apache.ignite.internal.util.retry.TimeoutStrategy.DEFAULT_RETRY_TIMEOUT_MS_MAX;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
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
 * that always returns {@link TimeoutStrategy#DEFAULT_RETRY_TIMEOUT_MS_MAX}. The limit is a soft cap and may be
 * slightly exceeded under concurrent insertions.
 *
 * <p>This class is thread-safe.
 */
public class KeyBasedRetryContext implements RetryContext {
    /**
     * Maximum number of keys tracked in {@link #registry}.
     * Once the limit is reached, untracked keys receive a fixed {@link #fallbackTimeoutState}.
     * Can be slightly exceeded under concurrent insertions.
     */
    private static final int REGISTRY_SIZE_LIMIT = 1_000;

    /** Strategy used to compute the next timeout from the current one on each advancement. */
    private final TimeoutStrategy timeoutStrategy;

    /**
     * Sentinel state returned for keys that cannot be tracked because the registry is full.
     * Initialized with {@link TimeoutStrategy#DEFAULT_RETRY_TIMEOUT_MS_MAX} and attempt {@code -1}
     * to distinguish it from legitimately tracked states.
     */
    private final TimeoutState fallbackTimeoutState;

    /** Per-key timeout state registry. Keys are typically transaction IDs or replication group IDs. */
    private final ConcurrentHashMap<String, TimeoutState> registry = new ConcurrentHashMap<>();

    /**
     * Creates a new context with the given initial timeout and strategy.
     *
     * @param timeoutStrategy strategy used to compute subsequent timeout values.
     */
    public KeyBasedRetryContext(TimeoutStrategy timeoutStrategy) {
        this.timeoutStrategy = timeoutStrategy;

        this.fallbackTimeoutState = new TimeoutState(DEFAULT_RETRY_TIMEOUT_MS_MAX, -1);
    }

    /**
     * Returns the current {@link TimeoutState} for the given key, if tracked.
     *
     * <p>Returns an empty {@link Optional} if the key has no recorded state yet.
     * If the registry is full and the key is not yet tracked, returns an {@link Optional}
     * containing a fallback state initialized to {@link TimeoutStrategy#DEFAULT_RETRY_TIMEOUT_MS_MAX}.
     *
     * <p>This method does not insert the key into the registry.
     *
     * @param key the key to look up, typically a transaction ID or replication group ID.
     * @return current state for the key, fallback state if registry is full, or empty if not tracked.
     */
    @Override
    public Optional<TimeoutState> getState(String key) {
        if (!registry.containsKey(key) && registry.size() >= REGISTRY_SIZE_LIMIT) {
            return of(fallbackTimeoutState);
        }

        return ofNullable(registry.get(key));
    }

    /**
     * Atomically advances the retry state for the given key and returns the updated state.
     *
     * <p>The update is performed inside {@link ConcurrentHashMap#compute}, which holds
     * an exclusive per-key lock for the duration of the lambda, ensuring that
     * {@link TimeoutState#update(TimeoutStrategy)} is never called concurrently on the same instance.
     *
     * <p>When the registry is full, untracked keys receive the maximum timeout.
     * This acts as implicit backpressure: if enough keys are actively retrying to fill
     * the registry, the system is under a heavy load and new operations should retry conservatively.
     *
     * @param key the key to advance state for, typically a transaction ID or replication group ID.
     * @return updated {@link TimeoutState} for the key, or {@link #fallbackTimeoutState}
     *         if the registry is full.
     */
    @Override
    public TimeoutState updateAndGetState(String key) {
        if (!registry.containsKey(key) && registry.size() >= REGISTRY_SIZE_LIMIT) {
            return fallbackTimeoutState;
        }

        return registry.compute(key, (k, state) -> {
            if (state == null) {
                state = new TimeoutState();
            }

            state.update(timeoutStrategy);

            return state;
        });
    }

    /**
     * Removes the retry state for the given key, resetting it as if no retries had occurred.
     *
     * @param key the key whose state should be removed.
     */
    @Override
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
