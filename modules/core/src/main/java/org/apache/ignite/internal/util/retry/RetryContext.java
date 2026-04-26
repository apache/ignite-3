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

import java.util.Optional;

/**
 * Manages per-key retry state for tracking timeout progression across retry attempts.
 *
 * <p>Each key (typically a transaction ID or replication group ID) maps to an independent
 * {@link TimeoutState} that records the current retry timeout and attempt count. The context
 * is responsible for creating, advancing, and removing that state.
 *
 * <p>Implementations must be thread-safe.
 *
 * @see KeyBasedRetryContext
 * @see TimeoutState
 */
public interface RetryContext {

    /**
     * Returns the current {@link TimeoutState} for the given key, if one exists.
     *
     * <p>Returns an empty {@link Optional} if the key has not yet been tracked.
     * Implementations may also return a fallback state when internal capacity limits are
     * reached, in which case the returned state reflects the maximum permissible timeout.
     *
     * <p>This method must not modify the registry — it is a read-only lookup.
     *
     * @param key the key to look up, typically a transaction ID or replication group ID.
     * @return current {@link TimeoutState} for the key, or empty if not yet tracked.
     */
    Optional<TimeoutState> getState(String key);

    /**
     * Atomically advances the retry state for the given key and returns the updated state.
     *
     * <p>If no state exists for the key yet, a fresh {@link TimeoutState} is created.
     * Otherwise, the existing state is advanced using the configured
     * {@link TimeoutStrategy} and the attempt count is incremented.
     *
     * <p>When internal capacity limits prevent tracking new keys, a fallback state with the
     * maximum timeout is returned instead.
     *
     * @param key the key to advance state for, typically a transaction ID or replication group ID.
     * @return updated {@link TimeoutState} for the key, or a fallback state if capacity is exhausted.
     */
    TimeoutState updateAndGetState(String key);

    /**
     * Removes the retry state for the given key, resetting it as if no retries had occurred.
     *
     * <p>This allows future calls to {@link #updateAndGetState(String)} for the same key to
     * start fresh with the initial timeout.
     *
     * @param key the key whose state should be removed.
     */
    void resetState(String key);
}
