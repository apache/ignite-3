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

import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static org.apache.ignite.internal.util.retry.TimeoutState.attempt;
import static org.apache.ignite.internal.util.retry.TimeoutState.timeout;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class KeyBasedRetryContext {
    /**
     * It can be slightly overflown.
     */
    private static final int REGISTRY_SIZE_LIMIT = 1_000;

    /**
     * a.
     */
    private final int initialTimeout;

    private final TimeoutStrategy timeoutStrategy;

    private final TimeoutState fallbackTimeoutState;

    /**
     * a.
     */
    private final ConcurrentHashMap<String, TimeoutState> registry = new ConcurrentHashMap<>();

    public KeyBasedRetryContext(int initialTimeout, TimeoutStrategy timeoutStrategy) {
        this.initialTimeout = initialTimeout;
        this.timeoutStrategy = timeoutStrategy;

        this.fallbackTimeoutState = new TimeoutState(timeoutStrategy.maxTimeout(), -1);
    }

    public Optional<TimeoutState> getState(String key) {
        if (!registry.containsKey(key) && registry.size() >= REGISTRY_SIZE_LIMIT) {
            return of(fallbackTimeoutState);
        }

        return ofNullable(registry.get(key));
    }

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

    public void resetState(String key) {
        registry.remove(key);
    }
}
