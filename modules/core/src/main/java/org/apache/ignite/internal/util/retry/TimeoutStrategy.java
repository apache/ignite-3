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

/**
 * Stateless strategy for computing the next retry timeout based on the current one.
 *
 * <p>Implementations must be stateless and thread-safe — the same instance is expected
 * to be shared across multiple callers and retry contexts. All mutable state (current
 * timeout, attempt count) is maintained externally by the caller or a retry context.
 *
 * @see ExponentialBackoffTimeoutStrategy
 * @see NoopTimeoutStrategy
 */
public interface TimeoutStrategy {
    /** Default maximum timeout that a strategy may produce, in milliseconds. */
    int DEFAULT_TIMEOUT_MS_MAX = 11_000;

    /**
     * Computes the next retry timeout based on the current one.
     *
     * <p>Implementations must not produce a value exceeding {@link #maxTimeout()}.
     * The returned value is used directly as the delay before the next retry attempt.
     *
     * @param currentTimeout current retry timeout in milliseconds.
     * @return next retry timeout in milliseconds, capped at {@link #maxTimeout()}.
     */
    int next(int currentTimeout);

    /**
     * Returns the maximum timeout this strategy can produce, in milliseconds.
     *
     * <p>Once the timeout reaches this ceiling, further calls to {@link #next(int)}
     * must continue to return this value rather than exceeding it.
     *
     * @return maximum timeout in milliseconds.
     */
    int maxTimeout();
}
