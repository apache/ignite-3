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

import java.util.concurrent.ThreadLocalRandom;

/**
 * A {@link TimeoutStrategy} that increases retry timeouts exponentially on each attempt.
 *
 * <p>Each call to {@link #next(int)} multiplies the current timeout by {@code backoffCoefficient},
 * capping the result at {@link #maxTimeout()}. Optionally, random jitter can be applied to spread
 * retry attempts across time and avoid thundering herd problems under high concurrency.
 *
 * <p>When jitter is enabled, the returned timeout is randomized within the range
 * {@code [raw / 2, raw * 1.5]}, then capped at {@link #maxTimeout()}.
 *
 * <p>This class is stateless and thread-safe. A single instance can be shared across
 * multiple retry contexts.
 */
public class ExponentialBackoffTimeoutStrategy implements TimeoutStrategy {
    /** Default backoff coefficient applied on each retry step. Doubles the timeout per attempt. */
    private static final double DEFAULT_BACKOFF_COEFFICIENT = 2.0;

    /**
     * Multiplier applied to the current timeout on each call to {@link #next(int)}.
     * Must be greater than {@code 1.0} to produce a growing sequence.
     */
    private final double backoffCoefficient;

    /**
     * Whether to apply random jitter to the computed timeout.
     * When {@code true}, the result is randomized within {@code [raw / 2, raw * 1.5]}.
     */
    private final boolean jitter;

    /**
     * Upper bound for the timeout produced by this strategy, in milliseconds.
     * The result of {@link #next(int)} is always capped at this value.
     */
    private final int maxTimeout;

    /**
     * Creates a strategy with default max timeout and backoff coefficient, and no jitter.
     *
     * @see TimeoutStrategy#DEFAULT_TIMEOUT_MS_MAX
     */
    public ExponentialBackoffTimeoutStrategy() {
        this(DEFAULT_TIMEOUT_MS_MAX, DEFAULT_BACKOFF_COEFFICIENT);
    }

    /**
     * Creates a strategy with the given max timeout and backoff coefficient, and no jitter.
     *
     * @param maxTimeout         maximum timeout this strategy may produce, in milliseconds.
     * @param backoffCoefficient multiplier applied to the current timeout on each step.
     *                           Must be greater than {@code 1.0}.
     */
    public ExponentialBackoffTimeoutStrategy(
            int maxTimeout,
            double backoffCoefficient
    ) {
        this(maxTimeout, backoffCoefficient, false);
    }

    /**
     * Creates a strategy with the given max timeout, backoff coefficient, and jitter setting.
     *
     * @param maxTimeout         maximum timeout this strategy may produce, in milliseconds.
     * @param backoffCoefficient multiplier applied to the current timeout on each step.
     *                           Must be greater than {@code 1.0}.
     * @param jitter             if {@code true}, random jitter is applied to each computed timeout.
     */
    public ExponentialBackoffTimeoutStrategy(
            int maxTimeout,
            double backoffCoefficient,
            boolean jitter
    ) {
        this.maxTimeout = maxTimeout;
        this.backoffCoefficient = backoffCoefficient;
        this.jitter = jitter;
    }

    /**
     * Computes the next retry timeout by multiplying {@code currentTimeout} by
     * {@link #backoffCoefficient}, then capping at {@link #maxTimeout}.
     * If jitter is enabled, the result is further randomized via {@link #applyJitter(int)}.
     *
     * @param currentTimeout current retry timeout in milliseconds.
     * @return next retry timeout in milliseconds, capped at {@link #maxTimeout}.
     */
    @Override
    public int next(int currentTimeout) {
        int raw = (int) Math.min((currentTimeout * backoffCoefficient), maxTimeout);

        return jitter ? applyJitter(raw) : raw;
    }

    /** {@inheritDoc} */
    @Override
    public int maxTimeout() {
        return maxTimeout;
    }

    /**
     * Applies random jitter to the given timeout value.
     *
     * <p>The result is uniformly distributed within {@code [raw / 2, raw * 1.5]},
     * then capped at {@link #maxTimeout} to ensure the strategy ceiling is never exceeded.
     *
     * @param raw computed timeout before jitter, in milliseconds.
     * @return jittered timeout in milliseconds, capped at {@link #maxTimeout}.
     */
    private int applyJitter(int raw) {
        int lo = raw / 2;
        int hi = raw + lo;

        return Math.min(lo + ThreadLocalRandom.current().nextInt(hi - lo + 1), maxTimeout);
    }
}
