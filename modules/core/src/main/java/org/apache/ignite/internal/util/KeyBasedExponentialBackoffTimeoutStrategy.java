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

package org.apache.ignite.internal.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * a.
 */
public class KeyBasedExponentialBackoffTimeoutStrategy implements TimeoutStrategy {
    /** Default backoff coefficient to calculate next timeout based on backoff strategy. */
    private static final double DEFAULT_BACKOFF_COEFFICIENT = 2.0;

    /** Default max timeout that strategy could generate, ms. */
    private static final int DEFAULT_TIMEOUT_MS_MAX = 11_000;

    /**
     * a.
     */
    private final int initialTimeout;

    /**
     * a.
     */
    private final int maxTimeout;

    /**
     * a.
     */
    private final double backoffCoefficient;

    /**
     * a.
     */
    private final boolean jitter;

    // Thread-safe key → state mapping
    private final ConcurrentHashMap<String, TimeoutState> registry = new ConcurrentHashMap<>();

    /**
     * a.
     *
     * @param initialTimeout a.
     */
    public KeyBasedExponentialBackoffTimeoutStrategy(
            int initialTimeout
    ) {
        this(initialTimeout, DEFAULT_TIMEOUT_MS_MAX, DEFAULT_BACKOFF_COEFFICIENT);
    }

    /**
     * a.
     *
     * @param initialTimeout a.
     * @param maxTimeout a.
     * @param backoffCoefficient a.
     */
    public KeyBasedExponentialBackoffTimeoutStrategy(
            int initialTimeout,
            int maxTimeout,
            double backoffCoefficient
    ) {
        this(initialTimeout, maxTimeout, backoffCoefficient, false);
    }

    /**
     * a.
     *
     * @param initialTimeout a.
     * @param maxTimeout a.
     * @param backoffCoefficient a.
     * @param jitter a.
     */
    public KeyBasedExponentialBackoffTimeoutStrategy(
            int initialTimeout,
            int maxTimeout,
            double backoffCoefficient,
            boolean jitter
    ) {
        this.initialTimeout = initialTimeout;
        this.maxTimeout = maxTimeout;
        this.backoffCoefficient = backoffCoefficient;
        this.jitter = jitter;
    }

    /** {@inheritDoc} */
    @Override
    public TimeoutState getCurrent(String key) {
        return registry.getOrDefault(key, new TimeoutState(initialTimeout, 0));
    }

    /** {@inheritDoc} */
    @Override
    public int next(String key) {
        return registry.compute(key, (k, prev) -> {
            int raw = (prev == null)
                    ? initialTimeout
                    : Math.min((int) (prev.getCurrentTimeout() * backoffCoefficient), maxTimeout);

            int timeout = jitter ? applyJitter(raw) : raw;
            int attempt  = (prev == null) ? 1 : prev.getAttempt() + 1;

            return new TimeoutState(timeout, attempt);
        }).getCurrentTimeout();
    }

    /** {@inheritDoc} */
    @Override
    public void reset(String key) {
        registry.remove(key);
    }

    /** {@inheritDoc} */
    @Override
    public void resetAll() {
        registry.clear();
    }

    /**
     * a.
     *
     * @param raw a.
     * @return a.
     */
    private static int applyJitter(int raw) {
        int lo = raw / 2;
        int hi = raw + lo; // raw * 1.5

        return lo + ThreadLocalRandom.current().nextInt(hi - lo + 1);
    }
}
