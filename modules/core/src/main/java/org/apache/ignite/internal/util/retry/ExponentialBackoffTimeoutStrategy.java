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
 * a.
 */
public class ExponentialBackoffTimeoutStrategy implements TimeoutStrategy {
    /** Default backoff coefficient to calculate next timeout based on backoff strategy. */
    private static final double DEFAULT_BACKOFF_COEFFICIENT = 2.0;

    /**
     * a.
     */
    private final double backoffCoefficient;

    /**
     * a.
     */
    private final boolean jitter;

    /**
     * a.
     */
    private final int maxTimeout;

    /**
     * a.
     */
    public ExponentialBackoffTimeoutStrategy() {
        this(DEFAULT_TIMEOUT_MS_MAX, DEFAULT_BACKOFF_COEFFICIENT);
    }

    /**
     * a.
     *
     * @param backoffCoefficient a.
     */
    public ExponentialBackoffTimeoutStrategy(
            int maxTimeout,
            double backoffCoefficient
    ) {
        this(maxTimeout, backoffCoefficient, false);
    }

    /**
     * a.
     *
     * @param backoffCoefficient a.
     * @param jitter a.
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

    /** {@inheritDoc} */
    @Override
    public int next(int currentTimeout) {
        int raw = (int) Math.min((currentTimeout * backoffCoefficient), maxTimeout);

        return jitter ? applyJitter(raw) : raw;
    }

    @Override
    public int maxTimeout() {
        return maxTimeout;
    }

    /**
     * a.
     *
     * @param raw a.
     * @return a.
     */
    private int applyJitter(int raw) {
        int lo = raw / 2;
        int hi = raw + lo;

        return Math.min(lo + ThreadLocalRandom.current().nextInt(hi - lo + 1), maxTimeout);
    }
}
