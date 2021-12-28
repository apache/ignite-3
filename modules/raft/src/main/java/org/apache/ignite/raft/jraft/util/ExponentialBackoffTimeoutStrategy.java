/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.raft.jraft.util;

import org.apache.ignite.internal.tostring.S;

/**
 * Timeout generation strategy.
 * Increases startTimeout based on exponential backoff algorithm.
 */
public class ExponentialBackoffTimeoutStrategy implements TimeoutStrategy {
    /** Default backoff coefficient to calculate next timeout based on backoff strategy. */
    private static final double DEFAULT_BACKOFF_COEFFICIENT = 2.0;

    /** Max timeout of the next try, ms. */
    private final int maxTimeout;

    /** Current calculated timeout, ms. */
    private int currentTimeout;

    /** Initial timeout. */
    private final int startTimeout;

    /* Overall number of retries to get next timeout without adjusting timeout. */
    private final int retryCounter;

    /** Current counter of retries without adjusting timeout. */
    private int currentRetry;

    /**
     * @param startTimeout Initial timeout.
     * @param maxTimeout Max timeout.
     * @param retryCounter Number of retries without increasing timeout.
     */
    public ExponentialBackoffTimeoutStrategy(
            int startTimeout,
            int maxTimeout,
            int retryCounter
    ) {
        this.maxTimeout = maxTimeout;

        this.startTimeout = startTimeout;

        this.currentTimeout = startTimeout;

        this.retryCounter = retryCounter;

        currentRetry = 0;
    }

    /**
     * @param timeout Timeout.
     * @param maxTimeout Maximum timeout for backoff function.
     * @return Next exponential backoff timeout.
     */
    public static int backoffTimeout(int timeout, int maxTimeout) {
        return (int) Math.min(timeout * DEFAULT_BACKOFF_COEFFICIENT, maxTimeout);
    }

    /** {@inheritDoc} */
    @Override
    public int nextTimeout() {
        if (currentRetry < retryCounter) {
            currentRetry++;

            return currentTimeout;
        }

        currentTimeout = backoffTimeout(currentTimeout, maxTimeout);

        currentRetry = 0;

        return currentTimeout;
    }

    /** {@inheritDoc} */
    @Override
    public int reset() {
        currentTimeout = startTimeout;

        currentRetry = 0;

        return currentTimeout;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(ExponentialBackoffTimeoutStrategy.class, this);
    }
}
