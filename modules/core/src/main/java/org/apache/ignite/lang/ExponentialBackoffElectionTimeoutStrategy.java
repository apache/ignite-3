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

package org.apache.ignite.lang;

import org.apache.ignite.internal.tostring.S;

/**
 * Strategy...
 * It increases startTimeout based on exponential backoff algorithm.
 */
public class ExponentialBackoffElectionTimeoutStrategy implements ElectionTimeoutStrategy {
    /** Default backoff coefficient to calculate next timeout based on backoff strategy. */
    private static final double DEFAULT_BACKOFF_COEFFICIENT = 2.0;

    /** Max timeout of the next try, ms. */
    private final long maxTimeout;

    /** Current calculated timeout, ms. */
    private long currentTimeout;

    private final long startTimeout;

    private final int retryCounter;

    private int currentElectionRound;

    /**
     * @param startTimeout Initial connection timeout.
     * @param maxTimeout   Max connection Timeout.
     */
    public ExponentialBackoffElectionTimeoutStrategy(
            long startTimeout,
            long maxTimeout,
            int retryCounter
    ) {
        this.maxTimeout = maxTimeout;

        this.startTimeout = startTimeout;

        this.currentTimeout = startTimeout;

        this.retryCounter = retryCounter;

        currentElectionRound = 0;
    }

    /**
     * @param timeout    Timeout.
     * @param maxTimeout Maximum startTimeout for backoff function.
     * @return Next exponential backoff timeout.
     */
    public static long backoffTimeout(long timeout, long maxTimeout) {
        return (long) Math.min(timeout * DEFAULT_BACKOFF_COEFFICIENT, maxTimeout);
    }

    /** {@inheritDoc} */
    @Override
    public long nextTimeout() throws IgniteInternalException {
        if (currentElectionRound < retryCounter) {
            currentElectionRound++;

            return currentTimeout;
        }

        currentTimeout = backoffTimeout(currentTimeout, maxTimeout);

        currentElectionRound = 0;

        return currentTimeout;
    }

    public long reset() {
        currentTimeout = startTimeout;

        currentElectionRound = 0;

        return currentTimeout;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(ExponentialBackoffElectionTimeoutStrategy.class, this);
    }
}
