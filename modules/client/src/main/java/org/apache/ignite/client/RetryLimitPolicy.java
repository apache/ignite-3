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

package org.apache.ignite.client;

import java.util.Objects;

/**
 * Retry policy that returns true when {@link RetryPolicyContext#iteration()} is less than the specified {@link #retryLimit()}.
 */
public class RetryLimitPolicy implements RetryPolicy {
    /** Default retry limit. */
    public static final int DFLT_RETRY_LIMIT = 16;

    /** Retry limit. */
    private int retryLimit = DFLT_RETRY_LIMIT;

    /**
     * Gets the retry limit. 0 or less for no limit.
     *
     * @return Retry limit.
     */
    public int retryLimit() {
        return retryLimit;
    }

    /**
     * Sets the retry limit. 0 or less for no limit.
     *
     * @return this instance.
     */
    public RetryLimitPolicy retryLimit(int retryLimit) {
        this.retryLimit = retryLimit;

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public boolean shouldRetry(RetryPolicyContext context) {
        Objects.requireNonNull(context);

        if (retryLimit <= 0) {
            return true;
        }

        return context.iteration() < retryLimit;
    }
}
