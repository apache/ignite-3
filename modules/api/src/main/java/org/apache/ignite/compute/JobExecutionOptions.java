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

package org.apache.ignite.compute;

import org.apache.ignite.lang.CancellationToken;
import org.jetbrains.annotations.Nullable;

/**
 * Job execution options.
 */
public class JobExecutionOptions {

    /**
     * Default job execution options with priority default value = 0 and max retries default value = 0.
     */
    public static final JobExecutionOptions DEFAULT = builder().priority(0).maxRetries(0).build();

    private final int priority;

    private final int maxRetries;

    @Nullable private final CancellationToken cancellationToken;

    /**
     * Constructor.
     *
     * @param priority Job execution priority.
     * @param maxRetries Number of times to retry job execution in case of failure, 0 to not retry.
     * @param cancellationToken Cancellation token or {@code null}.
     *
     */
    private JobExecutionOptions(int priority, int maxRetries, @Nullable CancellationToken cancellationToken) {
        this.priority = priority;
        this.maxRetries = maxRetries;
        this.cancellationToken = cancellationToken;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int priority() {
        return priority;
    }

    public int maxRetries() {
        return maxRetries;
    }

    public @Nullable CancellationToken cancellationToken() {
        return cancellationToken;
    }

    /** JobExecutionOptions builder. */
    public static class Builder {
        private int priority;

        private int maxRetries;

        private CancellationToken cancellationToken;

        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        /**
         * Bind {@link CancellationToken} with current job execution.
         *
         * @param cancellationToken Cancellation token or {@code null}.
         * @return {@code this} for chaining.
         */
        public Builder cancellationToken(CancellationToken cancellationToken) {
            this.cancellationToken = cancellationToken;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public JobExecutionOptions build() {
            return new JobExecutionOptions(priority, maxRetries, cancellationToken);
        }
    }
}
