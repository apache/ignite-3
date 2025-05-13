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

package org.apache.ignite.table;

import java.util.Objects;
import org.apache.ignite.compute.JobExecutorType;

/**
 * Streamer receiver execution options.
 */
public class ReceiverExecutionOptions {
    /**
     * Default receiver execution options.
     */
    public static final ReceiverExecutionOptions DEFAULT = builder().priority(0).maxRetries(0).build();

    private final int priority;

    private final int maxRetries;

    private final JobExecutorType executorType;

    /**
     * Constructor.
     *
     * @param priority Job execution priority.
     * @param maxRetries Number of times to retry job execution in case of failure, 0 to not retry.
     * @param executorType Job executor type.
     */
    private ReceiverExecutionOptions(int priority, int maxRetries, JobExecutorType executorType) {
        this.priority = priority;
        this.maxRetries = maxRetries;
        this.executorType = executorType;
    }

    /**
     * Creates the builder for job execution options.
     *
     * @return Builder for job execution options.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Gets the receiver execution priority.
     *
     * @return Receiver execution priority.
     */
    public int priority() {
        return priority;
    }

    /**
     * Gets the number of times to retry receiver execution in case of failure.
     *
     * @return Number of retries.
     */
    public int maxRetries() {
        return maxRetries;
    }

    /**
     * Gets the receiver executor type.
     *
     * @return Receiver executor type.
     */
    public JobExecutorType executorType() {
        return executorType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(priority, maxRetries, executorType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ReceiverExecutionOptions other = (ReceiverExecutionOptions) obj;

        return priority == other.priority
                && maxRetries == other.maxRetries
                && executorType == other.executorType;
    }

    /** ReceiverExecutionOptions builder. */
    public static class Builder {
        private int priority;

        private int maxRetries;

        private JobExecutorType executorType = JobExecutorType.JAVA_EMBEDDED;

        /**
         * Sets the receiver execution priority.
         *
         * @param priority Receiver execution priority.
         * @return Builder instance.
         */
        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        /**
         * Sets the number of times to retry receiver execution in case of failure.
         *
         * @param maxRetries Number of retries. 0 to not retry.
         * @return Builder instance.
         */
        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        /**
         * Sets the receiver executor type. See {@link JobExecutorType} for more details.
         *
         * @param jobExecutorType Receiver executor type.
         * @return Builder instance.
         */
        public Builder executorType(JobExecutorType jobExecutorType) {
            this.executorType = jobExecutorType;
            return this;
        }

        /**
         * Builds the receiver execution options.
         *
         * @return Receiver execution options.
         */
        public ReceiverExecutionOptions build() {
            return new ReceiverExecutionOptions(priority, maxRetries, executorType);
        }
    }
}
