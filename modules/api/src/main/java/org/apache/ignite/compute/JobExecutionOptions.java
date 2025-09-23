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

    private final JobExecutorType executorType;

    /**
     * Constructor.
     *
     * @param priority Job execution priority.
     * @param maxRetries Number of times to retry job execution in case of failure, 0 to not retry.
     * @param executorType Job executor type.
     */
    private JobExecutionOptions(int priority, int maxRetries, JobExecutorType executorType) {
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
     * Gets the job execution priority.
     *
     * @return Job execution priority.
     */
    public int priority() {
        return priority;
    }

    /**
     * Gets the number of times to retry job execution in case of failure.
     *
     * @return Number of retries.
     */
    public int maxRetries() {
        return maxRetries;
    }

    /**
     * Gets the job executor type.
     *
     * @return Job executor type.
     */
    public JobExecutorType executorType() {
        return executorType;
    }

    /** JobExecutionOptions builder. */
    public static class Builder {
        private int priority;

        private int maxRetries;

        private JobExecutorType executorType = JobExecutorType.JAVA_EMBEDDED;

        /**
         * Sets the job execution priority.
         *
         * @param priority Job execution priority.
         * @return Builder instance.
         */
        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        /**
         * Sets the number of times to retry job execution in case of failure.
         *
         * @param maxRetries Number of retries. 0 to not retry.
         * @return Builder instance.
         */
        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        /**
         * Sets the job executor type. See {@link JobExecutorType} for more details.
         *
         * @param jobExecutorType Job executor type.
         * @return Builder instance.
         */
        public Builder executorType(JobExecutorType jobExecutorType) {
            this.executorType = jobExecutorType;
            return this;
        }

        /**
         * Builds the job execution options.
         *
         * @return Job execution options.
         */
        public JobExecutionOptions build() {
            return new JobExecutionOptions(priority, maxRetries, executorType);
        }
    }
}
