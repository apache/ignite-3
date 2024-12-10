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

package org.apache.ignite.internal.compute;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.compute.JobExecutionOptions;
import org.jetbrains.annotations.Nullable;

/**
 * Compute job execution options.
 */
public class ExecutionOptions {
    public static final ExecutionOptions DEFAULT = builder().build();

    private final int priority;

    private final int maxRetries;

    private final List<Integer> partitions;

    /**
     * Constructor.
     *
     * @param priority Job execution priority.
     * @param maxRetries Number of times to retry job execution in case of failure, 0 to not retry.
     * @param partitions List of partitions numbers associated with this job.
     */
    private ExecutionOptions(int priority, int maxRetries, @Nullable List<Integer> partitions) {
        this.priority = priority;
        this.maxRetries = maxRetries;
        this.partitions = partitions;
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

    public @Nullable List<Integer> partitions() {
        return partitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExecutionOptions that = (ExecutionOptions) o;
        return priority == that.priority && maxRetries == that.maxRetries && Objects.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(priority, maxRetries, partitions);
    }

    /** Compose execution options.  */
    public static ExecutionOptions from(JobExecutionOptions jobExecutionOptions) {
        return builder().priority(jobExecutionOptions.priority()).maxRetries(jobExecutionOptions.maxRetries()).build();
    }

    /** Builder. */
    public static class Builder {
        private int priority;

        private int maxRetries;

        private @Nullable List<Integer> partitions;

        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder partitions(List<Integer> partitions) {
            this.partitions = partitions;
            return this;
        }

        public ExecutionOptions build() {
            return new ExecutionOptions(priority, maxRetries, partitions);
        }
    }
}
