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

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.TaskState;
import org.apache.ignite.compute.TaskStatus;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Task state implementation.
 */
public class TaskStateImpl implements TaskState {
    private static final long serialVersionUID = 8575969461073736006L;

    /**
     * Job ID.
     */
    private final UUID id;

    /**
     * Job status.
     */
    private final TaskStatus status;

    /**
     * Job create time.
     */
    private final Instant createTime;

    /**
     * Job start time.
     */
    @Nullable
    private final Instant startTime;

    /**
     * Job finish time.
     */
    @Nullable
    private final Instant finishTime;

    private TaskStateImpl(Builder builder) {
        this.id = Objects.requireNonNull(builder.id, "id");
        this.status = Objects.requireNonNull(builder.status, "status");
        this.createTime = Objects.requireNonNull(builder.createTime, "createTime");
        this.startTime = builder.startTime;
        this.finishTime = builder.finishTime;
    }

    /**
     * Creates a new builder.
     *
     * @return Builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns job ID.
     *
     * @return Job ID.
     */
    @Override
    public UUID id() {
        return id;
    }

    /**
     * Returns job status.
     *
     * @return Job status.
     */
    @Override
    public TaskStatus status() {
        return status;
    }

    /**
     * Returns job create time.
     *
     * @return Job create time.
     */
    @Override
    public Instant createTime() {
        return createTime;
    }

    /**
     * Returns job start time. {@code null} if the job has not started yet.
     *
     * @return Job start time. {@code null} if the job has not started yet.
     */
    @Nullable
    @Override
    public Instant startTime() {
        return startTime;
    }

    /**
     * Returns job finish time. {@code null} if the job has not finished yet.
     *
     * @return Job finish time. {@code null} if the job has not finished yet.
     */
    @Nullable
    @Override
    public Instant finishTime() {
        return finishTime;
    }

    /**
     * Returns a new builder with the same property values as this TaskState.
     *
     * @return Builder.
     */
    public static Builder toBuilder(TaskState state) {
        return new Builder(state);
    }

    /**
     * Returns a new builder with the same property values as this JobState.
     *
     * @return Builder.
     */
    public static Builder toBuilder(JobState state) {
        return new Builder()
                .id(state.id())
                .createTime(state.createTime())
                .finishTime(state.finishTime())
                .startTime(state.startTime())
                .status(JobTaskStatusMapper.toTaskStatus(state.status()));
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Builder.
     */
    public static class Builder {
        private UUID id;
        private TaskStatus status;
        private Instant createTime;
        @Nullable
        private Instant startTime;
        @Nullable
        private Instant finishTime;

        /**
         * Constructor.
         */
        private Builder() {
        }

        /**
         * Constructor.
         *
         * @param state Job state for copy.
         */
        private Builder(TaskState state) {
            this.id = state.id();
            this.status = state.status();
            this.createTime = state.createTime();
            this.startTime = state.startTime();
            this.finishTime = state.finishTime();
        }

        /**
         * Sets job ID.
         *
         * @param id Job ID.
         * @return This builder.
         */
        public Builder id(UUID id) {
            this.id = id;
            return this;
        }

        /**
         * Sets job status.
         *
         * @param status Job status.
         * @return This builder.
         */
        public Builder status(TaskStatus status) {
            this.status = status;
            return this;
        }

        /**
         * Sets job create time.
         *
         * @param createTime Job create time.
         * @return This builder.
         */
        public Builder createTime(Instant createTime) {
            this.createTime = createTime;
            return this;
        }

        /**
         * Sets job start time.
         *
         * @param startTime Job start time.
         * @return This builder.
         */
        public Builder startTime(@Nullable Instant startTime) {
            this.startTime = startTime;
            return this;
        }

        /**
         * Sets job finish time.
         *
         * @param finishTime Job finish time.
         * @return This builder.
         */
        public Builder finishTime(@Nullable Instant finishTime) {
            this.finishTime = finishTime;
            return this;
        }

        /**
         * Builds a new JobState.
         *
         * @return JobState.
         */
        public TaskStateImpl build() {
            return new TaskStateImpl(this);
        }
    }
}
