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

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Job status.
 */
public class JobStatus implements Serializable {
    private static final long serialVersionUID = 8575969461073736006L;

    /**
     * Job ID.
     */
    private final UUID id;

    /**
     * Job state.
     */
    private final JobState state;

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

    private JobStatus(Builder builder) {
        this.id = Objects.requireNonNull(builder.id, "id");
        this.state = Objects.requireNonNull(builder.state, "state");
        this.createTime = Objects.requireNonNull(builder.createTime, "createTime");
        this.startTime = builder.startTime;
        this.finishTime = builder.finishTime;
    }

    public static Builder builder() {
        return new Builder();
    }

    public UUID id() {
        return id;
    }

    public JobState state() {
        return state;
    }

    public Instant createTime() {
        return createTime;
    }

    @Nullable
    public Instant startTime() {
        return startTime;
    }

    @Nullable
    public Instant finishTime() {
        return finishTime;
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    /**
     * Builder.
     */
    public static class Builder {
        private UUID id;
        private JobState state;
        private Instant createTime;
        @Nullable
        private Instant startTime;
        @Nullable
        private Instant finishTime;

        public Builder() {
        }

        private Builder(JobStatus status) {
            this.id = status.id;
            this.state = status.state;
            this.createTime = status.createTime;
            this.startTime = status.startTime;
            this.finishTime = status.finishTime;
        }

        public Builder id(UUID id) {
            this.id = id;
            return this;
        }

        public Builder state(JobState state) {
            this.state = state;
            return this;
        }

        public Builder createTime(Instant createTime) {
            this.createTime = createTime;
            return this;
        }

        public Builder startTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder finishTime(Instant finishTime) {
            this.finishTime = finishTime;
            return this;
        }

        public JobStatus build() {
            return new JobStatus(this);
        }
    }
}

