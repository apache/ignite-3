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

package org.apache.ignite.internal.rest.api.compute;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema.RequiredMode;
import java.time.Instant;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Rest representation of {@link org.apache.ignite.compute.JobStatus}.
 */
@Schema(name = "JobStatus")
public class JobStatus {
    /**
     * Job ID.
     */
    @Schema(description = "Job ID.", requiredMode = RequiredMode.REQUIRED)
    private final UUID id;

    /**
     * Job state.
     */
    @Schema(description = "Job state.", requiredMode = RequiredMode.REQUIRED)
    private final JobState state;

    /**
     * Job create time.
     */
    @Schema(description = "Job create time.", requiredMode = RequiredMode.REQUIRED)
    private final Instant createTime;

    /**
     * Job start time.
     */
    @Schema(description = "Job start time.", requiredMode = RequiredMode.NOT_REQUIRED)
    @Nullable
    private final Instant startTime;

    /**
     * Job finish time.
     */
    @Schema(description = "Job finish time.", requiredMode = RequiredMode.NOT_REQUIRED)
    @Nullable
    private final Instant finishTime;

    /**
     * Constructor.
     *
     * @param id Job ID.
     * @param state Job state.
     * @param createTime Job create time.
     * @param startTime Job start time.
     * @param finishTime Job finish time.
     */
    @JsonCreator
    public JobStatus(
            @JsonProperty("id") UUID id,
            @JsonProperty("state") JobState state,
            @JsonProperty("createTime") Instant createTime,
            @JsonProperty("startTime") @Nullable Instant startTime,
            @JsonProperty("finishTime") @Nullable Instant finishTime
    ) {
        this.id = id;
        this.state = state;
        this.createTime = createTime;
        this.startTime = startTime;
        this.finishTime = finishTime;
    }

    @JsonProperty("id")
    public UUID id() {
        return id;
    }

    @JsonProperty("state")
    public JobState state() {
        return state;
    }

    @JsonProperty("createTime")
    public Instant createTime() {
        return createTime;
    }

    @Nullable
    @JsonProperty("startTime")
    public Instant startTime() {
        return startTime;
    }

    @Nullable
    @JsonProperty("finishTime")
    public Instant finishTime() {
        return finishTime;
    }
}
