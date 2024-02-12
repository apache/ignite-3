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
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema.RequiredMode;

/**
 * DTO of update job priority request body.
 */
@Schema(name = "UpdateJobPriorityBody")
public class UpdateJobPriorityBody {
    /**
     * Priority.
     */
    @Schema(description = "Priority.", requiredMode = RequiredMode.REQUIRED)
    private final int priority;

    @JsonCreator
    public UpdateJobPriorityBody(@JsonProperty("priority") int priority) {
        this.priority = priority;
    }

    @JsonGetter("priority")
    public int priority() {
        return priority;
    }
}
