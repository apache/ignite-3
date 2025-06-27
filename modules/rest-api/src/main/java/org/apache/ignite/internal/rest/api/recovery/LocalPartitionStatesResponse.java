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

package org.apache.ignite.internal.rest.api.recovery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;

/**
 * Local partition states schema class.
 */
@Schema(description = "Information about local partition states.")
public class LocalPartitionStatesResponse {
    // Using JsonInclude to handle empty list correctly.
    @Schema
    @JsonInclude
    private final List<LocalPartitionStateResponse> states;

    @JsonCreator
    public LocalPartitionStatesResponse(@JsonProperty("states") List<LocalPartitionStateResponse> states) {
        this.states = List.copyOf(states);
    }

    @JsonGetter("states")
    public List<LocalPartitionStateResponse> states() {
        return states;
    }
}
