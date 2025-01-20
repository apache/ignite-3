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

package org.apache.ignite.internal.rest.api.transaction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema.RequiredMode;
import java.time.Instant;
import java.util.UUID;

/**
 * Rest representation of transaction.
 */
@Schema(name = "Transaction")
public class Transaction {

    /**
     * Transaction ID.
     */
    @Schema(description = "Transaction ID.", requiredMode = RequiredMode.REQUIRED)
    private final UUID id;

    /**
     * Transaction state.
     */
    @Schema(description = "State.", requiredMode = RequiredMode.REQUIRED)
    private final String state;

    /**
     * Type.
     */
    @Schema(description = "Type.", requiredMode = RequiredMode.REQUIRED)
    private final String type;

    /**
     * Priority.
     */
    @Schema(description = "Priority.", requiredMode = RequiredMode.REQUIRED)
    private final String priority;

    /**
     * Start time.
     */
    @Schema(description = "Start time.", requiredMode = RequiredMode.REQUIRED)
    private final Instant startTime;


    /**
     * Constructor.
     *
     * @param id transaction id.
     * @param state transaction state.
     * @param type transaction type.
     * @param priority priority.
     * @param startTime start time.
     */
    @JsonCreator
    public Transaction(
            @JsonProperty("id") UUID id,
            @JsonProperty("state") String state,
            @JsonProperty("type") String type,
            @JsonProperty("priority") String priority,
            @JsonProperty("startTime") Instant startTime) {
        this.id = id;
        this.state = state;
        this.type = type;
        this.priority = priority;
        this.startTime = startTime;
    }

    @JsonProperty("id")
    public UUID id() {
        return id;
    }

    @JsonProperty("state")
    public String state() {
        return state;
    }

    @JsonProperty("type")
    public String type() {
        return type;
    }

    @JsonProperty("priority")
    public String priority() {
        return priority;
    }

    @JsonProperty("startTime")
    public Instant startTime() {
        return startTime;
    }
}
