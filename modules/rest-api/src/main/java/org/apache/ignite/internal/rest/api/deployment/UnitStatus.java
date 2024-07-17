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

package org.apache.ignite.internal.rest.api.deployment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema.RequiredMode;
import java.util.List;

/**
 * DTO of unit status.
 */
@Schema(description = "Unit status.")
public class UnitStatus {
    /**
     * Unit identifier.
     */
    @Schema(description = "Unit identifier.",
            requiredMode = RequiredMode.REQUIRED)
    private final String id;

    /**
     * Map from existing unit version to list of nodes consistent ids where unit deployed.
     */
    @Schema(description = "Map from unit version to unit deployment status.",
            requiredMode = RequiredMode.REQUIRED)
    private final List<UnitVersionStatus> versionToStatus;

    @JsonCreator
    public UnitStatus(@JsonProperty("id") String id,
            @JsonProperty("versionToStatus") List<UnitVersionStatus> versionToStatus) {
        this.id = id;
        this.versionToStatus = versionToStatus;
    }

    /**
     * Returns unit identifier.
     *
     * @return Unit identifier.
     */
    @JsonGetter("id")
    public String id() {
        return id;
    }

    /**
     * Returns the map of existing unit versions mapped to the list of node consistent ids where these units are deployed.
     *
     * @return Map from existing unit version to list of nodes consistent ids where unit deployed.
     */
    @JsonGetter("versionToStatus")
    public List<UnitVersionStatus> versionToStatus() {
        return versionToStatus;
    }
}
