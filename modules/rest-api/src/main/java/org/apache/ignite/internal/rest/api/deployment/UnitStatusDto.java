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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.ignite.deployment.UnitStatus;
import org.apache.ignite.deployment.version.Version;

/**
 * DTO of {@link UnitStatus}.
 */
@Schema(name = "UnitStatus", description = "Unit status.")
public class UnitStatusDto {

    /**
     * Unit identifier.
     */
    private final String id;

    /**
     * Map from existing unit version to list of nodes consistent ids where unit deployed.
     */
    private final Map<String, List<String>> versionToConsistentIds;

    @JsonCreator
    public UnitStatusDto(@JsonProperty("id") String id,
            @JsonProperty("versionToNodes") Map<String, List<String>> versionToConsistentIds) {
        this.id = id;
        this.versionToConsistentIds = versionToConsistentIds;
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
     * Returns map from existing unit version to list of nodes consistent ids where unit deployed.
     *
     * @return Map from existing unit version to list of nodes consistent ids where unit deployed.
     */
    @JsonGetter("versionToNodes")
    public Map<String, List<String>> versionToConsistentIds() {
        return versionToConsistentIds;
    }


    /**
     * Mapper method.
     *
     * @param status Unit status.
     * @return Unit status DTO.
     */
    public static UnitStatusDto fromUnitStatus(UnitStatus status) {
        Map<String, List<String>> versionToConsistentIds = new HashMap<>();
        Set<Version> versions = status.versions();
        for (Version version : versions) {
            versionToConsistentIds.put(version.render(), status.consistentIds(version));
        }
        return new UnitStatusDto(status.id(), versionToConsistentIds);
    }

}
