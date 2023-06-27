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

/**
 * DTO of unit version and status.
 */
@Schema(description = "Unit version and status.")
public class UnitVersionStatus {
    @Schema(description = "Unit version.", requiredMode = RequiredMode.REQUIRED)
    private final String version;

    @Schema(description = "Unit status.", requiredMode = RequiredMode.REQUIRED)
    private final DeploymentStatus status;

    @JsonCreator
    public UnitVersionStatus(@JsonProperty("version") String version, @JsonProperty("status") DeploymentStatus status) {
        this.version = version;
        this.status = status;
    }

    @JsonGetter("version")
    public String getVersion() {
        return version;
    }

    @JsonGetter("status")
    public DeploymentStatus getStatus() {
        return status;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UnitVersionStatus that = (UnitVersionStatus) o;

        if (version != null ? !version.equals(that.version) : that.version != null) {
            return false;
        }
        return status == that.status;
    }

    @Override
    public int hashCode() {
        int result = version != null ? version.hashCode() : 0;
        result = 31 * result + (status != null ? status.hashCode() : 0);
        return result;
    }
}
