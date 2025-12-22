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

import static io.swagger.v3.oas.annotations.media.Schema.RequiredMode.REQUIRED;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.ignite.internal.tostring.S;

/**
 * DTO of unit version and status.
 */
@Schema(description = "Unit version and status.")
public class UnitVersionStatus {
    @Schema(description = "Unit version.", requiredMode = REQUIRED)
    private final String version;

    @Schema(description = "Unit status.", requiredMode = REQUIRED)
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
    public final boolean equals(Object o) {
        if (!(o instanceof UnitVersionStatus)) {
            return false;
        }

        UnitVersionStatus that = (UnitVersionStatus) o;
        return version.equals(that.version) && status == that.status;
    }

    @Override
    public int hashCode() {
        int result = version.hashCode();
        result = 31 * result + status.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
