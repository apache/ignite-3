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

package org.apache.ignite.internal.rest.api.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Objects;
import java.util.StringJoiner;
import org.jetbrains.annotations.Nullable;

/**
 * REST representation of {@link org.apache.ignite.internal.properties.IgniteProductVersion}.
 */
@Schema(name = "IgniteProductVersion")
public class IgniteProductVersionDto {
    /** Major version number. */
    private final short major;

    /** Minor version number. */
    private final short minor;

    /** Maintenance version number. */
    private final short maintenance;

    /** Patch version number. */
    @Nullable
    private final Byte patch;

    /** Pre-release version. */
    @Nullable
    private final String preRelease;

    /** Constructor. */
    @JsonCreator
    public IgniteProductVersionDto(
            @JsonProperty("major") byte major,
            @JsonProperty("minor") byte minor,
            @JsonProperty("maintenance") byte maintenance,
            @JsonProperty("patch") @Nullable Byte patch,
            @JsonProperty("preRelease") @Nullable String preRelease
    ) {
        this.major = major;
        this.minor = minor;
        this.maintenance = maintenance;
        this.patch = patch;
        this.preRelease = preRelease;
    }

    /**
     * Returns the major version number.
     */
    @JsonGetter
    public short major() {
        return major;
    }

    /**
     * Returns the minor version number.
     */
    @JsonGetter
    public short minor() {
        return minor;
    }

    /**
     * Returns the maintenance version number.
     */
    @JsonGetter
    public short maintenance() {
        return maintenance;
    }

    /**
     * Returns the patch version number.
     */
    @JsonGetter
    public @Nullable Byte patch() {
        return patch;
    }

    /**
     * Returns the pre-release version.
     */
    @JsonGetter
    public @Nullable String preRelease() {
        return preRelease;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IgniteProductVersionDto that = (IgniteProductVersionDto) o;

        return major == that.major && minor == that.minor && maintenance == that.maintenance
                && Objects.equals(patch, that.patch) && Objects.equals(preRelease, that.preRelease);
    }

    @Override
    public int hashCode() {
        return Objects.hash(major, minor, maintenance, patch, preRelease);
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(".").add(String.valueOf(major)).add(String.valueOf(minor)).add(String.valueOf(maintenance));

        if (patch != null) {
            joiner.add(patch.toString());
        }

        return joiner + (preRelease == null ? "" : "-" + preRelease);
    }
}
