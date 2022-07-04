/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cluster.management.rest;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * REST representation of {@link org.apache.ignite.internal.properties.IgniteProductVersion}.
 */
@Schema(name = "IgniteProductVersion")
class IgniteProductVersionDto {
    /** Major version number. */
    private final byte major;

    /** Minor version number. */
    private final byte minor;

    /** Maintenance version number. */
    private final byte maintenance;

    /** Flag indicating if this is a snapshot release. */
    private final boolean isSnapshot;

    /** Alpha version part or an empty string if this is not an alpha release. */
    // TODO: IGNITE-17146 Fix and add support for beta and other releases
    private final String alphaVersion;

    IgniteProductVersionDto(byte major, byte minor, byte maintenance, boolean isSnapshot, @Nullable String alphaVersion) {
        this.major = major;
        this.minor = minor;
        this.maintenance = maintenance;
        this.isSnapshot = isSnapshot;
        this.alphaVersion = alphaVersion;
    }

    /**
     * Returns the major version number.
     */
    public byte major() {
        return major;
    }

    /**
     * Returns the minor version number.
     */
    public byte minor() {
        return minor;
    }

    /**
     * Returns the maintenance version number.
     */
    public byte maintenance() {
        return maintenance;
    }

    /**
     * Returns {@code true} if this is a snapshot release, {@code false} otherwise.
     */
    public boolean snapshot() {
        return isSnapshot;
    }

    /**
     * Returns the alpha version of this release or an empty string if this is not an alpha release.
     */
    public String alphaVersion() {
        return alphaVersion;
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
        return major == that.major && minor == that.minor && maintenance == that.maintenance && isSnapshot == that.isSnapshot
                && alphaVersion.equals(that.alphaVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(major, minor, maintenance, isSnapshot, alphaVersion);
    }

    @Override
    public String toString() {
        String version = String.join(".", String.valueOf(major), String.valueOf(minor), String.valueOf(maintenance));

        return version + (alphaVersion.isEmpty() ? "" : "-" + alphaVersion) + (isSnapshot ? "-SNAPSHOT" : "");
    }
}
