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

package org.apache.ignite.lang;

import java.util.Objects;
import java.util.StringJoiner;
import org.jetbrains.annotations.Nullable;

/**
 * Class representing a <a href="https://semver.org/spec/v2.0.0.html">semantic version</a>.
 */
public class SemanticVersion implements Comparable<SemanticVersion> {
    /** Major version number. */
    private final byte major;

    /** Minor version number. */
    private final byte minor;

    /** Maintenance version number. */
    private final byte maintenance;

    /** Patch version number. */
    @Nullable
    private final Byte patch;

    /** Pre-release version. */
    @Nullable
    private final String preRelease;

    /** Constructor. */
    public SemanticVersion(byte major, byte minor, byte maintenance, @Nullable Byte patch, @Nullable String preRelease) {
        this.major = major;
        this.minor = minor;
        this.maintenance = maintenance;
        this.patch = patch;
        this.preRelease = preRelease;
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
     * Returns the patch version number, {@code null} if no patch version has been specified.
     */
    public @Nullable Byte patch() {
        return patch;
    }

    /**
     * Returns the pre-release version, {@code null} if no pre-release version has been specified.
     */
    public @Nullable String preRelease() {
        return preRelease;
    }

    @Override
    public int compareTo(SemanticVersion other) {
        int res;

        // Compare major, minor, maintenance
        res = Byte.compare(major(), other.major());
        if (res != 0) {
            return res;
        }

        res = Byte.compare(minor(), other.minor());
        if (res != 0) {
            return res;
        }

        res = Byte.compare(maintenance(), other.maintenance());
        if (res != 0) {
            return res;
        }

        // Compare patch (nullable)
        res = compareNullable(patch(), other.patch());
        if (res != 0) {
            return res;
        }

        // Compare pre-release order (nullable)
        res = compareNullable(preReleaseOrder(preRelease()), preReleaseOrder(other.preRelease()));
        return res;
    }

    private static int compareNullable(@Nullable Byte a, @Nullable Byte b) {
        if (a != null && b != null) {
            return Byte.compare(a, b);
        } else if (a != null) {
            return 1;
        } else if (b != null) {
            return -1;
        }
        return 0;
    }

    @Nullable
    private static Byte preReleaseOrder(@Nullable String preRelease) {
        if (preRelease == null) {
            return null;
        }
        switch (preRelease.toLowerCase()) {
            case "alpha":
                return 0;
            case "beta":
                return 1;
            case "rc":
                return 2;
            case "final":
            case "":
                return 3;
            default:
                return 4; // Unknown or custom stages
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SemanticVersion that = (SemanticVersion) o;

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
