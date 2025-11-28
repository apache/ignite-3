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

package org.apache.ignite.deployment.version;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.lang.SemanticVersion;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link Version} interface based on the relaxed semantic versioning - minor and maintenance can be omitted.
 */
class UnitVersion implements Version {
    /**
     * This pattern allows optional minor and maintenance to maintain compatibility with previous versions.
     */
    private static final Pattern VERSION_PATTERN = Pattern.compile(
            "(?<major>\\d+)(?:\\.(?<minor>\\d+))?(?:\\.(?<maintenance>\\d+))?(?:\\.(?<patch>\\d+))?(?:-(?<preRelease>[0-9A-Za-z]+))?"
    );

    private final SemanticVersion version;

    /**
     * Constructor.
     *
     * @param major Major part of version.
     * @param minor Minor part of version.
     * @param maintenance Maintenance part of version.
     */
    UnitVersion(byte major, byte minor, byte maintenance, @Nullable Byte patch, @Nullable String preRelease) {
        this.version = new SemanticVersion(major, minor, maintenance, patch, preRelease);
    }

    @Override
    public String render() {
        return version.toString();
    }

    /**
     * Parse string representation of version to {@link UnitVersion} if possible.
     *
     * @param rawVersion String representation of version.
     * @return Instance of {@link UnitVersion}.
     * @throws VersionParseException in case when string is not required {@link UnitVersion} format.
     */
    public static UnitVersion parse(String rawVersion) {
        Objects.requireNonNull(rawVersion);
        try {
            Matcher matcher = VERSION_PATTERN.matcher(rawVersion);

            if (!matcher.matches()) {
                throw new VersionParseException(rawVersion, "Invalid version format");
            }

            String minor = matcher.group("minor");
            String maintenance = matcher.group("maintenance");
            String patch = matcher.group("patch");
            String preRelease = matcher.group("preRelease");

            return new UnitVersion(
                    Byte.parseByte(matcher.group("major")),
                    nullOrBlank(minor) ? 0 : Byte.parseByte(minor),
                    nullOrBlank(maintenance) ? 0 : Byte.parseByte(maintenance),
                    nullOrBlank(patch) ? null : Byte.parseByte(patch),
                    nullOrBlank(preRelease) ? null : preRelease
            );
        } catch (NumberFormatException e) {
            throw new VersionParseException(rawVersion, e);
        }
    }

    private static boolean nullOrBlank(String str) {
        return str == null || str.isBlank();
    }

    @Override
    public int compareTo(Version o) {
        if (o == LATEST) {
            return -1;
        }

        UnitVersion unitVersion = (UnitVersion) o;
        return version.compareTo(unitVersion.version);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UnitVersion unitVersion = (UnitVersion) o;
        return version.equals(unitVersion.version);
    }

    @Override
    public int hashCode() {
        return version.hashCode();
    }

    @Override
    public String toString() {
        return render();
    }
}
