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

package org.apache.ignite.internal.properties;

import java.io.Serializable;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.internal.util.StringUtils;

/**
 * Class representing an Ignite version.
 */
public class IgniteProductVersion implements Serializable {
    private static final Pattern VERSION_PATTERN =
            Pattern.compile("(?<major>\\d+)\\.(?<minor>\\d+)\\.(?<maintenance>\\d+)(?<snapshot>-SNAPSHOT)?");

    /**
     * Version of the current node.
     */
    public static final IgniteProductVersion CURRENT_VERSION = fromString(IgniteProperties.get(IgniteProperties.VERSION));

    /** Major version number. */
    private final byte major;

    /** Minor version number. */
    private final byte minor;

    /** Maintenance version number. */
    private final byte maintenance;

    /** Flag indicating if this is a snapshot release. */
    private final boolean isSnapshot;

    private IgniteProductVersion(byte major, byte minor, byte maintenance, boolean isSnapshot) {
        this.major = major;
        this.minor = minor;
        this.maintenance = maintenance;
        this.isSnapshot = isSnapshot;
    }

    /**
     * Parses Ignite version in either {@code "X.X.X-SNAPSHOT"} or {@code "X.X.X"} formats.
     *
     * @param versionStr String representation of an Ignite version.
     * @return Parsed Ignite version.
     * @throws IllegalArgumentException If the given string is empty or does not match the required format.
     */
    public static IgniteProductVersion fromString(String versionStr) {
        if (StringUtils.nullOrBlank(versionStr)) {
            throw new IllegalArgumentException("Ignite version is empty");
        }

        Matcher matcher = VERSION_PATTERN.matcher(versionStr);

        if (!matcher.matches()) {
            throw new IllegalArgumentException("Unexpected Ignite version format: " + versionStr);
        }

        return new IgniteProductVersion(
                Byte.parseByte(matcher.group("major")),
                Byte.parseByte(matcher.group("minor")),
                Byte.parseByte(matcher.group("maintenance")),
                matcher.group("snapshot") != null
        );
    }

    public byte major() {
        return major;
    }

    public byte minor() {
        return minor;
    }

    public byte maintenance() {
        return maintenance;
    }

    public boolean snapshot() {
        return isSnapshot;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IgniteProductVersion that = (IgniteProductVersion) o;
        return major == that.major && minor == that.minor && maintenance == that.maintenance && isSnapshot == that.isSnapshot;
    }

    @Override
    public int hashCode() {
        return Objects.hash(major, minor, maintenance, isSnapshot);
    }

    @Override
    public String toString() {
        String version = String.join(".", String.valueOf(major), String.valueOf(minor), String.valueOf(maintenance));

        return version + (isSnapshot ? "-SNAPSHOT" : "");
    }
}
