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

/**
 * Implementation of {@link Version} interface based on the three numbers format, like x.x.x. where x is short number.
 */
class UnitVersion implements Version {
    private final short major;

    private final short minor;

    private final short patch;

    /**
     * Constructor.
     *
     * @param major Major part of version.
     * @param minor Minor part of version.
     * @param patch Patch part of version.
     */
    UnitVersion(short major, short minor, short patch) {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
    }

    @Override
    public String render() {
        return major + "." + minor + "." + patch;
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
            String[] split = rawVersion.split("\\.", -1);
            if (split.length > 3 || split.length == 0) {
                throw new VersionParseException(rawVersion, "Invalid version format");
            }

            short major = Short.parseShort(split[0]);
            short minor = split.length > 1 ? Short.parseShort(split[1]) : 0;
            short patch = split.length > 2 ? Short.parseShort(split[2]) : 0;

            return new UnitVersion(major, minor, patch);
        } catch (NumberFormatException e) {
            throw new VersionParseException(rawVersion, e);
        }
    }

    @Override
    public int compareTo(Version o) {
        if (o == LATEST) {
            return -1;
        }

        UnitVersion version = (UnitVersion) o;

        int majorCompare = Short.compare(major, version.major);
        if (majorCompare != 0) {
            return majorCompare;
        }

        int minorCompare = Short.compare(minor, version.minor);
        if (minorCompare != 0) {
            return minorCompare;
        }

        return Short.compare(patch, version.patch);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UnitVersion version = (UnitVersion) o;

        if (major != version.major) {
            return false;
        }
        if (minor != version.minor) {
            return false;
        }
        return patch == version.patch;
    }

    @Override
    public int hashCode() {
        int result = major;
        result = 31 * result + minor;
        result = 31 * result + patch;
        return result;
    }

    @Override
    public String toString() {
        return render();
    }
}
