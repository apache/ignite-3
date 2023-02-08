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
import org.jetbrains.annotations.NotNull;

/**
 * Implementation of {@link Version} interface based on the three numbers format,
 * like x.x.x. where x is short number.
 */
public class UnitVersion implements Version {
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
    public UnitVersion(short major, short minor, short patch) {
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
     * @param s String representation of version.
     * @return Instance of {@link UnitVersion}.
     * @throws VersionParseException in case when string is not required {@link UnitVersion} format.
     */
    public static UnitVersion parse(String s) {
        try {
            String[] split = s.split("\\.");
            if (split.length > 3 || split.length == 0) {
                throw new VersionParseException("Invalid version format");
            }

            short major = Short.parseShort(split[0]);
            short minor = split.length > 1 ? Short.parseShort(split[1]) : 0;
            short patch = split.length > 2 ? Short.parseShort(split[2]) : 0;

            return new UnitVersion(major, minor, patch);
        } catch (NumberFormatException e) {
            throw new VersionParseException(e);
        }
    }

    @Override
    public int compareTo(@NotNull Version o) {
        if (o == LATEST) {
            return -1;
        }

        UnitVersion version = (UnitVersion) o;

        if (version.major != major) {
            return major - version.major;
        }

        if (version.minor != minor) {
            return minor - version.minor;
        }

        return patch - version.patch;
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
        return major == version.major && minor == version.minor && patch == version.patch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(major, minor, patch);
    }

    @Override
    public String toString() {
        return render();
    }
}
