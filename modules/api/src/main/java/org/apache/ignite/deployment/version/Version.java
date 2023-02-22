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


/**
 * Unit version interface. Version implementations should be comparable.
 */
public interface Version extends Comparable<Version> {
    /**
     * Render version representation in String format.
     *
     * @return version string representation.
     */
    String render();

    Version LATEST = new Version() {
        @Override
        public String render() {
            return "latest";
        }

        @Override
        public int compareTo(Version o) {
            if (o == LATEST) {
                return 0;
            }
            return 1;
        }

        @Override
        public boolean equals(Object obj) {
            return this == LATEST;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    };

    /**
     * Parse version from String.
     *
     * @param s string version representation.
     * @return Version instance of
     */
    static Version parseVersion(String s) {
        try {
            return UnitVersion.parse(s);
        } catch (VersionParseException ignored) {
            //No-op
        }

        if (!"latest".equals(s)) {
            throw new VersionParseException();
        }
        return LATEST;
    }
}
