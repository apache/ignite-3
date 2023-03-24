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
 * Unit version interface. Version implementations must be comparable.
 */
public interface Version extends Comparable<Version> {
    /**
     * Renders a version representation in the String format.
     *
     * @return version String representation.
     */
    String render();

    /**
     * Implementation of the {@link Version} interface with the special latest logic.
     * This version has a special unique representation. Moreover, by convention,
     * this implementation must be the oldest version of any another independent of implementation.
     */
    //I don't understand the explanations above.
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
        public String toString() {
            return render();
        }
    };

    /**
     * Parses a version from a String representation.
     *
     * @param s String version representation.
     * @return Version instance.
     */
    static Version parseVersion(String s) {
        if ("latest".equals(s)) {
            return LATEST;
        }

        return UnitVersion.parse(s);
    }
}
