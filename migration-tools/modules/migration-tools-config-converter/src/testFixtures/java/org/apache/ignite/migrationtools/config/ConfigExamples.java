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

package org.apache.ignite.migrationtools.config;

import java.util.stream.Stream;

/** Defines the location of all the configuration examples used in the tests. */
public class ConfigExamples {
    private ConfigExamples() {
        // Intentionally left blank.
    }

    /**
     * Returns all the know paths of the example configurations used in the tests.
     *
     * @return The configurations paths.
     */
    public static Stream<String> configPaths() {
        return Stream.of(
                "configs-custom/ignite-config.0.xml"
                // TODO: Uncomment the following after fixing IGNITE-27378
                // "configs-custom/ignite-config.1.xml",
                // "configs-custom/ignite-config.2.xml"
        );
    }
}
