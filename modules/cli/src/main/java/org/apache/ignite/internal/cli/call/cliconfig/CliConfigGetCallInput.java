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

package org.apache.ignite.internal.cli.call.cliconfig;

import org.apache.ignite.internal.cli.core.call.CallInput;

/**
 * Input for {@link CliConfigGetCall}.
 */
public class CliConfigGetCallInput implements CallInput {
    private final String key;

    private final String profileName;

    private CliConfigGetCallInput(String key, String profileName) {
        this.key = key;
        this.profileName = profileName;
    }

    public static CliConfigGetCallInputBuilder builder() {
        return new CliConfigGetCallInputBuilder();
    }

    public String getKey() {
        return key;
    }

    public String getProfileName() {
        return profileName;
    }

    /**
     * Builder of {@link CliConfigGetCallInput}.
     */

    public static class CliConfigGetCallInputBuilder {
        private String key;
        private String profileName;

        public CliConfigGetCallInputBuilder key(String key) {
            this.key = key;
            return this;
        }

        public CliConfigGetCallInputBuilder profileName(String profileName) {
            this.profileName = profileName;
            return this;
        }

        public CliConfigGetCallInput build() {
            return new CliConfigGetCallInput(key, profileName);
        }
    }
}
