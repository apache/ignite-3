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

import java.util.Map;
import org.apache.ignite.internal.cli.core.call.CallInput;

/**
 * Input for {@link CliConfigSetCall}.
 */
public class CliConfigSetCallInput implements CallInput {
    private final Map<String, String> parameters;

    private final String profileName;

    private CliConfigSetCallInput(Map<String, String> parameters, String profileName) {
        this.parameters = parameters;
        this.profileName = profileName;
    }

    public static CliConfigSetCallInputBuilder builder() {
        return new CliConfigSetCallInputBuilder();
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public String getProfileName() {
        return profileName;
    }

    /**
     * Builder of {@link CliConfigSetCallInput}.
     */
    public static class CliConfigSetCallInputBuilder {
        private Map<String, String> parameters;
        private String profileName;

        public CliConfigSetCallInputBuilder parameters(Map<String, String> parameters) {
            this.parameters = parameters;
            return this;
        }

        public CliConfigSetCallInputBuilder profileName(String profileName) {
            this.profileName = profileName;
            return this;
        }

        public CliConfigSetCallInput build() {
            return new CliConfigSetCallInput(parameters, profileName);
        }
    }
}
