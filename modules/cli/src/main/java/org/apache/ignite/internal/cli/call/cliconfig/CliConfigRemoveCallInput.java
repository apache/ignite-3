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

/** Input for {@link CliConfigRemoveCall}. */
public class CliConfigRemoveCallInput implements CallInput {
    private final String key;

    private final String profileName;

    private CliConfigRemoveCallInput(String key, String profileName) {
        this.key = key;
        this.profileName = profileName;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String key() {
        return key;
    }

    public String profileName() {
        return profileName;
    }

    /** Builder of {@link CliConfigRemoveCallInput}. */
    public static class Builder {
        private String key;
        private String profileName;

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        public Builder profileName(String profileName) {
            this.profileName = profileName;
            return this;
        }

        public CliConfigRemoveCallInput build() {
            return new CliConfigRemoveCallInput(key, profileName);
        }
    }
}
