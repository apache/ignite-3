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

package org.apache.ignite.internal.cli.call.cliconfig.profile;

import org.apache.ignite.internal.cli.core.call.CallInput;

/**
 * Create profile call parameters.
 */
public class CliConfigProfileCreateCallInput implements CallInput {
    private final String name;

    private final String copyFrom;

    private final boolean activate;

    private CliConfigProfileCreateCallInput(String name, String copyFrom, boolean activate) {
        this.name = name;
        this.copyFrom = copyFrom;
        this.activate = activate;
    }

    public static CliConfigProfileCreateCallInputBuilder builder() {
        return new CliConfigProfileCreateCallInputBuilder();
    }

    public String getName() {
        return name;
    }

    public String getCopyFrom() {
        return copyFrom;
    }

    public boolean isActivate() {
        return activate;
    }

    /**
     * Builder of {@link CliConfigProfileCreateCallInput}.
     */
    public static class CliConfigProfileCreateCallInputBuilder {
        private String name;

        private String copyFrom;

        private boolean activate;

        public CliConfigProfileCreateCallInputBuilder profileName(String name) {
            this.name = name;
            return this;
        }

        public CliConfigProfileCreateCallInputBuilder copyFrom(String copyFrom) {
            this.copyFrom = copyFrom;
            return this;
        }

        public CliConfigProfileCreateCallInputBuilder activate(boolean activate) {
            this.activate = activate;
            return this;
        }

        public CliConfigProfileCreateCallInput build() {
            return new CliConfigProfileCreateCallInput(name, copyFrom, activate);
        }
    }

}
