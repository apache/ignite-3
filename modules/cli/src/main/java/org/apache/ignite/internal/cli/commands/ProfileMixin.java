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

package org.apache.ignite.internal.cli.commands;

import static org.apache.ignite.internal.cli.commands.CommandConstants.PROFILE_OPTION_ORDER;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PROFILE_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PROFILE_OPTION_DESC;

import picocli.CommandLine.Option;

/**
 * Mixin for profile option.
 */
public class ProfileMixin {
    @Option(names = PROFILE_OPTION, description = PROFILE_OPTION_DESC, order = PROFILE_OPTION_ORDER)
    private String profileName;

    /**
     * Gets profile name.
     *
     * @return profile name
     */
    public String getProfileName() {
        return profileName;
    }
}
