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

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.config.ConfigManager;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.config.Profile;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;

/**
 * Create CLI profile.
 */
@Singleton
public class CliConfigProfileCreateCall implements Call<CliConfigProfileCreateCallInput, String> {

    @Inject
    private ConfigManagerProvider configManagerProvider;

    @Override
    public CallOutput<String> execute(CliConfigProfileCreateCallInput input) {
        ConfigManager configManager = configManagerProvider.get();
        Profile copyFrom = null;
        if (input.getCopyFrom() != null) {
            copyFrom = configManager.getProfile(input.getCopyFrom());
        }

        String profileName = input.getName();
        Profile newProfile = configManager.createProfile(profileName);

        if (copyFrom != null) {
            newProfile.setProperties(copyFrom.getAll());
        }

        if (input.isActivate()) {
            configManager.setCurrentProfile(profileName);
        }
        return DefaultCallOutput.success("Profile " + profileName + " was created successfully.");
    }
}
