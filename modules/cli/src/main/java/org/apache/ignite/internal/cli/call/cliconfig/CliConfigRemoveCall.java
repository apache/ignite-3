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

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.config.ConfigManager;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.config.exception.NonexistentPropertyException;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;

/**
 * Removes CLI configuration parameter.
 */
@Singleton
public class CliConfigRemoveCall implements Call<CliConfigRemoveCallInput, String> {
    @Inject
    private ConfigManagerProvider configManagerProvider;

    @Override
    public DefaultCallOutput<String> execute(CliConfigRemoveCallInput input) {
        ConfigManager configManager = configManagerProvider.get();
        String key = input.key();
        String profileName = input.profileName();
        if (configManager.removeProperty(key, profileName) != null) {
            return DefaultCallOutput.empty();
        }
        return DefaultCallOutput.failure(new NonexistentPropertyException(key));
    }
}
