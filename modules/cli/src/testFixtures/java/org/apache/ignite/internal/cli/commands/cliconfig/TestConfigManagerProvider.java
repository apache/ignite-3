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

package org.apache.ignite.internal.cli.commands.cliconfig;

import io.micronaut.context.annotation.Replaces;
import jakarta.inject.Singleton;
import java.io.File;
import org.apache.ignite.internal.cli.config.ConfigManager;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.config.ini.IniConfigManager;

/**
 * Test implementation of {@link ConfigManagerProvider}.
 */
@Singleton
@Replaces(ConfigManagerProvider.class)
public class TestConfigManagerProvider implements ConfigManagerProvider {

    public ConfigManager configManager = new IniConfigManager(
            TestConfigManagerHelper.createIntegrationTestsConfig(),
            TestConfigManagerHelper.createEmptySecretConfig()
    );

    @Override
    public ConfigManager get() {
        return configManager;
    }

    public void setConfigFile(File configFile) {
        setConfigFile(configFile, TestConfigManagerHelper.createEmptySecretConfig());
    }

    public void setConfigFile(File configFile, File secretConfigFile) {
        configManager = new IniConfigManager(configFile, secretConfigFile);
    }
}
