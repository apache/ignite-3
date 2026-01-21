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

import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createEmptyConfig;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createEmptySecretConfig;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createEmptyWithCurrentProfileConfig;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createNonExistingSecretConfig;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createSectionWithDefaultProfileConfig;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createSectionWithoutDefaultProfileConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.File;
import org.apache.ignite.internal.cli.config.CliConfigKeys.Constants;
import org.apache.ignite.internal.cli.config.ini.IniConfigManager;
import org.junit.jupiter.api.Test;

class ConfigManagerTest {
    @Test
    public void testSaveLoadConfig() {
        File tempFile = createSectionWithDefaultProfileConfig();
        File tempSecretFile = createEmptySecretConfig();
        IniConfigManager configManager = new IniConfigManager(tempFile, tempSecretFile);

        configManager.setProperty("ignite.cluster-endpoint-url", "test");

        IniConfigManager configAfterSave = new IniConfigManager(tempFile, tempSecretFile);
        assertThat(configAfterSave.getCurrentProperty("ignite.cluster-endpoint-url")).isEqualTo("test");
    }

    @Test
    public void testLoadConfigWithoutDefaultProfile() {
        IniConfigManager configManager = new IniConfigManager(createSectionWithoutDefaultProfileConfig(), createEmptySecretConfig());

        assertThat(configManager.getCurrentProfile().getName()).isEqualTo("owner");
    }

    @Test
    public void testEmptyConfigLoad() {
        IniConfigManager configManager = new IniConfigManager(createEmptyConfig(), createEmptySecretConfig());

        assertThat(configManager.getCurrentProfile().getName()).isEqualTo("default");
        assertThat(configManager.getCurrentProperty(Constants.CLUSTER_URL)).isEqualTo("http://localhost:10300");
        assertThat(configManager.getCurrentProperty(Constants.BASIC_AUTHENTICATION_USERNAME)).isNull();
    }

    @Test
    public void testEmptyWithCurrentProfileConfigLoad() {
        IniConfigManager configManager = new IniConfigManager(createEmptyWithCurrentProfileConfig(), createEmptySecretConfig());

        assertThat(configManager.getCurrentProfile().getName()).isEqualTo("default");
        assertThat(configManager.getCurrentProperty(Constants.CLUSTER_URL)).isNull();
        assertThat(configManager.getCurrentProperty(Constants.BASIC_AUTHENTICATION_USERNAME)).isNull();
    }

    @Test
    public void testNonExistingSecretConfigLoad() {
        IniConfigManager configManager = new IniConfigManager(createEmptyConfig(), createNonExistingSecretConfig());

        assertThat(configManager.getCurrentProfile().getName()).isEqualTo("default");
        assertThat(configManager.getCurrentProperty(Constants.CLUSTER_URL)).isEqualTo("http://localhost:10300");

        String username = configManager.getCurrentProperty(Constants.BASIC_AUTHENTICATION_USERNAME);
        assertAll(
                () -> assertThat(username).isNotNull(),
                () -> assertThat(username).isBlank()
        );
    }

    @Test
    public void testRemoveConfigFileOnRuntime() {
        File tempFile = createSectionWithDefaultProfileConfig();
        File tempSecretFile = createEmptySecretConfig();
        IniConfigManager configManager = new IniConfigManager(tempFile, tempSecretFile);

        assertThat(configManager.getCurrentProfile().getName()).isEqualTo("database");

        assertThat(tempFile.delete()).isTrue();
        assertThat(tempFile.exists()).isFalse();

        assertThat(configManager.getCurrentProfile().getName()).isEqualTo("database");

        configManager.setProperty("test", "testValue");

        assertThat(tempFile.exists()).isTrue();
        assertThat(configManager.getCurrentProfile().getProperty("test")).isEqualTo("testValue");
        assertThat(configManager.getCurrentProfile().getName()).isEqualTo("database");
    }

    @Test
    public void testRemoveProperty() {
        File tempFile = createSectionWithDefaultProfileConfig();
        IniConfigManager configManager = new IniConfigManager(tempFile, createEmptySecretConfig());

        assertThat(configManager.removeProperty("server", configManager.getCurrentProfile().getName())).isEqualTo("127.0.0.1");

        assertThat(tempFile.exists()).isTrue();
        assertThat(configManager.getCurrentProfile().getProperty("server")).isNull();
    }
}
