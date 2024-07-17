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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import org.apache.ignite.internal.cli.config.ini.IniConfigManager;
import org.junit.jupiter.api.Test;

class ConfigManagerTest {
    @Test
    public void testSaveLoadConfig() {
        File tempFile = TestConfigManagerHelper.createSectionWithDefaultProfileConfig();
        File tempSecretFile = TestConfigManagerHelper.createEmptySecretConfig();
        IniConfigManager configManager = new IniConfigManager(tempFile, tempSecretFile);

        configManager.setProperty("ignite.cluster-endpoint-url", "test");

        IniConfigManager configAfterSave = new IniConfigManager(tempFile, tempSecretFile);
        assertThat(configAfterSave.getCurrentProperty("ignite.cluster-endpoint-url")).isEqualTo("test");
    }

    @Test
    public void testLoadConfigWithoutDefaultProfile() {
        File tempFile = TestConfigManagerHelper.createSectionWithoutDefaultProfileConfig();
        File tempSecretFile = TestConfigManagerHelper.createEmptySecretConfig();
        IniConfigManager configManager = new IniConfigManager(tempFile, tempSecretFile);


        assertThat(configManager.getCurrentProfile().getName()).isEqualTo("owner");
    }

    @Test
    public void testEmptyConfigLoad() {
        File tempFile = TestConfigManagerHelper.createEmptyConfig();
        File tempSecretFile = TestConfigManagerHelper.createEmptySecretConfig();
        IniConfigManager configManager = new IniConfigManager(tempFile, tempSecretFile);

        assertThat(configManager.getCurrentProfile().getName()).isEqualTo("default");
    }


    @Test
    public void testRemoveConfigFileOnRuntime() {
        File tempFile = TestConfigManagerHelper.createSectionWithDefaultProfileConfig();
        File tempSecretFile = TestConfigManagerHelper.createEmptySecretConfig();
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
        File tempFile = TestConfigManagerHelper.createSectionWithDefaultProfileConfig();
        File tempSecretFile = TestConfigManagerHelper.createEmptySecretConfig();
        IniConfigManager configManager = new IniConfigManager(tempFile, tempSecretFile);

        assertThat(configManager.removeProperty("server", configManager.getCurrentProfile().getName())).isEqualTo("127.0.0.1");

        assertThat(tempFile.exists()).isTrue();
        assertThat(configManager.getCurrentProfile().getProperty("server")).isNull();
    }
}
