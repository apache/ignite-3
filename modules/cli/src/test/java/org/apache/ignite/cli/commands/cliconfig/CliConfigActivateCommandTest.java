/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.commands.cliconfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import jakarta.inject.Inject;
import org.apache.ignite.cli.commands.CliCommandTestBase;
import org.apache.ignite.cli.commands.cliconfig.profile.CliConfigProfileCommand;
import org.apache.ignite.cli.config.ini.IniConfigManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CliConfigActivateCommandTest extends CliCommandTestBase {
    @Inject
    private TestConfigManagerProvider configManagerProvider;

    @Override
    protected Class<?> getCommandClass() {
        return CliConfigProfileCommand.class;
    }

    @BeforeEach
    public void setup() {
        configManagerProvider.configManager = new IniConfigManager(TestConfigManagerHelper.createSectionWithInternalPart());
    }

    @Test
    public void activateTest() {
        execute("--set-current owner");

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains("Profile owner was activated successfully"),
                this::assertErrOutputIsEmpty,
                () -> assertThat(configManagerProvider.get().getCurrentProfile().getName()).isEqualTo("owner")
        );
    }

    @Test
    public void activateWithNotExistedProfileTest() {
        execute("--set-current notExist");

        assertAll(
                () -> assertExitCodeIs(1),
                () -> assertErrOutputContains("Profile notExist not found"),
                this::assertOutputIsEmpty,
                () -> assertThat(configManagerProvider.get().getCurrentProfile().getName()).isEqualTo("database")
        );
    }

}
