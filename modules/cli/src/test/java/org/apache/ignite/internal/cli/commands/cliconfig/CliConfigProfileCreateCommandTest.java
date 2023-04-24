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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.internal.cli.commands.cliconfig.profile.CliConfigProfileCreateCommand;
import org.apache.ignite.internal.cli.config.exception.ProfileNotFoundException;
import org.junit.jupiter.api.Test;

class CliConfigProfileCreateCommandTest extends CliConfigCommandTestBase {
    @Override
    protected Class<?> getCommandClass() {
        return CliConfigProfileCreateCommand.class;
    }

    @Test
    public void testWithoutProfileName() {
        execute();

        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Missing required parameter: '<profileName>'")
        );
    }

    @Test
    public void testProfileCreation() {
        execute("profileName");

        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Profile profileName was created successfully"),
                () -> assertThat(configManagerProvider.get().getCurrentProfile().getName()).isNotEqualTo("profileName")
        );
    }

    @Test
    public void testProfileCopyFrom() {
        execute("profileName --copy-from database");

        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Profile profileName was created successfully"),
                () -> assertThat(configManagerProvider.get().getProfile("profileName").getAll()).containsAllEntriesOf(
                        configManagerProvider.get().getProfile("database").getAll())
        );
    }

    @Test
    public void testCopyFromNonExist() {
        execute("profileName --copy-from notExist");

        assertAll(
                () -> assertExitCodeIs(1),
                () -> assertErrOutputContains("Profile notExist not found"),
                this::assertOutputIsEmpty,
                () -> assertThatThrownBy(() -> configManagerProvider.get().getProfile("profileName"))
                        .isInstanceOf(ProfileNotFoundException.class)
        );
    }

    @Test
    public void testProfileActivate() {
        execute("profileName --activate");

        assertAll(
                () -> assertOutputContains("Profile profileName was created successfully"),
                this::assertErrOutputIsEmpty,
                () -> assertThat(configManagerProvider.get().getCurrentProfile().getName()).isEqualTo("profileName")
        );
    }

    @Test
    public void testCreateExistedProfile() {
        execute("profileName");
        assertAll(
                () -> assertOutputContains("Profile profileName was created successfully"),
                this::assertErrOutputIsEmpty
        );

        execute("profileName");

        assertAll(
                () -> assertErrOutputContains("Section profileName already exist")
        );
    }

}
