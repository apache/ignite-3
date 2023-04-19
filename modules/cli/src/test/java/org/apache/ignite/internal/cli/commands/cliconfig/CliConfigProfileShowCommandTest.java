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

import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.internal.cli.commands.cliconfig.profile.CliConfigProfileShowCommand;
import org.junit.jupiter.api.Test;

class CliConfigProfileShowCommandTest extends CliConfigCommandTestBase {
    @Override
    protected Class<?> getCommandClass() {
        return CliConfigProfileShowCommand.class;
    }

    @Test
    public void testWithDefaultProfile() {
        execute();

        assertAll(
                () -> assertOutputContains("Current profile: database"),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    public void testWithoutDefaultProfile() {
        configManagerProvider.setConfigFile(TestConfigManagerHelper.createSectionWithoutDefaultProfile());
        execute();

        assertAll(
                () -> assertOutputContains("Current profile: owner"),
                this::assertErrOutputIsEmpty
        );
    }
}
