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

package org.apache.ignite.internal.cli.commands.sql;

import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.internal.cli.commands.CliCommandTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SqlCommandTest}.
 */
public class SqlCommandTest extends CliCommandTestBase {

    @Override
    protected Class<?> getCommandClass() {
        return SqlCommand.class;
    }

    @Test
    @DisplayName("Should throw error if executed without --execute or --file options")
    void withoutOptions() {
        execute("--jdbc-url=");

        assertAll(
                () -> assertExitCodeIs(2),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Missing required argument (specify one of these): (--file=<file> | <command>)")
        );
    }

    @Test
    @DisplayName("Should throw error if both --execute or --file options are present")
    void mutuallyExclusiveOptions() {
        execute("--jdbc-url=", "select", "--file=");

        assertAll(
                () -> assertExitCodeIs(2),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("<command>, --file=<file> are mutually exclusive (specify only one)")
        );
    }
}
