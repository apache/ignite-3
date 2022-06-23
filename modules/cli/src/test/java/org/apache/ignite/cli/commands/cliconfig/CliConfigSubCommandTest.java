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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThat;

import org.apache.ignite.cli.commands.CliCommandTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CliConfigSubCommandTest extends CliCommandTestBase {

    @Override
    protected Class<?> getCommandClass() {
        return CliConfigSubCommand.class;
    }

    @Test
    @DisplayName("Displays all keys")
    void noKey() {
        execute();

        String expectedResult1 = "ignite.cluster-url=test_cluster_url" + System.lineSeparator()
                + "ignite.jdbc-url=test_jdbc_url" + System.lineSeparator();
	      String expectedResult2 = "ignite.jdbc-url=test_jdbc_url" + System.lineSeparator()
		            + "ignite.cluster-url=test_cluster_url" + System.lineSeparator();
        assertAll(
                this::assertExitCodeIsZero,
                () -> assertThat(out.toString().equals(expectedResult1) || out.toString().equals(expectedResult2)),
                this::assertErrOutputIsEmpty
        );
    }
}