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

package org.apache.ignite.internal.cli.commands.recovery.restart;

import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_TABLE_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_ZONE_NAME_OPTION;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.internal.cli.commands.CliCommandTestNotInitializedIntegrationBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link RestartPartitionsCommand} on non-initialized cluster.
 */
public class ItRestartPartitionsNonInitilizedTest extends CliCommandTestNotInitializedIntegrationBase {

    protected String getExpectedErrorMessage() {
        return CLUSTER_NOT_INITIALIZED_ERROR_MESSAGE;
    }

    @Test
    @DisplayName("Should print error message when recovery restart-partitions on not initialized cluster")
    void deploy() {
        execute("recovery", "restart-partitions", CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_TABLE_NAME_OPTION, "tableName", RECOVERY_ZONE_NAME_OPTION, "zoneName");
        assertAll(
                () -> assertErrOutputContains("Cannot restart partitions"),
                () -> assertErrOutputContains(getExpectedErrorMessage())
        );
    }
}
