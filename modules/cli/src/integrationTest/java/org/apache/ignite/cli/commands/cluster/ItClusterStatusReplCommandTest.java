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

package org.apache.ignite.cli.commands.cluster;

import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.cli.commands.CliCommandTestIntegrationBase;
import org.apache.ignite.cli.commands.cluster.status.ClusterStatusReplSubCommand;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ClusterStatusReplSubCommand}.
 */
class ItClusterStatusReplCommandTest extends CliCommandTestIntegrationBase {

    @Test
    @DisplayName("Should print status when valid cluster url is given")
    @Disabled("Use topology call to get the number of nodes https://issues.apache.org/jira/browse/IGNITE-17092")
    void printStatus() {
        execute("cluster", "status", "--cluster-url", NODE_URL);

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Cluster status:")
        );
    }
}
