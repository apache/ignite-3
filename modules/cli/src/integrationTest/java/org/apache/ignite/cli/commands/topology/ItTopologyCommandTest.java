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

package org.apache.ignite.cli.commands.topology;

import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.cli.commands.CliCommandTestIntegrationBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TopologyCommand}.
 */
class ItTopologyCommandTest extends CliCommandTestIntegrationBase {

    @Test
    @DisplayName("Should print topology when valid cluster url is provided")
    void printTopology() {
        // When
        execute("topology", "--cluster-url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );
        // TODO: https://issues.apache.org/jira/browse/IGNITE-17092
        //consistent ID, ID, address, status
        //node 1, e2d4988a-b836-4e7e-a888-2639e6f79ef0, 127.0.0.1, RUNNING
        //node 2, 5cb561fc-1963-4f95-98f8-deb407669a86, 127.0.0.2, RECOVERY
    }
}
