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

package org.apache.ignite.internal.cli.commands.cluster.topology;

import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.internal.cli.commands.CliCommandTestNotInitializedIntegrationBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TopologyCommand}.
 */
class ItTopologyCommandNotInitializedClusterTest extends CliCommandTestNotInitializedIntegrationBase {

    @Test
    @DisplayName("Should print physical topology when valid cluster url is provided")
    void printPhysicalTopology() {
        // When
        execute("cluster", "topology", "physical", "--url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );
    }

    @Test
    @DisplayName("Should not print logical topology when valid cluster url is provided but cluster is not initialized")
    void printLogicalTopology() {
        // When
        execute("cluster", "topology", "logical", "--url", NODE_URL);

        // Then prints nothing
        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Cannot show logical topology" + System.lineSeparator()
                                + "Probably, you have not initialized the cluster, try to run ignite cluster init command"
                )
        );
    }
}
