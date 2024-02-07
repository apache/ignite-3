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

package org.apache.ignite.internal.cli.commands.cluster.status;

import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.Arrays;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ClusterStatusCommand} for the cluster that is initialized.
 */
class ItClusterStatusCommandInitializedTest extends CliIntegrationTest {
    @Test
    @DisplayName("Should print status when valid cluster url is given but cluster is initialized")
    void printStatus() {
        String cmgNodes = Arrays.stream(cmgMetastoreNodes())
                .mapToObj(CLUSTER::node)
                .map(IgniteImpl::name)
                .collect(joining(", ", "[", "]"));

        execute("cluster", "status", "--cluster-endpoint-url", NODE_URL);

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("name: cluster"),
                () -> assertOutputContains("nodes: 3"),
                () -> assertOutputContains("status: active"),
                () -> assertOutputContains("cmgNodes: " + cmgNodes),
                () -> assertOutputContains("msNodes: " + cmgNodes)
        );
    }
}
