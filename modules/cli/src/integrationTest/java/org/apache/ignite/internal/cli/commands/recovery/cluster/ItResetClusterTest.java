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

package org.apache.ignite.internal.cli.commands.recovery.cluster;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_CMG_NODES_OPTION;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.app.IgniteServerImpl;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/** Base test class for reset cluster commands. */
@TestMethodOrder(OrderAnnotation.class)
abstract class ItResetClusterTest extends CliIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    @Order(Integer.MAX_VALUE) // This test is run last as it restarts the node.
    void initiatesCmgRepair() throws Exception {
        execute(
                CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_CMG_NODES_OPTION, CLUSTER.aliveNode().name()
        );

        try {
            assertErrOutputIsEmpty();
            assertOutputContains("Node has gone, this most probably means that cluster repair is initiated and the node restarts.");
        } finally {
            waitTillNodeRestartsInternally(0);
        }
    }

    private static void waitTillNodeRestartsInternally(int nodeIndex) throws InterruptedException {
        // restartOrShutdownFuture() becomes non-null when restart or shutdown is initiated; we know it's restart.

        assertTrue(
                waitForCondition(() -> restartOrShutdownFuture(nodeIndex) != null, SECONDS.toMillis(20)),
                "Node did not attempt to be restarted (or shut down) in time"
        );
        assertThat(restartOrShutdownFuture(nodeIndex), willCompleteSuccessfully());

        unwrapIgniteImpl(CLUSTER.server(nodeIndex).api());
    }

    @Nullable
    private static CompletableFuture<Void> restartOrShutdownFuture(int nodeIndex) {
        return ((IgniteServerImpl) CLUSTER.server(nodeIndex)).restartOrShutdownFuture();
    }

    @Test
    void handlesErrors() {
        execute(
                CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_CMG_NODES_OPTION, "no-such-node"
        );

        assertErrOutputContains("Current node is not contained in the new CMG, so it cannot conduct a cluster reset.");
        assertOutputIsEmpty();
    }
}
