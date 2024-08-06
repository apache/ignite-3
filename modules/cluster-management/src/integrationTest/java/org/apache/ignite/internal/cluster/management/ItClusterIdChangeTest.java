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

package org.apache.ignite.internal.cluster.management;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.network.DefaultMessagingService;
import org.apache.ignite.internal.network.netty.ConnectionManager;
import org.apache.ignite.internal.network.netty.NettySender;
import org.junit.jupiter.api.Test;

/**
 * Tests that, if 2 nodes have different cluster IDs, they cannot communicate with each other using the network.
 */
class ItClusterIdChangeTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 2;
    }

    @Test
    void nodesWithDifferentClusterIdsCannotCommunicate() throws Exception {
        IgniteImpl node1 = node(0);
        IgniteImpl node2 = node(1);

        node2.clusterIdService().clusterId(UUID.randomUUID());

        ConnectionManager connectionManager1 = ((DefaultMessagingService) node1.clusterService().messagingService()).connectionManager();

        //noinspection rawtypes
        CompletableFuture[] closeFutures = connectionManager1.channels().values().stream()
                .map(NettySender::closeAsync)
                .toArray(CompletableFuture[]::new);
        assertThat(allOf(closeFutures), willCompleteSuccessfully());

        assertFalse(
                waitForCondition(() -> connectionManager1.channels().values().stream().anyMatch(NettySender::isOpen), SECONDS.toMillis(1)),
                "Nodes with different clusterIDs are able to communicate"
        );
    }
}
