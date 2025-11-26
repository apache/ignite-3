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

package org.apache.ignite.internal;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.parser.ConfigDocument;
import com.typesafe.config.parser.ConfigDocumentFactory;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.internal.Cluster.ServerRegistration;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.app.IgniteServerImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;

class ItIgniteStartTest extends ClusterPerTestIntegrationTest {
    private static final long RAFT_RETRY_TIMEOUT_MILLIS = 5000;

    @Override
    protected int initialNodes() {
        return 0;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        ConfigDocument document = ConfigDocumentFactory.parseString(super.getNodeBootstrapConfigTemplate())
                .withValueText("ignite.raft.retryTimeoutMillis", Long.toString(RAFT_RETRY_TIMEOUT_MILLIS));
        return document.render();
    }

    @Test
    void nodeStartDoesntTimeoutWhenCmgIsUnavailable() throws Exception {
        int nodeCount = 2;
        cluster.startAndInit(nodeCount, new int[]{1});

        stopNodes(nodeCount);

        ServerRegistration registration0 = cluster.startEmbeddedNode(0);

        // Now allow an attempt to join to timeout (if it doesn't have an infinite timeout).
        waitTillRaftTimeoutPasses();

        assertDoesNotThrow(() -> cluster.startNode(1));

        assertThat(registration0.registrationFuture(), willCompleteSuccessfully());
    }

    private void stopNodes(int nodeCount) {
        IntStream.range(0, nodeCount).parallel().forEach(nodeIndex -> cluster.stopNode(nodeIndex));
    }

    private static void waitTillRaftTimeoutPasses() throws InterruptedException {
        Thread.sleep(RAFT_RETRY_TIMEOUT_MILLIS + 1000);
    }

    @Test
    void nodeStartDoesntTimeoutWhenMgIsUnavailable() throws Exception {
        int nodeCount = 2;
        cluster.startAndInit(nodeCount, builder -> {
            builder.cmgNodeNames(cluster.nodeName(0));
            builder.metaStorageNodeNames(cluster.nodeName(1));
        });

        stopNodes(nodeCount);

        // This node has majority of CMG, but not MG.
        ServerRegistration registration0 = cluster.startEmbeddedNode(0);

        waitTill1NodeValidateItselfWithCmg(registration0);

        // Now allow an attempt to recover Metastorage to timeout (if it doesn't have an infinite timeout).
        waitTillRaftTimeoutPasses();

        assertDoesNotThrow(() -> cluster.startNode(1));

        assertThat(registration0.registrationFuture(), willCompleteSuccessfully());
    }

    @Test
    void testStartOnSameNetworkPort() {
        IgniteException exception = (IgniteException) assertThrows(
                IgniteException.class,
                () -> cluster.startAndInitWithUpdateBootstrapConfig(
                        2,
                        nodeBootstrapConfig ->
                                ConfigDocumentFactory.parseString(nodeBootstrapConfig)
                                        .withValueText("ignite.network.port", Integer.toString(ClusterConfiguration.DEFAULT_BASE_PORT))
                                        .render()
                ),
                "Unable to start [node="
        );

        assertEquals("IGN-NETWORK-2", exception.codeAsString());
    }

    private static void waitTill1NodeValidateItselfWithCmg(ServerRegistration registration) throws InterruptedException {
        IgniteImpl ignite = ((IgniteServerImpl) registration.server()).igniteImpl();

        assertTrue(
                waitForCondition(() -> validatedNodes(ignite).size() == 1, SECONDS.toMillis(10)),
                "Did not see 1 node being validated in time after restart"
        );
    }

    private static Set<InternalClusterNode> validatedNodes(IgniteImpl ignite) {
        CompletableFuture<Set<InternalClusterNode>> validatedNodesFuture = ignite.clusterManagementGroupManager().validatedNodes();
        assertThat(validatedNodesFuture, willCompleteSuccessfully());
        return validatedNodesFuture.join();
    }
}
