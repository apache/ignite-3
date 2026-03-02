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

package org.apache.ignite.internal.network;

import static org.apache.ignite.internal.ClusterConfiguration.DEFAULT_BASE_PORT;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapRootCause;
import static org.apache.ignite.lang.ErrorGroups.Network.ADDRESS_UNRESOLVED_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.typesafe.config.parser.ConfigDocumentFactory;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.NodeBootstrapConfigUpdater;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class ItStaticNodeFinderTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 0;
    }

    /** Tests that node finder failure causes node shutdown. */
    @Test
    void testNodeShutdownOnNodeFinderFailure(TestInfo testInfo) {
        Throwable throwable = assertThrowsWithCause(
                () -> startEmbeddedNode(
                        testInfo,
                        0,
                        config -> ConfigDocumentFactory.parseString(config)
                                .withValueText("ignite.network.nodeFinder.netClusterNodes", "[ \"bad.host:1234\" ]")
                                .withValueText("ignite.network.nodeFinder.nameResolutionAttempts", "1")
                                .render()
                ),
                IgniteInternalException.class
        );

        IgniteInternalException actual = (IgniteInternalException) unwrapRootCause(throwable);
        assertEquals(ADDRESS_UNRESOLVED_ERR, actual.code());
        assertEquals("No network addresses resolved through any provided names", actual.getMessage());
    }

    /**
     * Verifies a situation when two nodes are started simultaneously, but one of them is stuck trying to resolve host names. We then
     * check that no network threads are blocked while name resolution is in progress.
     */
    @Test
    void testNameResolutionDoesNotBlockNetworkThreads(TestInfo testInfo) {
        LogInspector watchdogLogInspector = LogInspector.create(FailureManager.class, true);

        var blockedThreadsCounter = new AtomicInteger();

        watchdogLogInspector.addHandler(
                event -> {
                    Throwable thrown = event.getThrown();

                    return thrown != null && thrown.getMessage().contains("A critical thread is blocked");
                },
                blockedThreadsCounter::incrementAndGet
        );

        try {
            // First, start the node that will get stuck trying to resolve host names.
            CompletableFuture<Void> startBrokenNodeFuture = runAsync(() -> startEmbeddedNode(
                    testInfo,
                    0,
                    config -> ConfigDocumentFactory.parseString(config)
                            .withValueText("ignite.network.port", String.valueOf(DEFAULT_BASE_PORT))
                            .withValueText("ignite.network.nodeFinder.netClusterNodes", "[ \"bad.host:1234\" ]")
                            .withValueText("ignite.network.nodeFinder.nameResolutionAttempts", "3")
                            .render()
            ));

            // Start a second node that will try to open a connection to the first node. It should start successfully.
            startEmbeddedNode(
                    testInfo,
                    1,
                    config -> ConfigDocumentFactory.parseString(config)
                            .withValueText("ignite.network.port", String.valueOf(DEFAULT_BASE_PORT + 1))
                            .withValueText(
                                    "ignite.network.nodeFinder.netClusterNodes",
                                    String.format("[ \"localhost:%d\" ]", DEFAULT_BASE_PORT)
                            )
                            .render()
            );

            // The first node should fail to start after host name resolution timeout.
            assertThat(
                    startBrokenNodeFuture,
                    willThrowWithCauseOrSuppressed(IgniteInternalException.class,
                            "No network addresses resolved through any provided names")
            );
        } finally {
            watchdogLogInspector.stop();
        }

        // Verify that no critical threads were blocked during the test.
        assertThat(blockedThreadsCounter.get(), is(0));
    }

    private void startEmbeddedNode(TestInfo testInfo, int nodeIndex, NodeBootstrapConfigUpdater nodeBootstrapConfigUpdater) {
        cluster.startEmbeddedNode(testInfo, nodeIndex, getNodeBootstrapConfigTemplate(), nodeBootstrapConfigUpdater);
    }
}
