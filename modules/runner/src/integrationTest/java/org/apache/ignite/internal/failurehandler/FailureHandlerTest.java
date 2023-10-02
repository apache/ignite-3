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

package org.apache.ignite.internal.failurehandler;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.IgniteIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureType;
import org.apache.ignite.internal.failure.handlers.FailureHandler;
import org.apache.ignite.internal.failure.handlers.StopNodeFailureHandler;
import org.apache.ignite.internal.failure.handlers.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests for failure handlers.
 */
public class FailureHandlerTest extends IgniteIntegrationTest {
    private static final int TIMEOUT_MILLIS = 10_000;

    @WorkDirectory
    private Path workDir;

    @Test
    void testStopNodeFailureHandler(TestInfo testInfo) throws Exception {
        testFailureHandler(new StopNodeFailureHandler(), testInfo);
    }

    @Test
    void testStopNodeOrHaltFailureHandler(TestInfo testInfo) throws Exception {
        testFailureHandler(new StopNodeOrHaltFailureHandler(true, TIMEOUT_MILLIS), testInfo);
    }

    void testFailureHandler(FailureHandler hnd, TestInfo testInfo) throws Exception {
        String nodeName = testNodeName(testInfo, 0);

        IgniteImpl node = startNode(nodeName);

        LogicalTopologyService topologyService = node.logicalTopologyService();

        hnd.onFailure(nodeName, new FailureContext(FailureType.CRITICAL_ERROR, null));

        waitForCondition(() -> topologyService.logicalTopologyOnLeader().isCompletedExceptionally(), TIMEOUT_MILLIS);

        assertThrowsWithCause(() -> topologyService.logicalTopologyOnLeader().join(), NodeStoppingException.class);
    }

    private IgniteImpl startNode(String nodeName) {
        CompletableFuture<Ignite> future = TestIgnitionManager.start(nodeName, null, workDir);

        InitParameters initParameters = InitParameters.builder()
                .destinationNodeName(nodeName)
                .metaStorageNodeNames(List.of(nodeName))
                .clusterName("cluster")
                .build();

        TestIgnitionManager.init(initParameters);

        assertThat(future, willCompleteSuccessfully());

        return (IgniteImpl) future.join();
    }
}
