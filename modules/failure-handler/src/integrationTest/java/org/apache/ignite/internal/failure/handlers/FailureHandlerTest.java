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

package org.apache.ignite.internal.failure.handlers;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.lang.ErrorGroups.Common.ILLEGAL_ARGUMENT_ERR;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.EmbeddedNode;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureType;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.junit.jupiter.api.Test;

/**
 * Tests for failure handlers.
 */
public class FailureHandlerTest extends ClusterPerTestIntegrationTest {
    private static final int TIMEOUT_MILLIS = 30_000;

    @Override
    protected int initialNodes() {
        return 0;
    }

    @Test
    void testStopNodeFailureHandler() {
        testFailureHandler(new StopNodeFailureHandler());
    }

    @Test
    void testStopNodeOrHaltFailureHandler() {
        testFailureHandler(new StopNodeOrHaltFailureHandler(true, TIMEOUT_MILLIS));
    }

    private void testFailureHandler(FailureHandler hnd) {
        EmbeddedNode node = cluster.startEmbeddedNode(0);
        CompletableFuture<Ignite> fut = node.joinClusterAsync();

        hnd.onFailure(
                node::stopAsync,
                new FailureContext(
                        FailureType.CRITICAL_ERROR,
                        new IgniteInternalException(ILLEGAL_ARGUMENT_ERR, "Test error")));

        await().timeout(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS).until(fut::isDone);

        assertThrowsWithCause(fut::get, NodeStoppingException.class, null);
    }
}
