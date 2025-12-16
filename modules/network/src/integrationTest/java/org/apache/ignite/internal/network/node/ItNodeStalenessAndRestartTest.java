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

package org.apache.ignite.internal.network.node;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.ConfigTemplates.FAST_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.ConfigOverride;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.TopologyEventHandler;
import org.apache.ignite.internal.network.message.ScaleCubeMessage;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class ItNodeStalenessAndRestartTest extends ClusterPerTestIntegrationTest {

    private static final String FAILURE_MESSAGE = "Ignite node is in invalid state due to a critical failure.";

    @Override
    protected int initialNodes() {
        return 2;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return FAST_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE;
    }

    @Test
    void nodeStalenessStatusIsClearedOnRestart() throws Exception {
        IgniteImpl ignite0 = unwrapIgniteImpl(cluster.node(0));

        simulateNetworkPartition(ignite0);

        IgniteImpl restartedIgnite0 = unwrapIgniteImpl(cluster.restartNode(0));

        assertTrue(
                waitForCondition(
                        () -> restartedIgnite0.clusterService().topologyService().allMembers().size() == 2,
                        SECONDS.toMillis(10)
                ),
                "Did not see node 1 in the topology of restarted node 0"
        );
    }

    @Test
    @ConfigOverride(name = "ignite.failureHandler.handler.type", value = "stop")
    @Disabled
    void staleNodeIsShutDown() throws Exception {
        IgniteImpl ignite0 = unwrapIgniteImpl(cluster.node(0));

        LogInspector logInspector = new LogInspector(
                FailureManager.class.getName(),
                evt -> evt.getLevel() == Level.ERROR && evt.getMessage().getFormattedMessage().contains(FAILURE_MESSAGE)
        );

        logInspector.start();
        try {
            simulateNetworkPartition(ignite0);

            await().timeout(10, SECONDS).until(logInspector::isMatched);
        } finally {
            logInspector.stop();
        }
    }

    private void simulateNetworkPartition(IgniteImpl ignite0) throws InterruptedException {
        CountDownLatch ignite1Left = new CountDownLatch(1);

        ignite0.clusterService().topologyService().addEventHandler(new TopologyEventHandler() {
            @Override
            public void onDisappeared(InternalClusterNode member) {
                if (member.name().equals(cluster.node(1).name())) {
                    ignite1Left.countDown();
                }
            }
        });

        ignite0.dropMessages((name, message) -> message instanceof ScaleCubeMessage);

        assertTrue(ignite1Left.await(30, SECONDS), "Did not see node 1 disappearing in time");
    }
}
