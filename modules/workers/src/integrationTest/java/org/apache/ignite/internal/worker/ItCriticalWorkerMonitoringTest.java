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

package org.apache.ignite.internal.worker;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.partition.replica.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replica.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replica.network.raft.SnapshotMetaResponse;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("resource")
class ItCriticalWorkerMonitoringTest extends ClusterPerTestIntegrationTest {
    private final LogInspector watchdogLogInspector = LogInspector.create(CriticalWorkerWatchdog.class, true);

    @Override
    protected int initialNodes() {
        return 1;
    }

    @AfterEach
    void closeLogInspectors() {
        watchdogLogInspector.stop();
    }

    @Test
    void nettyThreadBlockageIsReported() throws Exception {
        CountDownLatch blockageDetectedLatch = new CountDownLatch(1);

        watchdogLogInspector.addHandler(
                event -> matchesWithDotall(event, criticalThreadDetectedRegex("srv-worker-")),
                blockageDetectedLatch::countDown
        );

        CountDownLatch unblockLatch = new CountDownLatch(1);

        cluster.node(0).nettyBootstrapFactory().serverEventLoopGroup().execute(() -> {
            try {
                unblockLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        try {
            assertTrue(blockageDetectedLatch.await(10, TimeUnit.SECONDS), "Did not see blockage being detected in time");
        } finally {
            unblockLatch.countDown();
        }
    }

    private static boolean matchesWithDotall(LogEvent event, String regex) {
        Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
        return pattern.matcher(event.getMessage().getFormattedMessage()).matches();
    }

    private static String criticalThreadDetectedRegex(String threadSignature) {
        return "A critical thread is blocked for \\d+ ms that is more than the allowed \\d+ ms, it is .+" + threadSignature + ".+";
    }

    @Test
    void inboundNetworkThreadBlockageIsReported() throws Exception {
        IgniteImpl firstNode = cluster.node(0);
        IgniteImpl secondNode = cluster.startNode(1);

        CountDownLatch blockageDetectedLatch = new CountDownLatch(1);

        watchdogLogInspector.addHandler(
                event -> matchesWithDotall(event, criticalThreadDetectedRegex("MessagingService-inbound-")),
                blockageDetectedLatch::countDown
        );

        CountDownLatch unblockLatch = new CountDownLatch(1);

        firstNode.clusterService().messagingService().addMessageHandler(
                PartitionReplicationMessageGroup.class,
                (message, sender, correlationId) -> {
                    if (message instanceof SnapshotMetaResponse) {
                        try {
                            unblockLatch.await(10, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
        );

        try {
            secondNode.sendFakeMessage(firstNode.name(), snapshotMetaResponse());

            assertTrue(blockageDetectedLatch.await(10, TimeUnit.SECONDS), "Did not see blockage being detected in time");
        } finally {
            unblockLatch.countDown();
        }
    }

    private static SnapshotMetaResponse snapshotMetaResponse() {
        return new PartitionReplicationMessagesFactory().snapshotMetaResponse()
                .meta(new RaftMessagesFactory().snapshotMeta().build())
                .build();
    }
}
