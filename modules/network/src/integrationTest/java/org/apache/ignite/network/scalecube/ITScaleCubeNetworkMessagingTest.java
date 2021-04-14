/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.network.scalecube;

import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.transport.api.Transport;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.NetworkMessageHandler;
import org.apache.ignite.network.TestMessage;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.apache.ignite.network.message.NetworkMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** */
class ITScaleCubeNetworkMessagingTest {
    /** */
    private static final long CHECK_INTERVAL = 200;

    /** */
    private static final long CLUSTER_TIMEOUT = TimeUnit.SECONDS.toMillis(3);

    /** */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistry();

    /** */
    private static final ClusterServiceFactory NETWORK_FACTORY = new ScaleCubeClusterServiceFactory();

    /** */
    private final Map<String, NetworkMessage> messageStorage = new ConcurrentHashMap<>();

    /** */
    private final List<ClusterService> startedMembers = new ArrayList<>();

    /** */
    @AfterEach
    public void afterEach() {
        startedMembers.forEach(ClusterService::shutdown);
    }

    /**
     * Test sending and receiving messages.
     */
    @Test
    public void messageWasSentToAllMembersSuccessfully() throws Exception {
        //Given: Three started member which are gathered to cluster.
        List<String> addresses = List.of("localhost:3344", "localhost:3345", "localhost:3346");

        CountDownLatch messageReceivedLatch = new CountDownLatch(3);

        String aliceName = "Alice";

        ClusterService alice = startNetwork(aliceName, 3344, addresses);
        ClusterService bob = startNetwork("Bob", 3345, addresses);
        ClusterService carol = startNetwork("Carol", 3346, addresses);

        waitForCluster(alice);

        NetworkMessageHandler messageWaiter = (message, sender, correlationId) -> messageReceivedLatch.countDown();

        alice.messagingService().addMessageHandler(messageWaiter);
        bob.messagingService().addMessageHandler(messageWaiter);
        carol.messagingService().addMessageHandler(messageWaiter);

        TestMessage testMessage = new TestMessage("Message from Alice", Collections.emptyMap());

        //When: Send one message to all members in cluster.
        for (ClusterNode member : alice.topologyService().allMembers()) {
            alice.messagingService().weakSend(member, testMessage);
        }

        boolean messagesReceived = messageReceivedLatch.await(3, TimeUnit.SECONDS);
        assertTrue(messagesReceived);

        //Then: All members successfully received message.
        assertThat(getLastMessage(alice), is(testMessage));
        assertThat(getLastMessage(bob), is(testMessage));
        assertThat(getLastMessage(carol), is(testMessage));
    }

    /**
     * Test graceful shutdown.
     * @throws Exception If failed.
     */
    @Test
    public void testShutdown() throws Exception {
        testShutdown0(false);
    }

    /**
     * Test forceful shutdown.
     * @throws Exception If failed.
     */
    @Test
    public void testForcefulShutdown() throws Exception {
        testShutdown0(true);
    }

    /**
     * Test shutdown.
     * @param forceful Whether shutdown should be forceful.
     * @throws Exception If failed.
     */
    private void testShutdown0(boolean forceful) throws Exception {
        //Given: Three started member which are gathered to cluster.
        List<String> addresses = List.of("localhost:3344", "localhost:3345");

        String aliceName = "Alice";

        ClusterService alice = startNetwork(aliceName, 3344, addresses);
        ClusterService bob = startNetwork("Bob", 3345, addresses);

        waitForCluster(alice);

        CountDownLatch aliceShutdownLatch = new CountDownLatch(1);

        bob.topologyService().addEventHandler(new TopologyEventHandler() {
            /** {@inheritDoc} */
            @Override public void onAppeared(ClusterNode member) {
                // No-op.
            }

            /** {@inheritDoc} */
            @Override public void onDisappeared(ClusterNode member) {
                if (aliceName.equals(member.name()))
                    aliceShutdownLatch.countDown();
            }
        });

        if (forceful)
            stopForcefully(alice);
        else
            alice.shutdown();

        boolean aliceShutdownReceived = aliceShutdownLatch.await(forceful ? 10 : 3, TimeUnit.SECONDS);
        assertTrue(aliceShutdownReceived);

        Collection<ClusterNode> networkMembers = bob.topologyService().allMembers();

        assertEquals(1, networkMembers.size());
    }

    /** */
    private NetworkMessage getLastMessage(ClusterService clusterService) {
        return messageStorage.get(clusterService.localConfiguration().getName());
    }

    /** */
    private ClusterService startNetwork(String name, int port, List<String> addresses) {
        var context = new ClusterLocalConfiguration(name, port, addresses, SERIALIZATION_REGISTRY);

        ClusterService clusterService = NETWORK_FACTORY.createClusterService(context);

        clusterService.messagingService().addMessageHandler((message, sender, correlationId) -> {
            messageStorage.put(name, message);
        });

        clusterService.start();

        startedMembers.add(clusterService);

        return clusterService;
    }

    /**
     * Find cluster's transport and force it to stop.
     * @param cluster Cluster to be shutdown.
     * @throws Exception If failed to stop.
     */
    private static void stopForcefully(ClusterService cluster) throws Exception {
        Field clusterImplField = cluster.getClass().getDeclaredField("val$cluster");
        clusterImplField.setAccessible(true);

        ClusterImpl clusterImpl = (ClusterImpl) clusterImplField.get(cluster);
        Field transportField = clusterImpl.getClass().getDeclaredField("transport");
        transportField.setAccessible(true);

        Transport transport = (Transport) transportField.get(clusterImpl);
        Method stop = transport.getClass().getDeclaredMethod("stop");
        stop.setAccessible(true);

        Mono invoke = (Mono) stop.invoke(transport);
        invoke.block();
    }

    /**
     * Wait for cluster to come up.
     * @param cluster Network cluster.
     */
    private void waitForCluster(ClusterService cluster) {
        AtomicInteger integer = new AtomicInteger(0);

        cluster.topologyService().addEventHandler(new TopologyEventHandler() {
            /** {@inheritDoc} */
            @Override public void onAppeared(ClusterNode member) {
                integer.set(cluster.topologyService().allMembers().size());
            }

            /** {@inheritDoc} */
            @Override public void onDisappeared(ClusterNode member) {
                integer.decrementAndGet();
            }
        });

        integer.set(cluster.topologyService().allMembers().size());

        long curTime = System.currentTimeMillis();
        long endTime = curTime + CLUSTER_TIMEOUT;

        while (curTime < endTime) {
            if (integer.get() == startedMembers.size()) {
                return;
            }

            if (CHECK_INTERVAL > 0) {
                try {
                    Thread.sleep(CHECK_INTERVAL);
                }
                catch (InterruptedException ignored) {
                }
            }

            curTime = System.currentTimeMillis();
        }

        throw new RuntimeException("Failed to wait for cluster startup");
    }
}
