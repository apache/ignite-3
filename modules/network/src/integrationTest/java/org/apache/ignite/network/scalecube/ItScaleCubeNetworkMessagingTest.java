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

package org.apache.ignite.network.scalecube;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.utils.ClusterServiceTestUtils.findLocalAddresses;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.transport.api.Transport;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.network.NetworkMessageTypes;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.handshake.HandshakeException;
import org.apache.ignite.internal.network.messages.TestMessage;
import org.apache.ignite.internal.network.messages.TestMessageTypes;
import org.apache.ignite.internal.network.messages.TestMessagesFactory;
import org.apache.ignite.internal.network.netty.ConnectionManager;
import org.apache.ignite.internal.network.recovery.RecoveryClientHandshakeManager;
import org.apache.ignite.internal.network.recovery.RecoveryServerHandshakeManager;
import org.apache.ignite.internal.network.recovery.message.HandshakeFinishMessage;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.DefaultMessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.NodeFinder;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;

/**
 * Integration tests for messaging based on ScaleCube.
 */
class ItScaleCubeNetworkMessagingTest {
    /**
     * Test cluster.
     *
     * <p>Each test should create its own cluster with the required number of nodes.
     */
    private Cluster testCluster;

    /** Message factory. */
    private final TestMessagesFactory messageFactory = new TestMessagesFactory();

    /** List of test log inspectors. */
    private final List<LogInspector> logInspectors = new ArrayList<>();

    private TestInfo testInfo;

    @BeforeEach
    void saveTestInfo(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    @AfterEach
    void tearDown() {
        testCluster.shutdown();
        logInspectors.forEach(LogInspector::stop);
        logInspectors.clear();
    }

    /**
     * Tests sending and receiving messages.
     *
     * @throws Exception in case of errors.
     */
    @Test
    public void messageWasSentToAllMembersSuccessfully(TestInfo testInfo) throws Exception {
        Map<String, TestMessage> messageStorage = new ConcurrentHashMap<>();

        var messageReceivedLatch = new CountDownLatch(3);

        testCluster = new Cluster(3, testInfo);

        for (ClusterService member : testCluster.members) {
            member.messagingService().addMessageHandler(
                    TestMessageTypes.class,
                    (message, sender, correlationId) -> {
                        messageStorage.put(member.nodeName(), (TestMessage) message);
                        messageReceivedLatch.countDown();
                    }
            );
        }

        testCluster.startAwait();

        var testMessage = messageFactory.testMessage().msg("Message from Alice").build();

        ClusterService alice = testCluster.members.get(0);

        for (ClusterNode member : alice.topologyService().allMembers()) {
            alice.messagingService().weakSend(member, testMessage);
        }

        boolean messagesReceived = messageReceivedLatch.await(3, TimeUnit.SECONDS);
        assertTrue(messagesReceived);

        testCluster.members.stream()
                .map(ClusterService::nodeName)
                .map(messageStorage::get)
                .forEach(msg -> assertThat(msg.msg(), is(testMessage.msg())));
    }

    /**
     * Tests a graceful shutdown.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testShutdown(TestInfo testInfo) throws Exception {
        testShutdown0(testInfo, false);
    }

    /**
     * Tests a forceful shutdown.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testForcefulShutdown(TestInfo testInfo) throws Exception {
        testShutdown0(testInfo, true);
    }

    /**
     * Sends a message from a node to itself and verifies that it gets delivered successfully.
     *
     * @throws Exception in case of errors.
     */
    @Test
    public void testSendMessageToSelf(TestInfo testInfo) throws Exception {
        testCluster = new Cluster(1, testInfo);
        testCluster.startAwait();

        ClusterService member = testCluster.members.get(0);

        ClusterNode self = member.topologyService().localMember();

        class Data {
            private final TestMessage message;

            private final String senderConsistentId;

            private final Long correlationId;

            private Data(TestMessage message, String senderConsistentId, Long correlationId) {
                this.message = message;
                this.senderConsistentId = senderConsistentId;
                this.correlationId = correlationId;
            }
        }

        var dataFuture = new CompletableFuture<Data>();

        member.messagingService().addMessageHandler(
                TestMessageTypes.class,
                (message, sender, correlationId) ->
                        dataFuture.complete(new Data((TestMessage) message, sender, correlationId))
        );

        var requestMessage = messageFactory.testMessage().msg("request").build();

        member.messagingService().send(self, requestMessage);

        Data actualData = dataFuture.get(3, TimeUnit.SECONDS);

        assertThat(actualData.message.msg(), is(requestMessage.msg()));
        assertThat(actualData.senderConsistentId, is(self.name()));
        assertNull(actualData.correlationId);
    }

    /**
     * Sends a messages from a node to itself and awaits the response.
     *
     * @throws Exception in case of errors.
     */
    @Test
    public void testInvokeMessageToSelf(TestInfo testInfo) throws Exception {
        testCluster = new Cluster(1, testInfo);
        testCluster.startAwait();

        ClusterService member = testCluster.members.get(0);

        ClusterNode self = member.topologyService().localMember();

        var requestMessage = messageFactory.testMessage().msg("request").build();
        var responseMessage = messageFactory.testMessage().msg("response").build();

        member.messagingService().addMessageHandler(
                TestMessageTypes.class,
                (message, sender, correlationId) -> {
                    if (message.equals(requestMessage)) {
                        member.messagingService().respond(self, responseMessage, correlationId);
                    }
                }
        );

        TestMessage actualResponseMessage = member.messagingService()
                .invoke(self, requestMessage, 1000)
                .thenApply(TestMessage.class::cast)
                .get(3, TimeUnit.SECONDS);

        assertThat(actualResponseMessage.msg(), is(responseMessage.msg()));
    }

    /**
     * Tests that if the network component is stopped before trying to make an "invoke" call, the corresponding future completes
     * exceptionally.
     */
    @Test
    public void testInvokeAfterStop(TestInfo testInfo) throws InterruptedException {
        testCluster = new Cluster(2, testInfo);
        testCluster.startAwait();

        ClusterService member0 = testCluster.members.get(0);
        ClusterService member1 = testCluster.members.get(1);

        member0.stop();

        // Perform two invokes to test that multiple requests can get cancelled.
        CompletableFuture<NetworkMessage> invoke0 = member0.messagingService().invoke(
                member1.topologyService().localMember(),
                messageFactory.testMessage().build(),
                1000
        );

        CompletableFuture<NetworkMessage> invoke1 = member0.messagingService().invoke(
                member1.topologyService().localMember(),
                messageFactory.testMessage().build(),
                1000
        );

        ExecutionException e = assertThrows(ExecutionException.class, () -> invoke0.get(1, TimeUnit.SECONDS));

        assertThat(e.getCause(), instanceOf(NodeStoppingException.class));

        e = assertThrows(ExecutionException.class, () -> invoke1.get(1, TimeUnit.SECONDS));

        assertThat(e.getCause(), instanceOf(NodeStoppingException.class));
    }

    /**
     * Tests that if the network component is stopped while making an "invoke" call, the corresponding future completes
     * exceptionally.
     */
    @Test
    public void testInvokeDuringStop(TestInfo testInfo) throws InterruptedException {
        testCluster = new Cluster(2, testInfo);
        testCluster.startAwait();

        ClusterService member0 = testCluster.members.get(0);
        ClusterService member1 = testCluster.members.get(1);

        // We don't register a message listener on the receiving side, so all "invoke"s should timeout.

        // Perform two invokes to test that multiple requests can get cancelled.
        CompletableFuture<NetworkMessage> invoke0 = member0.messagingService().invoke(
                member1.topologyService().localMember(),
                messageFactory.testMessage().build(),
                1000
        );

        CompletableFuture<NetworkMessage> invoke1 = member0.messagingService().invoke(
                member1.topologyService().localMember(),
                messageFactory.testMessage().build(),
                1000
        );

        member0.stop();

        ExecutionException e = assertThrows(ExecutionException.class, () -> invoke0.get(1, TimeUnit.SECONDS));

        assertThat(e.getCause(), instanceOf(NodeStoppingException.class));

        e = assertThrows(ExecutionException.class, () -> invoke1.get(1, TimeUnit.SECONDS));

        assertThat(e.getCause(), instanceOf(NodeStoppingException.class));
    }

    /**
     * Tests that if the network component is stopped while waiting for a response to an "invoke" call, the corresponding future completes
     * exceptionally.
     */
    @Test
    public void testStopDuringAwaitingForInvokeResponse(TestInfo testInfo) throws InterruptedException {
        testCluster = new Cluster(2, testInfo);
        testCluster.startAwait();

        ClusterService member0 = testCluster.members.get(0);
        ClusterService member1 = testCluster.members.get(1);

        CountDownLatch receivedTestMessages = new CountDownLatch(2);

        member1.messagingService().addMessageHandler(
                TestMessageTypes.class,
                (message, senderConsistentId, correlationId) -> receivedTestMessages.countDown()
        );

        // The registered message listener on the receiving side does not send responses, so all "invoke"s should timeout.

        // Perform two invokes to test that multiple requests can get cancelled.
        CompletableFuture<NetworkMessage> invoke0 = member0.messagingService().invoke(
                member1.topologyService().localMember(),
                messageFactory.testMessage().build(),
                1000
        );

        CompletableFuture<NetworkMessage> invoke1 = member0.messagingService().invoke(
                member1.topologyService().localMember(),
                messageFactory.testMessage().build(),
                1000
        );

        assertTrue(receivedTestMessages.await(10, TimeUnit.SECONDS), "Did not receive invocations on the receiver in time");

        member0.stop();

        ExecutionException e = assertThrows(ExecutionException.class, () -> invoke0.get(1, TimeUnit.SECONDS));

        assertThat(e.getCause(), instanceOf(NodeStoppingException.class));

        e = assertThrows(ExecutionException.class, () -> invoke1.get(1, TimeUnit.SECONDS));

        assertThat(e.getCause(), instanceOf(NodeStoppingException.class));
    }

    /**
     * Tests that Scalecube messages are not blocked if some message handler blocks handling of 'normal' messages.
     */
    @Test
    public void scalecubeMessagesAreSentSeparatelyFromOtherMessages(TestInfo testInfo) throws InterruptedException {
        testCluster = new Cluster(2, testInfo);
        testCluster.startAwait();

        ClusterService member0 = testCluster.members.get(0);
        ClusterService member1 = testCluster.members.get(1);

        CountDownLatch startedBlocking = new CountDownLatch(1);
        CountDownLatch blocking = new CountDownLatch(1);

        member1.messagingService().addMessageHandler(
                TestMessageTypes.class,
                (message, senderConsistentId, correlationId) -> {
                    startedBlocking.countDown();

                    try {
                        blocking.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
        );

        try {
            // Send a message that will block the inbound thread for normal messages.
            member0.messagingService().send(member1.topologyService().localMember(), messageFactory.testMessage().build());

            assertTrue(startedBlocking.await(10, TimeUnit.SECONDS));

            CountDownLatch receivedScalecubeMessage = new CountDownLatch(1);

            member1.messagingService().addMessageHandler(
                    NetworkMessageTypes.class,
                    (message, senderConsistentId, correlationId) -> {
                        receivedScalecubeMessage.countDown();
                    }
            );

            assertTrue(
                    receivedScalecubeMessage.await(10, TimeUnit.SECONDS),
                    "Did not receive Scalecube messages, probably Scalecube inbound thread is blocked"
            );
        } finally {
            blocking.countDown();
        }
    }

    /**
     * Tests that messages from different message groups can be delivered to different sets of handlers.
     *
     * @throws Exception in case of errors.
     */
    @Test
    public void testMessageGroupsHandlers(TestInfo testInfo) throws Exception {
        testCluster = new Cluster(2, testInfo);
        testCluster.startAwait();

        ClusterService node1 = testCluster.members.get(0);
        ClusterService node2 = testCluster.members.get(1);

        var testMessageFuture1 = new CompletableFuture<NetworkMessage>();
        var testMessageFuture2 = new CompletableFuture<NetworkMessage>();
        var networkMessageFuture = new CompletableFuture<NetworkMessage>();

        // Register multiple handlers for the same group.
        node1.messagingService().addMessageHandler(
                TestMessageTypes.class,
                (message, sender, correlationId) -> assertTrue(testMessageFuture1.complete(message))
        );

        node1.messagingService().addMessageHandler(
                TestMessageTypes.class,
                (message, sender, correlationId) -> assertTrue(testMessageFuture2.complete(message))
        );

        // Register a different handle for the second group.
        node1.messagingService().addMessageHandler(
                NetworkMessageTypes.class,
                (message, sender, correlationId) -> {
                    if (message instanceof HandshakeFinishMessage) {
                        assertTrue(networkMessageFuture.complete(message));
                    }
                }
        );

        var testMessage = messageFactory.testMessage().msg("foo").build();

        HandshakeFinishMessage networkMessage = new NetworkMessagesFactory().handshakeFinishMessage().build();

        // Test that a message gets delivered to both handlers.
        node2.messagingService()
                .send(node1.topologyService().localMember(), testMessage)
                .get(1, TimeUnit.SECONDS);

        // Test that a message from the other group is only delivered to a single handler.
        node2.messagingService()
                .send(node1.topologyService().localMember(), networkMessage)
                .get(1, TimeUnit.SECONDS);

        assertThat(testMessageFuture1, willBe(equalTo(testMessage)));
        assertThat(testMessageFuture2, willBe(equalTo(testMessage)));
        assertThat(networkMessageFuture, willBe(equalTo(networkMessage)));
    }

    /**
     * Makes sure that a node that dropped out from the Physical Topology cannot reappear with same ID.
     *
     * @throws Exception in case of errors.
     */
    @SuppressWarnings("ConstantConditions")
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void nodeCannotReuseOldId(boolean keepPreExistingConnections) throws Exception {
        testCluster = new Cluster(3, testInfo);

        testCluster.startAwait();

        String outcastName = testCluster.members.get(testCluster.members.size() - 1).nodeName();

        knockOutNode(outcastName, !keepPreExistingConnections);

        IgniteBiTuple<CountDownLatch, AtomicBoolean> pair = reanimateNode(outcastName);
        CountDownLatch ready = pair.get1();
        AtomicBoolean reappeared = pair.get2();

        assertTrue(ready.await(10, TimeUnit.SECONDS), "Node neither reappeared, nor was rejected");

        assertThat(reappeared.get(), is(false));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void nodeCannotCommunicateAfterLeavingPhysicalTopology(boolean keepPreExistingConnections) throws Exception {
        testCluster = new Cluster(3, testInfo);

        testCluster.startAwait();

        ClusterService notOutcast = testCluster.members.get(0);
        ClusterService outcast = testCluster.members.get(testCluster.members.size() - 1);

        ClusterNode outcastNode = notOutcast.topologyService().getByConsistentId(outcast.nodeName());
        ClusterNode notOutcastNode = outcast.topologyService().getByConsistentId(notOutcast.nodeName());
        assertNotNull(outcastNode);
        assertNotNull(notOutcastNode);

        if (keepPreExistingConnections) {
            assertThat(notOutcast.messagingService().send(outcastNode, messageFactory.testMessage().build()), willCompleteSuccessfully());
            assertThat(outcast.messagingService().send(notOutcastNode, messageFactory.testMessage().build()), willCompleteSuccessfully());
        }

        knockOutNode(outcast.nodeName(), !keepPreExistingConnections);

        stopDroppingMessagesTo(outcast.nodeName());

        CompletableFuture<Void> sendFromOutcast = outcast.messagingService().send(notOutcastNode, messageFactory.testMessage().build());
        assertThat(sendFromOutcast, willThrow(HandshakeException.class));

        CompletableFuture<?> invokeFromOutcast = outcast.messagingService().invoke(notOutcastNode, messageFactory.testMessage().build(), 10_000);
        assertThat(invokeFromOutcast, willThrow(HandshakeException.class));

        CompletableFuture<Void> sendToOutcast = notOutcast.messagingService().send(outcastNode, messageFactory.testMessage().build());
        assertThat(sendToOutcast, willThrow(HandshakeException.class));

        CompletableFuture<?> invokeToOutcast = notOutcast.messagingService().invoke(outcastNode, messageFactory.testMessage().build(), 10_000);
        assertThat(invokeToOutcast, willThrow(HandshakeException.class));
    }

    private void knockOutNode(String outcastName, boolean closeConnectionsForcibly) throws InterruptedException {
        CountDownLatch disappeared = new CountDownLatch(2);

        TopologyEventHandler disappearListener = new TopologyEventHandler() {
            @Override
            public void onDisappeared(ClusterNode member) {
                if (Objects.equals(member.name(), outcastName)) {
                    disappeared.countDown();
                }
            }
        };

        List<ClusterService> notOutcasts = testCluster.members.stream()
                .filter(service -> !outcastName.equals(service.nodeName()))
                .collect(toList());

        notOutcasts.forEach(clusterService -> {
            clusterService.topologyService().addEventHandler(disappearListener);
        });

        notOutcasts.forEach(service -> {
            DefaultMessagingService messagingService = (DefaultMessagingService) service.messagingService();
            messagingService.dropMessages((recipientConsistentId, message) -> outcastName.equals(recipientConsistentId));
        });

        // Wait until all nodes see disappearance of the outcast.
        assertTrue(disappeared.await(10, TimeUnit.SECONDS), "Node did not disappear in time");

        DefaultMessagingService messagingService = (DefaultMessagingService) testCluster.members.stream()
                .filter(service -> outcastName.equals(service.nodeName()))
                .findFirst().orElseThrow()
                .messagingService();

        ConnectionManager cm = messagingService.connectionManager();

        if (closeConnectionsForcibly) {
            // Forcefully close channels, so that nodes will create new channels on reanimation of the outcast.
            cm.channels().forEach((stringConnectorKey, nettySender) -> {
                nettySender.close().awaitUninterruptibly();
            });
        }
    }

    private IgniteBiTuple<CountDownLatch, AtomicBoolean> reanimateNode(String outcastName) {
        CountDownLatch ready = new CountDownLatch(1);
        AtomicBoolean reappeared = new AtomicBoolean(false);

        testCluster.members.get(0).topologyService().addEventHandler(new TopologyEventHandler() {
            @Override
            public void onAppeared(ClusterNode member) {
                if (Objects.equals(member.name(), outcastName)) {
                    reappeared.compareAndSet(false, true);

                    ready.countDown();
                }
            }
        });

        Predicate<LogEvent> matcher = evt -> evt.getMessage().getFormattedMessage().startsWith("Handshake rejected by ");

        logInspectors.add(new LogInspector(
                RecoveryClientHandshakeManager.class.getName(),
                matcher,
                () -> ready.countDown()));

        logInspectors.add(new LogInspector(
                RecoveryServerHandshakeManager.class.getName(),
                matcher,
                () -> ready.countDown()));

        logInspectors.forEach(LogInspector::start);

        stopDroppingMessagesTo(outcastName);

        return new IgniteBiTuple<>(ready, reappeared);
    }

    private void stopDroppingMessagesTo(String outcastName) {
        testCluster.members.stream()
                .filter(service -> !outcastName.equals(service.nodeName()))
                .forEach(service -> {
                    DefaultMessagingService messagingService = (DefaultMessagingService) service.messagingService();
                    messagingService.stopDroppingMessages();
                });
    }

    /**
     * Tests shutdown.
     *
     * @param testInfo Test info.
     * @param forceful Whether shutdown should be forceful.
     * @throws Exception If failed.
     */
    private void testShutdown0(TestInfo testInfo, boolean forceful) throws Exception {
        testCluster = new Cluster(2, testInfo);
        testCluster.startAwait();

        ClusterService alice = testCluster.members.get(0);
        ClusterService bob = testCluster.members.get(1);
        String aliceName = alice.nodeName();

        var aliceShutdownLatch = new CountDownLatch(1);

        bob.topologyService().addEventHandler(new TopologyEventHandler() {
            /** {@inheritDoc} */
            @Override
            public void onDisappeared(ClusterNode member) {
                if (aliceName.equals(member.name())) {
                    aliceShutdownLatch.countDown();
                }
            }
        });

        if (forceful) {
            stopForcefully(alice);
        } else {
            alice.stop();
        }

        boolean aliceShutdownReceived = aliceShutdownLatch.await(forceful ? 10 : 3, TimeUnit.SECONDS);
        assertTrue(aliceShutdownReceived);

        Collection<ClusterNode> networkMembers = bob.topologyService().allMembers();

        assertEquals(1, networkMembers.size());
    }

    /**
     * Find the cluster's transport and force it to stop.
     *
     * @param cluster Cluster to be shutdown.
     * @throws Exception If failed to stop.
     */
    private static void stopForcefully(ClusterService cluster) throws Exception {
        Field clusterSvcImplField = cluster.getClass().getDeclaredField("val$clusterSvc");
        clusterSvcImplField.setAccessible(true);

        ClusterService innerClusterSvc = (ClusterService) clusterSvcImplField.get(cluster);

        Field clusterImplField = innerClusterSvc.getClass().getDeclaredField("cluster");
        clusterImplField.setAccessible(true);

        ClusterImpl clusterImpl = (ClusterImpl) clusterImplField.get(innerClusterSvc);
        Field transportField = clusterImpl.getClass().getDeclaredField("transport");
        transportField.setAccessible(true);

        Transport transport = (Transport) transportField.get(clusterImpl);
        Method stop = transport.getClass().getDeclaredMethod("stop");
        stop.setAccessible(true);

        Mono<?> invoke = (Mono<?>) stop.invoke(transport);
        invoke.block();
    }

    /**
     * Wrapper for a cluster.
     */
    private static final class Cluster {
        /** Members of the cluster. */
        final List<ClusterService> members;

        /** Node finder. */
        private final NodeFinder nodeFinder;

        /**
         * Creates a test cluster with the given amount of members.
         *
         * @param numOfNodes Amount of cluster members.
         * @param testInfo   Test info.
         */
        Cluster(int numOfNodes, TestInfo testInfo) {
            int initialPort = 3344;

            List<NetworkAddress> addresses = findLocalAddresses(initialPort, initialPort + numOfNodes);

            this.nodeFinder = new StaticNodeFinder(addresses);

            members = addresses.stream()
                    .map(addr -> startNode(testInfo, addr))
                    .collect(toUnmodifiableList());
        }

        /**
         * Start cluster node.
         *
         * @param testInfo Test info.
         * @param addr     Node address.
         * @return Started cluster node.
         */
        private ClusterService startNode(TestInfo testInfo, NetworkAddress addr) {
            return ClusterServiceTestUtils.clusterService(testInfo, addr.port(), nodeFinder);
        }

        /**
         * Starts and waits for the cluster to come up.
         *
         * @throws InterruptedException If failed.
         * @throws AssertionError       If the cluster was unable to start in 3 seconds.
         */
        void startAwait() throws InterruptedException {
            members.forEach(ClusterService::start);

            if (!waitForCondition(this::allMembersSeeEachOther, TimeUnit.SECONDS.toMillis(3))) {
                throw new AssertionError();
            }
        }

        private boolean allMembersSeeEachOther() {
            int totalMembersSeen = members.stream()
                    .mapToInt(m -> m.topologyService().allMembers().size())
                    .sum();
            return totalMembersSeen == members.size() * members.size();
        }

        /**
         * Stops the cluster.
         */
        void shutdown() {
            members.forEach(ClusterService::stop);
        }
    }
}
