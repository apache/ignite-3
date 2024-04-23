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

package org.apache.ignite.internal.network.scalecube;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.findLocalAddresses;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutIn;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.transport.api.Transport;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.DefaultMessagingService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageTypes;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.NodeFinder;
import org.apache.ignite.internal.network.OutNetworkObject;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.handshake.HandshakeException;
import org.apache.ignite.internal.network.messages.TestMessage;
import org.apache.ignite.internal.network.messages.TestMessageTypes;
import org.apache.ignite.internal.network.messages.TestMessagesFactory;
import org.apache.ignite.internal.network.netty.ConnectionManager;
import org.apache.ignite.internal.network.netty.ConnectorKey;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.internal.network.netty.OutgoingAcknowledgementSilencer;
import org.apache.ignite.internal.network.recovery.RecoveryClientHandshakeManager;
import org.apache.ignite.internal.network.recovery.RecoveryServerHandshakeManager;
import org.apache.ignite.internal.network.recovery.message.HandshakeFinishMessage;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.logging.log4j.core.LogEvent;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;

/**
 * Integration tests for messaging based on ScaleCube.
 */
class ItScaleCubeNetworkMessagingTest {
    /** Message sent to establish a connection. */
    private static final String TRAILBLAZER = "trailblazer";

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
    public void messageWasSentToAllMembersSuccessfully() throws Exception {
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

        var testMessage = testMessage("Message from Alice");

        ClusterService alice = testCluster.members.get(0);

        for (ClusterNode member : alice.topologyService().allMembers()) {
            alice.messagingService().weakSend(member, testMessage);
        }

        boolean messagesReceived = messageReceivedLatch.await(3, SECONDS);
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
    public void testShutdown() throws Exception {
        testShutdown0(false);
    }

    /**
     * Tests a forceful shutdown.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testForcefulShutdown() throws Exception {
        testShutdown0(true);
    }

    /**
     * Sends a message from a node to itself and verifies that it gets delivered successfully.
     *
     * @throws Exception in case of errors.
     */
    @Test
    public void testSendMessageToSelf() throws Exception {
        testCluster = new Cluster(1, testInfo);
        testCluster.startAwait();

        ClusterService member = testCluster.members.get(0);

        ClusterNode self = member.topologyService().localMember();

        class Data {
            private final TestMessage message;

            private final String senderConsistentId;

            @Nullable
            private final Long correlationId;

            private Data(TestMessage message, String senderConsistentId, @Nullable Long correlationId) {
                this.message = message;
                this.senderConsistentId = senderConsistentId;
                this.correlationId = correlationId;
            }
        }

        var dataFuture = new CompletableFuture<Data>();

        member.messagingService().addMessageHandler(
                TestMessageTypes.class,
                (message, sender, correlationId) ->
                        dataFuture.complete(new Data((TestMessage) message, sender.name(), correlationId))
        );

        TestMessage requestMessage = testMessage("request");

        member.messagingService().send(self, requestMessage);

        Data actualData = dataFuture.get(3, SECONDS);

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
    public void testInvokeMessageToSelf() throws Exception {
        testCluster = new Cluster(1, testInfo);
        testCluster.startAwait();

        ClusterService member = testCluster.members.get(0);

        ClusterNode self = member.topologyService().localMember();

        var requestMessage = testMessage("request");
        var responseMessage = testMessage("response");

        member.messagingService().addMessageHandler(
                TestMessageTypes.class,
                (message, sender, correlationId) -> {
                    if (correlationId != null && message.equals(requestMessage)) {
                        member.messagingService().respond(self, responseMessage, correlationId);
                    }
                }
        );

        TestMessage actualResponseMessage = member.messagingService()
                .invoke(self, requestMessage, 1000)
                .thenApply(TestMessage.class::cast)
                .get(3, SECONDS);

        assertThat(actualResponseMessage.msg(), is(responseMessage.msg()));
    }

    /**
     * Tests that if the network component is stopped before trying to make an "invoke" call, the corresponding future completes
     * exceptionally.
     */
    @Test
    public void testInvokeAfterStop() throws InterruptedException {
        testCluster = new Cluster(2, testInfo);
        testCluster.startAwait();

        ClusterService member0 = testCluster.members.get(0);
        ClusterService member1 = testCluster.members.get(1);

        assertThat(member0.stopAsync(), willCompleteSuccessfully());

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

        ExecutionException e = assertThrows(ExecutionException.class, () -> invoke0.get(1, SECONDS));

        assertThat(e.getCause(), instanceOf(NodeStoppingException.class));

        e = assertThrows(ExecutionException.class, () -> invoke1.get(1, SECONDS));

        assertThat(e.getCause(), instanceOf(NodeStoppingException.class));
    }

    /**
     * Tests that if the network component is stopped while making an "invoke" call, the corresponding future completes
     * exceptionally.
     */
    @Test
    public void testInvokeDuringStop() throws InterruptedException {
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

        assertThat(member0.stopAsync(), willCompleteSuccessfully());

        ExecutionException e = assertThrows(ExecutionException.class, () -> invoke0.get(1, SECONDS));

        assertThat(e.getCause(), instanceOf(NodeStoppingException.class));

        e = assertThrows(ExecutionException.class, () -> invoke1.get(1, SECONDS));

        assertThat(e.getCause(), instanceOf(NodeStoppingException.class));
    }

    /**
     * Tests that if the network component is stopped while waiting for a response to an "invoke" call, the corresponding future completes
     * exceptionally.
     */
    @Test
    public void testStopDuringAwaitingForInvokeResponse() throws InterruptedException {
        testCluster = new Cluster(2, testInfo);
        testCluster.startAwait();

        ClusterService member0 = testCluster.members.get(0);
        ClusterService member1 = testCluster.members.get(1);

        CountDownLatch receivedTestMessages = new CountDownLatch(2);

        member1.messagingService().addMessageHandler(
                TestMessageTypes.class,
                (message, sender, correlationId) -> receivedTestMessages.countDown()
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

        assertTrue(receivedTestMessages.await(10, SECONDS), "Did not receive invocations on the receiver in time");

        assertThat(member0.stopAsync(), willCompleteSuccessfully());

        ExecutionException e = assertThrows(ExecutionException.class, () -> invoke0.get(1, SECONDS));

        assertThat(e.getCause(), instanceOf(NodeStoppingException.class));

        e = assertThrows(ExecutionException.class, () -> invoke1.get(1, SECONDS));

        assertThat(e.getCause(), instanceOf(NodeStoppingException.class));
    }

    /**
     * Tests that Scalecube messages are not blocked if some message handler blocks handling of 'normal' messages.
     */
    @Test
    public void scalecubeMessagesAreSentSeparatelyFromOtherMessages() throws InterruptedException {
        testCluster = new Cluster(2, testInfo);
        testCluster.startAwait();

        ClusterService member0 = testCluster.members.get(0);
        ClusterService member1 = testCluster.members.get(1);

        CountDownLatch startedBlocking = new CountDownLatch(1);
        CountDownLatch blocking = new CountDownLatch(1);

        member1.messagingService().addMessageHandler(
                TestMessageTypes.class,
                (message, sender, correlationId) -> {
                    startedBlocking.countDown();

                    try {
                        blocking.await(10, SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
        );

        try {
            // Send a message that will block the inbound thread for normal messages.
            send(messageFactory.testMessage().build(), member0, member1);

            assertTrue(startedBlocking.await(10, SECONDS));

            CountDownLatch receivedScalecubeMessage = new CountDownLatch(1);

            member1.messagingService().addMessageHandler(
                    NetworkMessageTypes.class,
                    (message, sender, correlationId) -> receivedScalecubeMessage.countDown()
            );

            assertTrue(
                    receivedScalecubeMessage.await(10, SECONDS),
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
    public void testMessageGroupsHandlers() throws Exception {
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

        // Register a different handler for the second group.
        node1.messagingService().addMessageHandler(
                NetworkMessageTypes.class,
                (message, sender, correlationId) -> {
                    if (message instanceof HandshakeFinishMessage) {
                        assertTrue(networkMessageFuture.complete(message));
                    }
                }
        );

        var testMessage = testMessage("foo");

        HandshakeFinishMessage networkMessage = new NetworkMessagesFactory().handshakeFinishMessage().build();

        // Test that a message gets delivered to both handlers.
        send(testMessage, node2, node1)
                .get(1, SECONDS);

        // Test that a message from the other group is only delivered to a single handler.
        node2.messagingService()
                .send(node1.topologyService().localMember(), networkMessage)
                .get(1, SECONDS);

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

        assertTrue(ready.await(10, SECONDS), "Node neither reappeared, nor was rejected");

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
        assertThat(sendFromOutcast, either(willThrow(HandshakeException.class)).or(willThrow(RecipientLeftException.class)));

        CompletableFuture<?> invokeFromOutcast = outcast.messagingService().invoke(
                notOutcastNode,
                messageFactory.testMessage().build(),
                10_000
        );
        assertThat(invokeFromOutcast, either(willThrow(HandshakeException.class)).or(willThrow(RecipientLeftException.class)));

        CompletableFuture<Void> sendToOutcast = notOutcast.messagingService().send(outcastNode, messageFactory.testMessage().build());
        assertThat(sendToOutcast, either(willThrow(HandshakeException.class)).or(willThrow(RecipientLeftException.class)));

        CompletableFuture<?> invokeToOutcast = notOutcast.messagingService().invoke(
                outcastNode,
                messageFactory.testMessage().build(),
                10_000
        );
        assertThat(invokeToOutcast, either(willThrow(HandshakeException.class)).or(willThrow(RecipientLeftException.class)));
    }

    @Test
    public void reconnectsAfterConnectionDrop() throws Exception {
        testCluster = new Cluster(2, testInfo);

        testCluster.startAwait();

        ClusterService sender = testCluster.members.get(0);
        ClusterService receiver = testCluster.members.get(1);

        establishConnection(sender, receiver);

        closeAllChannels(sender.messagingService());

        receiver.messagingService().addMessageHandler(
                TestMessageTypes.class,
                (message, senderParam, correlationId) -> {
                    if (correlationId != null) {
                        receiver.messagingService().respond(sender.topologyService().localMember(), message, correlationId);
                    }
                }
        );

        // This must cause a reconnection.
        CompletableFuture<?> secondInvoke = invoke(messageFactory.testMessage().build(), sender, receiver);
        assertThat(secondInvoke, willCompleteSuccessfully());
    }

    /**
     * This is the scenario:
     *
     * <ol>
     *     <li>Messages get added to the queue of a channel</li>
     *     <li>Before they actually get sent, the channel closure is initiated</li>
     *     <li>But when the sends are processed by the event loop, the channel has still not closed fully (and it still holds
     *     the recovery descriptor)</li>
     * </ol>
     *
     * <p>The expected outcome is that the messages get delivered without any more external sends/invokes and that they
     * are delivered in the right order.
     */
    @Test
    public void messagesQueuedAtNonFullyClosedOldChannelGetDeliveredAfterReconnection() throws Exception {
        testCluster = new Cluster(2, testInfo);
        testCluster.startAwait();

        ClusterService sender = testCluster.members.get(0);
        ClusterService receiver = testCluster.members.get(1);

        List<String> receivedPayloads = new CopyOnWriteArrayList<>();
        collectReceivedPayloads(sender, receiver, receivedPayloads);

        establishConnection(sender, receiver);

        // Block work on the channel's event loop until we put close command and send commands on the loop.
        // As a result, after we unblock the loop, the sends will fail with ClosedChannelException in Netty.
        // We'll then observe whether this is handled transparently by opening a new channel and re-sending via new channel.
        NettySender defaultChannelSender = nettySenderForDefaultChannel(sender, receiver.nodeName());

        CountDownLatch proceedToClosing = new CountDownLatch(1);
        blockEventLoopWith(proceedToClosing, defaultChannelSender);

        CompletableFuture<Void> closeFuture = defaultChannelSender.closeAsync();

        CompletableFuture<?> sendViaOldChannel = send(testMessage("first"), sender, receiver);
        CompletableFuture<?> invokeViaOldChannel = invoke(testMessage("second"), sender, receiver);

        try {
            assertFalse(closeFuture.isDone());
            proceedToClosing.countDown();
            assertThat(closeFuture, willCompleteSuccessfully());

            assertThat(sendViaOldChannel, willSucceedIn(3, SECONDS));
            assertThat(invokeViaOldChannel, willSucceedIn(3, SECONDS));

            List<String> expectedPayloads = List.of("trailblazer", "first", "second");
            waitForCondition(() -> receivedPayloads.equals(expectedPayloads), 3_000);
            assertThat(receivedPayloads, equalTo(expectedPayloads));

            NettySender nettySender = nettySenderForDefaultChannel(sender, receiver.nodeName());
            assertThatHasNoUnacknowledgedMessages(nettySender);
        } finally {
            proceedToClosing.countDown();
        }
    }

    private static void collectReceivedPayloads(ClusterService sender, ClusterService receiver, List<String> receivedPayloads) {
        receiver.messagingService().addMessageHandler(
                TestMessageTypes.class,
                (message, senderParam, correlationId) -> {
                    if (message instanceof TestMessage) {
                        receivedPayloads.add(((TestMessage) message).msg());
                    }

                    if (correlationId != null) {
                        receiver.messagingService().respond(
                                sender.topologyService().localMember(),
                                message,
                                correlationId
                        );
                    }
                }
        );
    }

    private TestMessage testMessage(String message) {
        return messageFactory.testMessage().msg(message).build();
    }

    private static CompletableFuture<Void> send(TestMessage message, ClusterService sender, ClusterService receiver) {
        return sender.messagingService().send(
                receiver.topologyService().localMember(),
                message
        );
    }

    private static CompletableFuture<NetworkMessage> invoke(TestMessage message, ClusterService sender, ClusterService receiver) {
        return sender.messagingService().invoke(
                receiver.topologyService().localMember(),
                message,
                10_000
        );
    }

    private static void assertThatHasNoUnacknowledgedMessages(NettySender nettySender) {
        CompletableFuture<List<OutNetworkObject>> unackedMessagesFuture = new CompletableFuture<>();

        nettySender.channel().eventLoop().execute(() -> {
            unackedMessagesFuture.complete(nettySender.recoveryDescriptor().unacknowledgedMessages());
        });

        assertThat(unackedMessagesFuture, willBe(empty()));
    }

    private static NettySender nettySenderForDefaultChannel(ClusterService sender, String receiverConsistentId) {
        return connectionManager(sender).channels()
                .get(new ConnectorKey<>(receiverConsistentId, ChannelType.DEFAULT));
    }

    private static ConnectionManager connectionManager(ClusterService clusterService) {
        return ((DefaultMessagingService) clusterService.messagingService()).connectionManager();
    }

    /**
     * This is the scenario:
     *
     * <ol>
     *     <li>Messages get added to the queue of a channel</li>
     *     <li>Before they actually get sent, the channel closure is initiated</li>
     *     <li>When the sends are processed by the event loop, the channel has already been closed (and does not hold
     *     the recovery descriptor anymore)</li>
     *     <li>Additionally, we variate whether a new channel in the same logical connection has been opened or not</li>
     * </ol>
     *
     * <p>The expected outcome is that the messages get delivered without any more external sends/invokes and that they
     * are delivered in the right order.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void messagesQueuedOnFullyClosedOldChannelGetDeliveredAfterReconnection(boolean openNewChannelBeforeSendingToOld)
            throws Exception {
        testCluster = new Cluster(2, testInfo);
        testCluster.startAwait();

        ClusterService sender = testCluster.members.get(0);
        ClusterService receiver = testCluster.members.get(1);

        List<String> receivedPayloads = new CopyOnWriteArrayList<>();
        collectReceivedPayloads(sender, receiver, receivedPayloads);

        establishConnection(sender, receiver);

        NettySender defaultChannelSender = nettySenderForDefaultChannel(sender, receiver.nodeName());

        // Now close the sender completely.
        assertThat(defaultChannelSender.closeAsync(), willCompleteSuccessfully());

        if (openNewChannelBeforeSendingToOld) {
            establishConnectionWithoutSendingMessages(sender, receiver);
        }

        // Sending first message directly via NettySender. This is a hack, but there is no reliable way to hit a closed
        // sender via MessagingService methods as its internals try to avoid such a situation; only in a tight race can
        // this be reproduced.
        OutNetworkObject outViaOldChannel = new OutNetworkObject(testMessage("first"), List.of());
        defaultChannelSender.send(outViaOldChannel);
        CompletableFuture<?> sendViaNewChannel = send(testMessage("second"), sender, receiver);

        assertThat(outViaOldChannel.acknowledgedFuture(), willSucceedIn(3, SECONDS));
        assertThat(sendViaNewChannel, willSucceedIn(3, SECONDS));

        List<String> expectedPayloads = List.of("trailblazer", "first", "second");
        waitForCondition(() -> receivedPayloads.equals(expectedPayloads), 3_000);
        assertThat(receivedPayloads, equalTo(expectedPayloads));

        NettySender nettySender = nettySenderForDefaultChannel(sender, receiver.nodeName());
        assertThatHasNoUnacknowledgedMessages(nettySender);
    }

    private static void establishConnectionWithoutSendingMessages(ClusterService sender, ClusterService receiver) {
        NetworkAddress receiverAddress = receiver.topologyService().localMember().address();
        CompletableFuture<NettySender> newSenderFuture = connectionManager(sender).channel(
                receiver.nodeName(),
                ChannelType.DEFAULT,
                new InetSocketAddress(receiverAddress.host(), receiverAddress.port())
        ).toCompletableFuture();

        assertThat(newSenderFuture, willCompleteSuccessfully());
    }

    /**
     * This is the scenario:
     *
     * <ol>
     *     <li>Messages get added to the queue of a channel</li>
     *     <li>Before they actually get sent, the channel is closed</li>
     *     <li>New messages are sent, which spawns a new channel</li>
     * </ol>>
     *
     * <p>The expected outcome is that all the messages get delivered in the right order.
     */
    @Test
    public void messagesQueuedOnOldChannelGetDeliveredBeforeMessagesSentViaNewChannel() throws Exception {
        testCluster = new Cluster(2, testInfo);
        testCluster.startAwait();

        ClusterService sender = testCluster.members.get(0);
        ClusterService receiver = testCluster.members.get(1);

        List<String> receivedPayloads = new CopyOnWriteArrayList<>();
        collectReceivedPayloads(sender, receiver, receivedPayloads);

        establishConnection(sender, receiver);

        // Block work on the channel's event loop until we put close command and send commands on the loop.
        // As a result, after we unblock the loop, the sends will fail with ClosedChannelException in Netty.
        // We'll then observe whether this is handled transparently by opening a new channel and re-sending via new channel.
        NettySender defaultChannelSender = nettySenderForDefaultChannel(sender, receiver.nodeName());

        CountDownLatch proceedToClosing = new CountDownLatch(1);
        blockEventLoopWith(proceedToClosing, defaultChannelSender);

        CompletableFuture<Void> closeFuture = defaultChannelSender.closeAsync();

        CountDownLatch proceedToSendingViaOldChannel = new CountDownLatch(1);
        blockEventLoopWith(proceedToSendingViaOldChannel, defaultChannelSender);

        CompletableFuture<?> sendViaOldChannel = send(testMessage("first"), sender, receiver);
        CompletableFuture<?> invokeViaOldChannel = invoke(testMessage("second"), sender, receiver);

        assertFalse(closeFuture.isDone());
        proceedToClosing.countDown();
        assertThat(closeFuture, willCompleteSuccessfully());

        assertFalse(sendViaOldChannel.isDone());
        assertFalse(invokeViaOldChannel.isDone());

        CompletableFuture<?> sendViaNewChannel = send(testMessage("third"), sender, receiver);
        CompletableFuture<?> invokeViaNewChannel = invoke(testMessage("fourth"), sender, receiver);

        try {
            assertFalse(sendViaOldChannel.isDone());
            assertFalse(invokeViaOldChannel.isDone());

            proceedToSendingViaOldChannel.countDown();

            assertThat(sendViaOldChannel, willCompleteSuccessfully());
            assertThat(invokeViaOldChannel, willCompleteSuccessfully());
            assertThat(sendViaNewChannel, willCompleteSuccessfully());
            assertThat(invokeViaNewChannel, willCompleteSuccessfully());

            List<String> expectedPayloads = List.of("trailblazer", "first", "second", "third", "fourth");
            waitForCondition(() -> receivedPayloads.equals(expectedPayloads), 3_000);
            assertThat(receivedPayloads, equalTo(expectedPayloads));

            NettySender nettySender = nettySenderForDefaultChannel(sender, receiver.nodeName());
            assertThatHasNoUnacknowledgedMessages(nettySender);
        } finally {
            proceedToClosing.countDown();
            proceedToSendingViaOldChannel.countDown();
        }
    }

    private void establishConnection(ClusterService sender, ClusterService receiver) {
        CompletableFuture<?> future = send(testMessage(TRAILBLAZER), sender, receiver);

        assertThat(future, willCompleteSuccessfully());
    }

    private static void blockEventLoopWith(CountDownLatch release, NettySender nettySender) {
        nettySender.channel().eventLoop().execute(() -> {
            try {
                release.await(10, SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void doesNotDeliverMessagesWhoseSenderLeftPhysicalTopology() throws Exception {
        testCluster = new Cluster(2, testInfo);

        testCluster.startAwait();

        ClusterService sender = testCluster.members.get(0);
        ClusterService receiver = testCluster.members.get(1);

        // We are going to send 3 messages, of which 2 will arrive after the sender has been removed from the physical topology,
        // so we expect 2 messages to be 'skipped' and not delivered on the receiver.

        AtomicBoolean first = new AtomicBoolean(true);
        CountDownLatch canProceed = new CountDownLatch(1);
        CountDownLatch blockingStarted = new CountDownLatch(1);
        AtomicInteger messagesDelivered = new AtomicInteger();

        receiver.messagingService().addMessageHandler(
                TestMessageTypes.class,
                (message, senderParam, correlationId) -> {
                    if (first.compareAndSet(true, false)) {
                        blockingStarted.countDown();

                        try {
                            assertTrue(canProceed.await(10, SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    }

                    messagesDelivered.incrementAndGet();
                }
        );

        TestMessage message = messageFactory.testMessage().build();

        // This message will get stuck.
        send(message, sender, receiver);

        // These 2 will be handled after the sender has left the physical topology.
        send(message, sender, receiver);
        invoke(message, sender, receiver);

        assertTrue(blockingStarted.await(10, SECONDS));

        knockOutNode(sender.nodeName(), false);

        canProceed.countDown();

        assertTrue(waitForCondition(() -> messagesDelivered.get() >= 1, 10_000));

        // Let other messages a chance to be delivered.
        Thread.sleep(300);

        assertThat(messagesDelivered.get(), is(1));
    }

    private static void closeAllChannels(MessagingService messagingService) {
        ConnectionManager connectionManager = ((DefaultMessagingService) messagingService).connectionManager();

        for (NettySender sender : connectionManager.channels().values()) {
            assertThat(sender.closeAsync(), willCompleteSuccessfully());
        }
    }

    @ParameterizedTest
    @EnumSource(SendOperation.class)
    public void messageSendFuturesGetCompleteWhenAcknowledgementHappens(SendOperation operation) throws Exception {
        testCluster = new Cluster(2, testInfo);

        testCluster.startAwait();

        ClusterService sender = testCluster.members.get(0);
        ClusterService receiver = testCluster.members.get(1);

        echoMessagesBackAt(receiver);

        // Open a channel to allow a silencer to be installed on it.
        openDefaultChannelBetween(sender, receiver);
        OutgoingAcknowledgementSilencer ackSilencer = dropAcksWhenDefaultChannelOpens(receiver);

        CompletableFuture<Void> sendFuture = operation.send(
                sender.messagingService(),
                messageFactory.testMessage().build(),
                receiver.topologyService().localMember()
        );
        assertThat(sendFuture, willTimeoutIn(100, TimeUnit.MILLISECONDS));

        ackSilencer.stopSilencing();

        provokeAckFor(sender, receiver);

        assertThat(sendFuture, willCompleteSuccessfully());
    }

    private static void echoMessagesBackAt(ClusterService clusterService) {
        clusterService.messagingService().addMessageHandler(
                TestMessageTypes.class,
                (message, sender, correlationId) -> {
                    if (correlationId != null) {
                        clusterService.messagingService().respond(sender, message, correlationId);
                    }
                }
        );
    }

    private void openDefaultChannelBetween(ClusterService sender, ClusterService receiver) {
        CompletableFuture<Void> future = sendMessage(sender, receiver);

        assertThat(future, willCompleteSuccessfully());
    }

    private CompletableFuture<Void> sendMessage(ClusterService sender, ClusterService receiver) {
        return send(messageFactory.testMessage().build(), sender, receiver);
    }

    private static OutgoingAcknowledgementSilencer dropAcksWhenDefaultChannelOpens(ClusterService clusterService)
            throws InterruptedException {
        DefaultMessagingService messagingService = (DefaultMessagingService) clusterService.messagingService();

        ConnectionManager connectionManager = messagingService.connectionManager();
        assertTrue(
                waitForCondition(
                        () -> connectionManager.channels().keySet().stream().anyMatch(key -> key.type() == ChannelType.DEFAULT),
                        SECONDS.toMillis(10)
                ),
                "Did not see a default channel to be opened on the connection manager"
        );

        return OutgoingAcknowledgementSilencer.installOn(connectionManager.channels().values());
    }

    private void provokeAckFor(ClusterService sideToGetAck, ClusterService sideToSendAck) {
        sendMessage(sideToGetAck, sideToSendAck);
    }

    @ParameterizedTest
    @EnumSource(SendOperation.class)
    public void sendFutureFailsWhenReceiverLeavesPhysicalTopology(SendOperation operation) throws Exception {
        testCluster = new Cluster(2, testInfo);

        testCluster.startAwait();

        ClusterService notOutcast = testCluster.members.get(0);
        ClusterService outcast = testCluster.members.get(testCluster.members.size() - 1);

        echoMessagesBackAt(outcast);

        openDefaultChannelBetween(notOutcast, outcast);
        dropAcksWhenDefaultChannelOpens(outcast);

        CompletableFuture<Void> sendFuture = operation.send(
                notOutcast.messagingService(),
                messageFactory.testMessage().build(),
                outcast.topologyService().localMember()
        );

        knockOutNode(outcast.nodeName(), false);

        assertThat(sendFuture, willThrow(RecipientLeftException.class));
    }

    @ParameterizedTest
    @EnumSource(SendOperation.class)
    public void sendFutureFailsWhenSenderNodeStops(SendOperation operation) throws Exception {
        testCluster = new Cluster(2, testInfo);

        testCluster.startAwait();

        ClusterService sender = testCluster.members.get(0);
        ClusterService receiver = testCluster.members.get(testCluster.members.size() - 1);

        echoMessagesBackAt(receiver);

        openDefaultChannelBetween(sender, receiver);
        dropAcksWhenDefaultChannelOpens(receiver);

        CompletableFuture<Void> sendFuture = operation.send(
                sender.messagingService(),
                messageFactory.testMessage().build(),
                receiver.topologyService().localMember()
        );

        assertThat(sender.stopAsync(), willCompleteSuccessfully());

        assertThat(sendFuture, willThrow(NodeStoppingException.class));
    }

    private void knockOutNode(String outcastName, boolean closeConnectionsForcibly) throws InterruptedException {
        CountDownLatch disappeared = new CountDownLatch(testCluster.members.size() - 1);

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
        assertTrue(disappeared.await(10, SECONDS), "Node did not disappear in time");

        if (closeConnectionsForcibly) {
            MessagingService messagingService = testCluster.members.stream()
                    .filter(service -> outcastName.equals(service.nodeName()))
                    .findFirst().orElseThrow()
                    .messagingService();

            // Forcefully close channels, so that nodes will create new channels on reanimation of the outcast.
            closeAllChannels(messagingService);
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
                ready::countDown));

        logInspectors.add(new LogInspector(
                RecoveryServerHandshakeManager.class.getName(),
                matcher,
                ready::countDown));

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
     * @param forceful Whether shutdown should be forceful.
     * @throws Exception If failed.
     */
    private void testShutdown0(boolean forceful) throws Exception {
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
            assertThat(alice.stopAsync(), willCompleteSuccessfully());
        }

        boolean aliceShutdownReceived = aliceShutdownLatch.await(forceful ? 10 : 3, SECONDS);
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
            CompletableFuture<Void> future = allOf(members.stream().map(ClusterService::startAsync).toArray(CompletableFuture[]::new));

            assertThat(future, willCompleteSuccessfully());

            if (!waitForCondition(this::allMembersSeeEachOther, SECONDS.toMillis(3))) {
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
            CompletableFuture<Void> future = allOf(members.stream().map(ClusterService::stopAsync).toArray(CompletableFuture[]::new));

            assertThat(future, willCompleteSuccessfully());
        }
    }

    private enum SendOperation {
        SEND {
            @Override
            CompletableFuture<Void> send(MessagingService messagingService, NetworkMessage message, ClusterNode recipient) {
                return messagingService.send(recipient, message);
            }
        },
        INVOKE {
            @Override
            CompletableFuture<Void> send(MessagingService messagingService, NetworkMessage message, ClusterNode recipient) {
                return messagingService.invoke(recipient, message, Long.MAX_VALUE).thenApply(unused -> null);
            }
        };

        abstract CompletableFuture<Void> send(MessagingService messagingService, NetworkMessage message, ClusterNode recipient);
    }
}
