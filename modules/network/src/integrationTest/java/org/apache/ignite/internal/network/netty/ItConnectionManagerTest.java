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

package org.apache.ignite.internal.network.netty;

import static java.util.Collections.emptyList;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.defaultSerializationRegistry;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureCompletedMatcher.completedFuture;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutIn;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.handler.codec.DecoderException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.future.OrderingFuture;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.OutNetworkObject;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.configuration.NetworkView;
import org.apache.ignite.internal.network.messages.TestMessage;
import org.apache.ignite.internal.network.messages.TestMessagesFactory;
import org.apache.ignite.internal.network.recovery.AllIdsAreFresh;
import org.apache.ignite.internal.network.recovery.message.HandshakeFinishMessage;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.network.serialization.SerializationService;
import org.apache.ignite.internal.network.serialization.UserObjectSerializationContext;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link ConnectionManager}.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItConnectionManagerTest extends BaseIgniteAbstractTest {
    /** Started connection managers. */
    private final List<ConnectionManagerWrapper> startedManagers = new ArrayList<>();

    /** Message factory. */
    private final TestMessagesFactory messageFactory = new TestMessagesFactory();

    private final TestMessage emptyTestMessage = messageFactory.testMessage().build();

    /** Reusable network configuration object. */
    @InjectConfiguration
    private NetworkConfiguration networkConfiguration;

    private TestInfo testInfo;

    @BeforeEach
    void setTestInfo(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    /**
     * After each.
     */
    @AfterEach
    final void tearDown() throws Exception {
        IgniteUtils.closeAll(startedManagers);
    }

    /**
     * Tests that a message is sent successfully using the ConnectionManager.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSentSuccessfully() throws Exception {
        String msgText = "test";

        int port1 = 4000;
        int port2 = 4001;

        try (ConnectionManagerWrapper manager1 = startManager(port1);
                ConnectionManagerWrapper manager2 = startManager(port2)) {
            var fut = new CompletableFuture<NetworkMessage>();

            manager2.connectionManager.addListener((obj) -> fut.complete(obj.message()));

            NettySender sender = manager1.openChannelTo(manager2).get(3, TimeUnit.SECONDS);

            TestMessage testMessage = messageFactory.testMessage().msg(msgText).build();

            sender.send(new OutNetworkObject(testMessage, emptyList())).get(3, TimeUnit.SECONDS);

            NetworkMessage receivedMessage = fut.get(3, TimeUnit.SECONDS);

            assertEquals(msgText, ((TestMessage) receivedMessage).msg());
        }
    }

    /**
     * Tests that incoming connection is reused for sending messages.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReuseIncomingConnection() throws Exception {
        final String msgText = "test";

        TestMessage testMessage = messageFactory.testMessage().msg("test").build();

        int port1 = 4000;
        int port2 = 4001;

        try (ConnectionManagerWrapper manager1 = startManager(port1);
                ConnectionManagerWrapper manager2 = startManager(port2)) {
            var receivedAt1 = new CompletableFuture<NetworkMessage>();
            manager1.connectionManager.addListener((obj) -> receivedAt1.complete(obj.message()));

            NettySender senderFrom1to2 = manager1.openChannelTo(manager2).get(3, TimeUnit.SECONDS);
            assertThat(senderFrom1to2, is(notNullValue()));

            // Ensure a handshake has finished on both sides by sending a message.
            assertThat(senderFrom1to2.send(new OutNetworkObject(testMessage, emptyList())), willCompleteSuccessfully());

            NettySender senderFrom2to1 = manager2.openChannelTo(manager1).get(3, TimeUnit.SECONDS);
            assertThat(senderFrom2to1, is(notNullValue()));

            InetSocketAddress clientLocalAddress = (InetSocketAddress) senderFrom1to2.channel().localAddress();
            InetSocketAddress clientRemoteAddress = (InetSocketAddress) senderFrom2to1.channel().remoteAddress();

            assertEquals(clientLocalAddress, clientRemoteAddress);

            assertThat(
                    senderFrom2to1.send(new OutNetworkObject(messageFactory.testMessage().msg("2->1").build(), emptyList())),
                    willCompleteSuccessfully()
            );

            NetworkMessage receivedMessage = receivedAt1.get(3, TimeUnit.SECONDS);

            assertThat(((TestMessage) receivedMessage).msg(), is("2->1"));
        }
    }

    /**
     * Tests that the resources of a connection manager are closed after a shutdown.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testShutdown() throws Exception {
        int port1 = 4000;
        int port2 = 4001;

        ConnectionManagerWrapper manager1 = startManager(port1);
        ConnectionManagerWrapper manager2 = startManager(port2);

        NettySender sender1 = manager1.openChannelTo(manager2).get(3, TimeUnit.SECONDS);

        // Wait for the channel to appear on the recipient side.
        waitForCondition(() -> !manager2.channels().isEmpty(), 10_000);

        NettySender sender2 = manager2.openChannelTo(manager1).get(3, TimeUnit.SECONDS);

        assertNotNull(sender1);
        assertNotNull(sender2);

        for (ConnectionManagerWrapper manager : List.of(manager1, manager2)) {
            NettyServer server = manager.connectionManager.server();
            Collection<NettyClient> clients = manager.connectionManager.clients();

            manager.close();

            assertFalse(server.isRunning());

            boolean clientsStopped = clients.stream().allMatch(NettyClient::isDisconnected);

            assertTrue(clientsStopped);
        }
    }

    /**
     * Tests that after a channel was closed, a new channel is opened upon a request.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCanReconnectAfterFail() throws Exception {
        String msgText = "test";

        int port1 = 4000;
        int port2 = 4001;
        UUID launchId2 = UUID.randomUUID();

        ConnectionManagerWrapper manager1 = startManager(port1);

        ConnectionManagerWrapper manager2 = startManager(port2, launchId2);

        NettySender sender = manager1.openChannelTo(manager2).get(300, TimeUnit.SECONDS);
        assertNotNull(sender);

        TestMessage testMessage = messageFactory.testMessage().msg(msgText).build();

        manager2.close();

        sender.send(new OutNetworkObject(testMessage, emptyList()));

        manager2 = startManager(port2, launchId2);

        var latch = new CountDownLatch(2);

        manager2.connectionManager.addListener((obj) -> {
            if (testMessage.equals(obj.message())) {
                latch.countDown();
            }
        });

        sender = manager1.openChannelTo(manager2).get(3, TimeUnit.SECONDS);
        assertNotNull(sender);

        sender.send(new OutNetworkObject(testMessage, emptyList())).get(3, TimeUnit.SECONDS);

        assertTrue(latch.await(3, TimeUnit.SECONDS));
    }

    /**
     * Tests that a connection to a misconfigured server results in a connection close and an exception on the client side.
     */
    @Test
    public void testConnectMisconfiguredServer() throws Exception {
        ConnectionManagerWrapper client = startManager(4000);

        try (ConnectionManagerWrapper server = startManager(4001, mockSerializationRegistry())) {
            client.openChannelTo(server).get(3, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            assertThat(e.getCause(), isA(IOException.class));
        }
    }

    /**
     * Tests that a connection from a misconfigured client results in an exception.
     */
    @Test
    public void testConnectMisconfiguredClient() throws Exception {
        ConnectionManagerWrapper client = startManager(4000, mockSerializationRegistry());

        try (ConnectionManagerWrapper server = startManager(4001)) {
            client.openChannelTo(server).get(3, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            assertThat(e.getCause(), isA(DecoderException.class));
        }
    }

    /**
     * Tests that a connection manager fails to start twice.
     */
    @Test
    public void testStartTwice() throws Exception {
        ConnectionManagerWrapper server = startManager(4000);

        assertThrows(IgniteInternalException.class, server.connectionManager::start);
    }

    /**
     * Tests that a connection manager can be stopped twice.
     */
    @Test
    public void testStopTwice() throws Exception {
        ConnectionManagerWrapper server = startManager(4000);

        server.close();
        server.close();
    }

    /**
     * Tests that if two nodes are opening channels to each other, only one channel survives.
     *
     * @throws Exception If failed.
     */
    @RepeatedTest(100)
    @Timeout(10)
    public void testOneChannelLeftIfConnectToEachOther() throws Exception {
        try (
                ConnectionManagerWrapper manager1 = startManager(4000);
                ConnectionManagerWrapper manager2 = startManager(4001)
        ) {
            CompletableFuture<NettySender> fut1 = manager1.openChannelTo(manager2).toCompletableFuture();
            CompletableFuture<NettySender> fut2 = manager2.openChannelTo(manager1).toCompletableFuture();

            NettySender sender1 = fut1.get(1, TimeUnit.SECONDS);
            NettySender sender2 = fut2.get(1, TimeUnit.SECONDS);

            assertTrue(sender1.isOpen());
            assertTrue(sender2.isOpen());

            assertTrue(
                    waitForCondition(
                            () -> singleOpenedChannel(manager1) && singleOpenedChannel(manager2),
                            TimeUnit.SECONDS.toMillis(1)
                    )
            );

            CompletableFuture<NettySender> channelFut1 = manager1.connectionManager.channel(
                    manager2.connectionManager.consistentId(),
                    ChannelType.DEFAULT,
                    manager2.connectionManager.localAddress()
            ).toCompletableFuture();

            CompletableFuture<NettySender> channelFut2 = manager2.connectionManager.channel(
                    manager1.connectionManager.consistentId(),
                    ChannelType.DEFAULT,
                    manager1.connectionManager.localAddress()
            ).toCompletableFuture();

            assertThat(channelFut1, is(completedFuture()));
            assertThat(channelFut2, is(completedFuture()));

            NettySender channel1 = channelFut1.getNow(null);
            NettySender channel2 = channelFut2.getNow(null);

            InetSocketAddress locAddr1 = (InetSocketAddress) channel1.channel().localAddress();
            InetSocketAddress remoteAddr1 = (InetSocketAddress) channel1.channel().remoteAddress();

            InetSocketAddress locAddr2 = (InetSocketAddress) channel2.channel().localAddress();
            InetSocketAddress remoteAddr2 = (InetSocketAddress) channel2.channel().remoteAddress();

            // Only compare ports because hosts may look different, eg localhost and 0.0.0.0. They are technically not same,
            // although equal.
            assertEquals(locAddr1.getPort(), remoteAddr2.getPort());
            assertEquals(locAddr2.getPort(), remoteAddr1.getPort());
        }
    }

    private static boolean singleOpenedChannel(ConnectionManagerWrapper manager) {
        Iterator<NettySender> it = manager.channels().values().iterator();

        return it.hasNext() && it.next().isOpen() && !it.hasNext();
    }

    @Test
    public void sendFutureOfMessageNeedingAckCompletesWhenMessageGetsAcknowledged() throws Exception {
        try (
                ConnectionManagerWrapper manager1 = startManager(4000);
                ConnectionManagerWrapper manager2 = startManager(4001)
        ) {
            NettySender sender = manager1.openChannelTo(manager2).toCompletableFuture().get(10, TimeUnit.SECONDS);
            waitTillChannelAppearsInMapOnAcceptor(sender, manager1, manager2);

            OutgoingAcknowledgementSilencer ackSilencer = dropAcksFrom(manager2);

            CompletableFuture<Void> sendFuture = sender.send(new OutNetworkObject(emptyTestMessage, emptyList(), true));
            assertThat(sendFuture, willTimeoutIn(100, TimeUnit.MILLISECONDS));

            ackSilencer.stopSilencing();

            provokeAckFor(sender);

            assertThat(sendFuture, willCompleteSuccessfully());
        }
    }

    private static void waitTillChannelAppearsInMapOnAcceptor(
            NettySender senderFromOpener,
            ConnectionManagerWrapper opener,
            ConnectionManagerWrapper acceptor
    ) throws InterruptedException {
        assertTrue(
                waitForCondition(
                        () -> acceptor.channels().values().stream().anyMatch(acceptorSender
                                -> acceptorSender.consistentId().equals(opener.connectionManager.consistentId())
                                        && acceptorSender.channelId() == senderFromOpener.channelId()),
                        TimeUnit.SECONDS.toMillis(10)
                ),
                "Did not observe the sender appearing in the acceptor's sender map in time"
        );
    }

    @Test
    public void sendFuturesCompleteInSendOrder() throws Exception {
        try (
                ConnectionManagerWrapper manager1 = startManager(4000);
                ConnectionManagerWrapper manager2 = startManager(4001)
        ) {
            NettySender sender = manager1.openChannelTo(manager2).toCompletableFuture().get(10, TimeUnit.SECONDS);
            waitTillChannelAppearsInMapOnAcceptor(sender, manager1, manager2);

            OutgoingAcknowledgementSilencer ackSilencer = dropAcksFrom(manager2);

            List<Integer> ordinals = new CopyOnWriteArrayList<>();

            CompletableFuture<Void> future1 = sender.send(new OutNetworkObject(emptyTestMessage, emptyList(), true))
                    .whenComplete((res, ex) -> ordinals.add(1));
            CompletableFuture<Void> future2 = sender.send(new OutNetworkObject(emptyTestMessage, emptyList(), true))
                    .whenComplete((res, ex) -> ordinals.add(2));

            ackSilencer.stopSilencing();

            provokeAckFor(sender);

            assertThat(CompletableFuture.allOf(future1, future2), willCompleteSuccessfully());
            assertThat(ordinals, contains(1, 2));
        }
    }

    @Test
    public void sendFutureOfMessageNotNeedingAckCompletesWhenMessageGetsWritten() throws Exception {
        try (
                ConnectionManagerWrapper manager1 = startManager(4000);
                ConnectionManagerWrapper manager2 = startManager(4001)
        ) {
            NettySender sender = manager1.openChannelTo(manager2).toCompletableFuture().get(10, TimeUnit.SECONDS);
            waitTillChannelAppearsInMapOnAcceptor(sender, manager1, manager2);

            dropAcksFrom(manager2);

            HandshakeFinishMessage messageNotNeedingAck = new NetworkMessagesFactory().handshakeFinishMessage().build();
            CompletableFuture<Void> sendFuture = sender.send(new OutNetworkObject(messageNotNeedingAck, emptyList(), true));
            assertThat(sendFuture, willCompleteSuccessfully());
        }
    }

    private static OutgoingAcknowledgementSilencer dropAcksFrom(ConnectionManagerWrapper connectionManagerWrapper)
            throws InterruptedException {
        return OutgoingAcknowledgementSilencer.installOn(connectionManagerWrapper.channels().values());
    }

    private void provokeAckFor(NettySender sender1) {
        sender1.send(new OutNetworkObject(emptyTestMessage, emptyList(), true));
    }

    /**
     * Creates a mock {@link MessageSerializationRegistry} that throws an exception when trying to get a serializer or a deserializer.
     */
    private static MessageSerializationRegistry mockSerializationRegistry() {
        var mockRegistry = mock(MessageSerializationRegistry.class);

        when(mockRegistry.createDeserializer(anyShort(), anyShort())).thenThrow(RuntimeException.class);
        when(mockRegistry.createSerializer(anyShort(), anyShort())).thenThrow(RuntimeException.class);

        return mockRegistry;
    }

    /**
     * Creates and starts a {@link ConnectionManager} listening on the given port.
     *
     * @param port Port for the connection manager to listen on.
     * @return Connection manager.
     */
    private ConnectionManagerWrapper startManager(int port) throws Exception {
        return startManager(port, defaultSerializationRegistry());
    }

    private ConnectionManagerWrapper startManager(int port, MessageSerializationRegistry registry) throws Exception {
        return startManager(port, UUID.randomUUID(), registry);
    }

    private ConnectionManagerWrapper startManager(int port, UUID launchId) throws Exception {
        return startManager(port, launchId, defaultSerializationRegistry());
    }

    /**
     * Creates and starts a {@link ConnectionManager} listening on the given port, configured with the provided serialization registry.
     *
     * @param port     Port for the connection manager to listen on.
     * @param registry Serialization registry.
     * @return Connection manager.
     */
    private ConnectionManagerWrapper startManager(int port, UUID launchId, MessageSerializationRegistry registry) throws Exception {
        String consistentId = testNodeName(testInfo, port);

        networkConfiguration.port().update(port).join();

        NetworkView cfg = networkConfiguration.value();

        NettyBootstrapFactory bootstrapFactory = new NettyBootstrapFactory(networkConfiguration, consistentId);

        bootstrapFactory.startAsync();

        try {
            var manager = new ConnectionManager(
                    cfg,
                    new SerializationService(registry, mock(UserObjectSerializationContext.class)),
                    consistentId,
                    bootstrapFactory,
                    new AllIdsAreFresh(),
                    mock(FailureProcessor.class)
            );

            manager.start();
            manager.setLocalNode(new ClusterNodeImpl(
                    launchId.toString(),
                    consistentId,
                    new NetworkAddress(manager.localAddress().getHostName(), port)
            ));

            var wrapper = new ConnectionManagerWrapper(manager, bootstrapFactory);

            startedManagers.add(wrapper);

            return wrapper;
        } catch (Exception e) {
            bootstrapFactory.stopAsync();

            throw e;
        }
    }

    private static class ConnectionManagerWrapper implements AutoCloseable {
        final ConnectionManager connectionManager;

        private final NettyBootstrapFactory nettyFactory;

        ConnectionManagerWrapper(ConnectionManager connectionManager, NettyBootstrapFactory nettyFactory) {
            this.connectionManager = connectionManager;
            this.nettyFactory = nettyFactory;
        }

        @Override
        public void close() throws Exception {
            IgniteUtils.closeAll(connectionManager::initiateStopping, connectionManager::stop, nettyFactory::stopAsync);
        }

        OrderingFuture<NettySender> openChannelTo(ConnectionManagerWrapper recipient) {
            return connectionManager.channel(
                    recipient.connectionManager.consistentId(),
                    ChannelType.DEFAULT,
                    recipient.connectionManager.localAddress()
            );
        }

        Map<ConnectorKey<String>, NettySender> channels() {
            return connectionManager.channels();
        }
    }
}
