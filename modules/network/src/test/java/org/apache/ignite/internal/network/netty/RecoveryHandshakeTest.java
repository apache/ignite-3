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

import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.defaultSerializationRegistry;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.OutNetworkObject;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.messages.TestMessage;
import org.apache.ignite.internal.network.messages.TestMessagesFactory;
import org.apache.ignite.internal.network.recovery.AllIdsAreFresh;
import org.apache.ignite.internal.network.recovery.AllIdsAreStale;
import org.apache.ignite.internal.network.recovery.RecoveryClientHandshakeManager;
import org.apache.ignite.internal.network.recovery.RecoveryDescriptor;
import org.apache.ignite.internal.network.recovery.RecoveryDescriptorProvider;
import org.apache.ignite.internal.network.recovery.RecoveryServerHandshakeManager;
import org.apache.ignite.internal.network.recovery.StaleIdDetector;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.network.serialization.PerSessionSerializationService;
import org.apache.ignite.internal.network.serialization.SerializationService;
import org.apache.ignite.internal.network.serialization.UserObjectSerializationContext;
import org.apache.ignite.internal.network.serialization.marshal.DefaultUserObjectMarshaller;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Recovery protocol handshake flow test.
 */
public class RecoveryHandshakeTest extends BaseIgniteAbstractTest {
    /** Connection id. */
    private static final short CONNECTION_ID = 1337;

    private static final UUID LOWER_UUID = new UUID(100, 200);
    private static final UUID HIGHER_UUID = new UUID(300, 400);

    private static final String SERVER_HOST = "server-host";
    private static final String CLIENT_HOST = "client-host";

    private static final int PORT = 1000;

    /** Serialization registry. */
    private static final MessageSerializationRegistry MESSAGE_REGISTRY = defaultSerializationRegistry();

    /** Message factory. */
    private static final NetworkMessagesFactory MESSAGE_FACTORY = new NetworkMessagesFactory();

    /** Test message factory. */
    private static final TestMessagesFactory TEST_MESSAGES_FACTORY = new TestMessagesFactory();

    @Test
    public void testHandshake() throws Exception {
        RecoveryDescriptorProvider clientRecovery = createRecoveryDescriptorProvider();
        RecoveryDescriptorProvider serverRecovery = createRecoveryDescriptorProvider();

        EmbeddedChannel clientSideChannel = createUnregisteredChannel();
        EmbeddedChannel serverSideChannel = createUnregisteredChannel();

        RecoveryClientHandshakeManager clientHandshakeManager = createRecoveryClientHandshakeManager(clientSideChannel, clientRecovery);
        RecoveryServerHandshakeManager serverHandshakeManager = createRecoveryServerHandshakeManager(serverSideChannel, serverRecovery);

        setupChannel(clientSideChannel, clientHandshakeManager, noMessageListener);
        setupChannel(serverSideChannel, serverHandshakeManager, noMessageListener);

        assertTrue(serverSideChannel.isActive());

        exchangeClientToServer(serverSideChannel, clientSideChannel);
        exchangeServerToClient(serverSideChannel, clientSideChannel);
        exchangeClientToServer(serverSideChannel, clientSideChannel);
        exchangeServerToClient(serverSideChannel, clientSideChannel);

        assertNull(clientSideChannel.readOutbound());
        assertNull(serverSideChannel.readOutbound());

        checkHandshakeCompleted(serverHandshakeManager);
        checkHandshakeCompleted(clientHandshakeManager);

        checkPipelineAfterHandshake(serverSideChannel);
        checkPipelineAfterHandshake(clientSideChannel);

        assertFalse(serverSideChannel.finish());
        assertFalse(clientSideChannel.finish());
    }

    @Test
    public void testHandshakeWithUnacknowledgedServerMessage() throws Exception {
        RecoveryDescriptorProvider clientRecovery = createRecoveryDescriptorProvider();
        RecoveryDescriptorProvider serverRecovery = createRecoveryDescriptorProvider();

        EmbeddedChannel clientSideChannel = createUnregisteredChannel();
        EmbeddedChannel serverSideChannel = createUnregisteredChannel();

        UUID clientLaunchId = UUID.randomUUID();
        RecoveryDescriptor serverRecoveryDescriptor = serverRecovery.getRecoveryDescriptor("client", clientLaunchId, CONNECTION_ID);
        addUnacknowledgedMessages(serverRecoveryDescriptor);

        RecoveryClientHandshakeManager clientHandshakeManager = createRecoveryClientHandshakeManager(
                clientSideChannel,
                "client",
                clientLaunchId,
                clientRecovery
        );
        RecoveryServerHandshakeManager serverHandshakeManager = createRecoveryServerHandshakeManager(serverSideChannel, serverRecovery);

        var messageCaptor = new AtomicReference<TestMessage>();
        setupChannel(clientSideChannel, clientHandshakeManager, (inObject) -> {
            NetworkMessage msg = inObject.message();

            assertInstanceOf(TestMessage.class, msg);

            messageCaptor.set((TestMessage) msg);
        });

        setupChannel(serverSideChannel, serverHandshakeManager, noMessageListener);

        assertTrue(serverSideChannel.isActive());

        exchangeClientToServer(serverSideChannel, clientSideChannel);
        exchangeServerToClient(serverSideChannel, clientSideChannel);
        exchangeClientToServer(serverSideChannel, clientSideChannel);
        exchangeServerToClient(serverSideChannel, clientSideChannel);

        assertNull(clientSideChannel.readOutbound());

        exchangeServerToClient(serverSideChannel, clientSideChannel);
        assertNull(serverSideChannel.readOutbound());

        TestMessage ackedMessage = messageCaptor.get();
        assertNotNull(ackedMessage);

        checkHandshakeNotCompleted(serverHandshakeManager);
        checkHandshakeCompleted(clientHandshakeManager);

        exchangeClientToServer(serverSideChannel, clientSideChannel);

        checkHandshakeCompleted(serverHandshakeManager);
        checkHandshakeCompleted(clientHandshakeManager);

        checkPipelineAfterHandshake(serverSideChannel);
        checkPipelineAfterHandshake(clientSideChannel);

        assertFalse(serverSideChannel.finish());
        assertFalse(clientSideChannel.finish());
    }

    @Test
    public void testHandshakeWithUnacknowledgedClientMessage() throws Exception {
        RecoveryDescriptorProvider clientRecovery = createRecoveryDescriptorProvider();
        RecoveryDescriptorProvider serverRecovery = createRecoveryDescriptorProvider();

        EmbeddedChannel clientSideChannel = createUnregisteredChannel();
        EmbeddedChannel serverSideChannel = createUnregisteredChannel();

        UUID serverLaunchId = UUID.randomUUID();
        RecoveryDescriptor clientRecoveryDescriptor = clientRecovery.getRecoveryDescriptor("server", serverLaunchId, CONNECTION_ID);
        addUnacknowledgedMessages(clientRecoveryDescriptor);

        RecoveryClientHandshakeManager clientHandshakeManager = createRecoveryClientHandshakeManager(clientSideChannel, clientRecovery);
        RecoveryServerHandshakeManager serverHandshakeManager = createRecoveryServerHandshakeManager(
                serverSideChannel,
                "server",
                serverLaunchId,
                serverRecovery
        );

        var messageCaptor = new AtomicReference<TestMessage>();
        setupChannel(clientSideChannel, clientHandshakeManager, noMessageListener);

        setupChannel(serverSideChannel, serverHandshakeManager, (inObject) -> {
            NetworkMessage msg = inObject.message();

            assertInstanceOf(TestMessage.class, msg);

            messageCaptor.set((TestMessage) msg);
        });

        assertTrue(serverSideChannel.isActive());

        exchangeClientToServer(serverSideChannel, clientSideChannel);
        exchangeServerToClient(serverSideChannel, clientSideChannel);
        exchangeClientToServer(serverSideChannel, clientSideChannel);
        exchangeServerToClient(serverSideChannel, clientSideChannel);

        assertNull(serverSideChannel.readOutbound());

        exchangeClientToServer(serverSideChannel, clientSideChannel);
        assertNull(clientSideChannel.readOutbound());

        TestMessage ackedMessage = messageCaptor.get();
        assertNotNull(ackedMessage);

        checkHandshakeCompleted(serverHandshakeManager);
        checkHandshakeNotCompleted(clientHandshakeManager);

        exchangeServerToClient(serverSideChannel, clientSideChannel);

        checkHandshakeCompleted(serverHandshakeManager);
        checkHandshakeCompleted(clientHandshakeManager);

        checkPipelineAfterHandshake(serverSideChannel);
        checkPipelineAfterHandshake(clientSideChannel);

        assertFalse(serverSideChannel.finish());
        assertFalse(clientSideChannel.finish());
    }

    @Test
    public void testPairedRecoveryDescriptorsClinch() throws Exception {
        RecoveryDescriptorProvider node1Recovery = createRecoveryDescriptorProvider();
        RecoveryDescriptorProvider node2Recovery = createRecoveryDescriptorProvider();

        EmbeddedChannel channel1Src = createUnregisteredChannel();
        EmbeddedChannel channel1Dst = createUnregisteredChannel();
        EmbeddedChannel channel2Src = createUnregisteredChannel();
        EmbeddedChannel channel2Dst = createUnregisteredChannel();

        UUID node1Uuid = LOWER_UUID;
        UUID node2Uuid = HIGHER_UUID;

        RecoveryClientHandshakeManager chm1 = createRecoveryClientHandshakeManager(channel1Src, "client", node1Uuid, node1Recovery);
        RecoveryServerHandshakeManager shm1 = createRecoveryServerHandshakeManager(channel2Dst, "client", node1Uuid, node1Recovery);

        RecoveryClientHandshakeManager chm2 = createRecoveryClientHandshakeManager(channel2Src, "server", node2Uuid, node2Recovery);
        RecoveryServerHandshakeManager shm2 = createRecoveryServerHandshakeManager(channel1Dst, "server", node2Uuid, node2Recovery);

        // Channel opened from node1 to node2 is channel 1.
        // Channel opened from node2 to node1 is channel 2.

        // Channel 1.
        setupChannel(channel1Src, chm1, noMessageListener);
        setupChannel(channel1Dst, shm2, noMessageListener);

        // Channel 2.
        setupChannel(channel2Src, chm2, noMessageListener);
        setupChannel(channel2Dst, shm1, noMessageListener);

        exchangeClientToServer(channel2Dst, channel2Src);
        exchangeClientToServer(channel1Dst, channel1Src);

        exchangeServerToClient(channel2Dst, channel2Src);
        exchangeServerToClient(channel1Dst, channel1Src);

        exchangeClientToServer(channel2Dst, channel2Src);
        exchangeClientToServer(channel1Dst, channel1Src);

        // 2 -> 1 (Channel 2) is alive, while 1 -> 2 (Channel 1) closes because of the tie-breaking.
        exchangeServerToClient(channel1Dst, channel1Src);
        exchangeServerToClient(channel2Dst, channel2Src);
        assertFalse(channel1Src.isOpen());
        assertFalse(channel1Dst.isOpen());

        assertTrue(channel2Src.isOpen());
        assertTrue(channel2Dst.isOpen());

        assertFalse(channel1Src.finish());
        assertFalse(channel2Dst.finish());
        assertFalse(channel2Src.finish());
        assertFalse(channel1Dst.finish());
    }

    /**
     * This tests the following scenario: two handshakes in the opposite directions are started,
     * Handshake 1 is faster and it takes both client-side and server-side locks (using recovery descriptors
     * as locks), and only then Handshake 2 tries to take the first lock (the one on the client side).
     * In such a situation, tie-breaking logic should not be applied (as Handshake 1 could have already
     * established, or almost established, a logical connection); instead, Handshake 2 must stop
     * itself (regardless of what that the Tie Breaker would prescribe).
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testLateHandshakeDoesNotUseTieBreaker(boolean node1LaunchIdIsLower) throws Exception {
        RecoveryDescriptorProvider node1Recovery = createRecoveryDescriptorProvider();
        RecoveryDescriptorProvider node2Recovery = createRecoveryDescriptorProvider();

        EmbeddedChannel channel1Src = createUnregisteredChannel();
        EmbeddedChannel channel1Dst = createUnregisteredChannel();
        EmbeddedChannel channel2Src = createUnregisteredChannel();
        EmbeddedChannel channel2Dst = createUnregisteredChannel();

        UUID node1Uuid = node1LaunchIdIsLower ? LOWER_UUID : HIGHER_UUID;
        UUID node2Uuid = node1LaunchIdIsLower ? HIGHER_UUID : LOWER_UUID;

        RecoveryClientHandshakeManager chm1 = createRecoveryClientHandshakeManager(channel1Src, "client", node1Uuid, node1Recovery);
        RecoveryServerHandshakeManager shm1 = createRecoveryServerHandshakeManager(channel2Dst, "client", node1Uuid, node1Recovery);

        RecoveryClientHandshakeManager chm2 = createRecoveryClientHandshakeManager(channel2Src, "server", node2Uuid, node2Recovery);
        RecoveryServerHandshakeManager shm2 = createRecoveryServerHandshakeManager(channel1Dst, "server", node2Uuid, node2Recovery);

        // Channel opened from node1 to node2 is channel 1.
        // Channel opened from node2 to node1 is channel 2.

        // Channel 1.
        setupChannel(channel1Src, chm1, noMessageListener);
        setupChannel(channel1Dst, shm2, noMessageListener);

        // Channel 2.
        setupChannel(channel2Src, chm2, noMessageListener);
        setupChannel(channel2Dst, shm1, noMessageListener);

        // Channel 2's handshake acquires both locks.
        exchangeClientToServer(channel2Dst, channel2Src);
        exchangeServerToClient(channel2Dst, channel2Src);
        exchangeClientToServer(channel2Dst, channel2Src);

        // Now Channel 1's handshake cannot acquire even first lock.
        exchangeClientToServer(channel1Dst, channel1Src);
        exchangeServerToClient(channel1Dst, channel1Src);

        // 2 -> 1 is alive, while 1 -> 2 closes because it is late.
        exchangeServerToClient(channel2Dst, channel2Src);
        assertFalse(channel1Src.isOpen());

        assertTrue(channel2Src.isOpen());
        assertTrue(channel2Dst.isOpen());

        assertFalse(channel1Src.finish());
        assertFalse(channel2Dst.finish());
        assertFalse(channel2Src.finish());
        assertFalse(channel1Dst.finish());
    }

    @Test
    public void testExactlyOnceServer() throws Exception {
        testExactlyOnce(true);
    }

    @Test
    public void testExactlyOnceClient() throws Exception {
        testExactlyOnce(false);
    }

    /**
     * Tests that message was received exactly once in case of network failure during acknowledgement.
     *
     * @param serverDidntReceiveAck {@code true} if server didn't receive the acknowledgement, {@code false} if client didn't receive
     *                              the acknowledgement.
     * @throws Exception If failed.
     */
    private void testExactlyOnce(boolean serverDidntReceiveAck) throws Exception {
        var server = "server";
        UUID serverLaunchId = UUID.randomUUID();
        var client = "client";
        UUID clientLaunchId = UUID.randomUUID();

        RecoveryDescriptorProvider clientRecovery = createRecoveryDescriptorProvider();
        RecoveryDescriptorProvider serverRecovery = createRecoveryDescriptorProvider();

        EmbeddedChannel clientSideChannel = createUnregisteredChannel();
        EmbeddedChannel serverSideChannel = createUnregisteredChannel();

        RecoveryClientHandshakeManager clientHandshakeManager = createRecoveryClientHandshakeManager(
                clientSideChannel,
                client,
                clientLaunchId,
                clientRecovery
        );
        RecoveryServerHandshakeManager serverHandshakeManager = createRecoveryServerHandshakeManager(
                serverSideChannel,
                server,
                serverLaunchId,
                serverRecovery
        );

        var receivedFirst = new AtomicBoolean();

        var listener1 = new MessageListener("1", receivedFirst);

        setupChannel(clientSideChannel, clientHandshakeManager, serverDidntReceiveAck ? listener1 : noMessageListener);
        setupChannel(serverSideChannel, serverHandshakeManager, serverDidntReceiveAck ?  noMessageListener : listener1);

        // Normal handshake
        exchangeClientToServer(serverSideChannel, clientSideChannel);
        exchangeServerToClient(serverSideChannel, clientSideChannel);
        exchangeClientToServer(serverSideChannel, clientSideChannel);
        exchangeServerToClient(serverSideChannel, clientSideChannel);

        EmbeddedChannel ch = serverDidntReceiveAck ? serverSideChannel : clientSideChannel;

        // Add two messages to the outbound
        ch.writeOutbound(new OutNetworkObject(TEST_MESSAGES_FACTORY.testMessage().msg("1").build(), Collections.emptyList()));
        ch.writeOutbound(new OutNetworkObject(TEST_MESSAGES_FACTORY.testMessage().msg("2").build(), Collections.emptyList()));

        // Send one of the messages
        if (serverDidntReceiveAck) {
            exchangeServerToClient(serverSideChannel, clientSideChannel);
        } else {
            exchangeClientToServer(serverSideChannel, clientSideChannel);
        }

        // Message should be received
        assertTrue(receivedFirst.get());

        // Transfer only one acknowledgement, don't transfer the second one (simulates network failure on acknowledgement)
        if (serverDidntReceiveAck) {
            exchangeClientToServer(serverSideChannel, clientSideChannel);
        } else {
            exchangeServerToClient(serverSideChannel, clientSideChannel);
        }

        // Simulate reconnection
        clientHandshakeManager = createRecoveryClientHandshakeManager(clientSideChannel, client, clientLaunchId, clientRecovery);
        serverHandshakeManager = createRecoveryServerHandshakeManager(serverSideChannel, server, serverLaunchId, serverRecovery);

        var receivedSecond = new AtomicBoolean();

        var listener2 = new MessageListener("2", receivedSecond);

        clientSideChannel.finishAndReleaseAll();
        serverSideChannel.finishAndReleaseAll();

        clientSideChannel = createUnregisteredChannel();
        serverSideChannel = createUnregisteredChannel();

        setupChannel(clientSideChannel, clientHandshakeManager, serverDidntReceiveAck ? listener2 : noMessageListener);
        setupChannel(serverSideChannel, serverHandshakeManager, serverDidntReceiveAck ? noMessageListener : listener2);

        // Handshake
        exchangeClientToServer(serverSideChannel, clientSideChannel);
        exchangeServerToClient(serverSideChannel, clientSideChannel);
        exchangeClientToServer(serverSideChannel, clientSideChannel);
        exchangeServerToClient(serverSideChannel, clientSideChannel);

        // Resending message
        if (serverDidntReceiveAck) {
            exchangeServerToClient(serverSideChannel, clientSideChannel);
        } else {
            exchangeClientToServer(serverSideChannel, clientSideChannel);
        }

        // Send another acknowledgement
        if (serverDidntReceiveAck) {
            exchangeClientToServer(serverSideChannel, clientSideChannel);
        } else {
            exchangeServerToClient(serverSideChannel, clientSideChannel);
        }

        assertNull(serverSideChannel.readOutbound());
        assertNull(clientSideChannel.readOutbound());

        assertTrue(receivedSecond.get());

        assertFalse(serverSideChannel.finish());
        assertFalse(clientSideChannel.finish());
    }

    @Test
    public void serverFailsHandshakeIfClientIdIsAlreadySeen() throws Exception {
        RecoveryDescriptorProvider clientRecovery = createRecoveryDescriptorProvider();
        RecoveryDescriptorProvider serverRecovery = createRecoveryDescriptorProvider();

        EmbeddedChannel clientSideChannel = createUnregisteredChannel();
        EmbeddedChannel serverSideChannel = createUnregisteredChannel();

        RecoveryClientHandshakeManager clientHandshakeManager = createRecoveryClientHandshakeManager(clientSideChannel, clientRecovery);
        RecoveryServerHandshakeManager serverHandshakeManager = createRecoveryServerHandshakeManager(
                serverSideChannel,
                "server",
                UUID.randomUUID(),
                serverRecovery,
                new AllIdsAreStale()
        );

        setupChannel(clientSideChannel, clientHandshakeManager, noMessageListener);
        setupChannel(serverSideChannel, serverHandshakeManager, noMessageListener);

        assertTrue(serverSideChannel.isActive());

        exchangeClientToServer(serverSideChannel, clientSideChannel);
        exchangeServerToClient(serverSideChannel, clientSideChannel);
        exchangeClientToServer(serverSideChannel, clientSideChannel);
        exchangeServerToClient(serverSideChannel, clientSideChannel);

        assertNull(clientSideChannel.readOutbound());
        assertNull(serverSideChannel.readOutbound());

        checkHandshakeCompletedExceptionally(serverHandshakeManager);
        checkHandshakeCompletedExceptionally(clientHandshakeManager);

        checkPipelineAfterHandshake(serverSideChannel);
        checkPipelineAfterHandshake(clientSideChannel);

        assertFalse(serverSideChannel.finish());
        assertFalse(clientSideChannel.finish());
    }

    @Test
    public void clientFailsHandshakeIfServerIdIsAlreadySeen() throws Exception {
        RecoveryDescriptorProvider clientRecovery = createRecoveryDescriptorProvider();
        RecoveryDescriptorProvider serverRecovery = createRecoveryDescriptorProvider();

        EmbeddedChannel clientSideChannel = createUnregisteredChannel();
        EmbeddedChannel serverSideChannel = createUnregisteredChannel();

        RecoveryClientHandshakeManager clientHandshakeManager = createRecoveryClientHandshakeManager(
                clientSideChannel,
                "client",
                UUID.randomUUID(),
                clientRecovery,
                new AllIdsAreStale()
        );
        RecoveryServerHandshakeManager serverHandshakeManager = createRecoveryServerHandshakeManager(serverSideChannel, serverRecovery);

        setupChannel(clientSideChannel, clientHandshakeManager, noMessageListener);
        setupChannel(serverSideChannel, serverHandshakeManager, noMessageListener);

        assertTrue(serverSideChannel.isActive());

        exchangeClientToServer(serverSideChannel, clientSideChannel);
        exchangeServerToClient(serverSideChannel, clientSideChannel);
        exchangeClientToServer(serverSideChannel, clientSideChannel);

        assertNull(clientSideChannel.readOutbound());
        assertNull(serverSideChannel.readOutbound());

        checkHandshakeCompletedExceptionally(serverHandshakeManager);
        checkHandshakeCompletedExceptionally(clientHandshakeManager);

        checkPipelineAfterHandshake(serverSideChannel);
        checkPipelineAfterHandshake(clientSideChannel);

        assertFalse(serverSideChannel.finish());
        assertFalse(clientSideChannel.finish());
    }

    /** Message listener that accepts a specific message only once. */
    private static class MessageListener implements Consumer<InNetworkObject> {
        /** Expected message. */
        private final String expectedMessage;

        /** Flag indicating that expected messages was received. */
        private final AtomicBoolean flag;

        private MessageListener(String expectedMessage, AtomicBoolean flag) {
            this.expectedMessage = expectedMessage;
            this.flag = flag;
        }

        /** {@inheritDoc} */
        @Override
        public void accept(InNetworkObject inNetworkObject) {
            TestMessage msg = (TestMessage) inNetworkObject.message();
            if (expectedMessage.equals(msg.msg())) {
                if (!flag.compareAndSet(false, true)) {
                    fail();
                }
                return;
            }
            fail();
        }
    }

    private void checkPipelineAfterHandshake(EmbeddedChannel channel) {
        assertNull(channel.pipeline().get(HandshakeHandler.NAME));
    }

    private void checkHandshakeNotCompleted(HandshakeManager manager) {
        CompletableFuture<NettySender> localHandshakeFuture = manager.localHandshakeFuture();
        assertFalse(localHandshakeFuture.isDone());
        assertFalse(localHandshakeFuture.isCompletedExceptionally());
        assertFalse(localHandshakeFuture.isCancelled());

        CompletableFuture<NettySender> finalHandshakeFuture = manager.finalHandshakeFuture().toCompletableFuture();
        assertFalse(finalHandshakeFuture.isDone());
        assertFalse(finalHandshakeFuture.isCompletedExceptionally());
        assertFalse(finalHandshakeFuture.isCancelled());
    }

    private void checkHandshakeCompleted(HandshakeManager manager) {
        CompletableFuture<NettySender> localHandshakeFuture = manager.localHandshakeFuture();
        assertTrue(localHandshakeFuture.isDone());
        assertFalse(localHandshakeFuture.isCompletedExceptionally());
        assertFalse(localHandshakeFuture.isCancelled());

        CompletableFuture<NettySender> finalHandshakeFuture = manager.finalHandshakeFuture().toCompletableFuture();
        assertTrue(finalHandshakeFuture.isDone());
        assertFalse(finalHandshakeFuture.isCompletedExceptionally());
        assertFalse(finalHandshakeFuture.isCancelled());
    }

    private void checkHandshakeCompletedExceptionally(HandshakeManager manager) {
        CompletableFuture<NettySender> handshakeFuture = manager.localHandshakeFuture();

        assertTrue(handshakeFuture.isDone());
        assertTrue(handshakeFuture.isCompletedExceptionally());
        assertFalse(handshakeFuture.isCancelled());

        CompletableFuture<NettySender> finalHandshakeFuture = manager.finalHandshakeFuture().toCompletableFuture();

        assertTrue(finalHandshakeFuture.isDone());
        assertTrue(finalHandshakeFuture.isCompletedExceptionally());
        assertFalse(finalHandshakeFuture.isCancelled());
    }

    private void addUnacknowledgedMessages(RecoveryDescriptor recoveryDescriptor) {
        TestMessage msg = TEST_MESSAGES_FACTORY.testMessage().msg("test").build();
        recoveryDescriptor.add(new OutNetworkObject(msg, Collections.emptyList()));
    }

    private void exchangeServerToClient(EmbeddedChannel serverSideChannel, EmbeddedChannel clientSideChannel) {
        runPendingTasks(serverSideChannel, clientSideChannel);

        ByteBuf outgoingMessageBuffer = serverSideChannel.readOutbound();
        // No need to release buffer because inbound buffers are released by InboundDecoder
        clientSideChannel.writeInbound(outgoingMessageBuffer);
    }

    private void exchangeClientToServer(EmbeddedChannel serverSideChannel, EmbeddedChannel clientSideChannel) {
        runPendingTasks(serverSideChannel, clientSideChannel);

        ByteBuf outgoingMessageBuffer = clientSideChannel.readOutbound();
        // No need to release buffer because inbound buffers are released by InboundDecoder
        serverSideChannel.writeInbound(outgoingMessageBuffer);
    }

    private void runPendingTasks(EmbeddedChannel channel1, EmbeddedChannel channel2) {
        channel1.runPendingTasks();
        channel2.runPendingTasks();
    }

    private final Consumer<InNetworkObject> noMessageListener = inNetworkObject ->
            fail("Received message while shouldn't have, [" + inNetworkObject.message() + "]");

    private void setupChannel(EmbeddedChannel channel, HandshakeManager handshakeManager, Consumer<InNetworkObject> messageListener)
            throws Exception {
        var serializationService = new SerializationService(MESSAGE_REGISTRY, createUserObjectSerializationContext());
        var sessionSerializationService = new PerSessionSerializationService(serializationService);

        PipelineUtils.setup(channel.pipeline(), sessionSerializationService, handshakeManager, messageListener);

        channel.register();
    }

    private static EmbeddedChannel createUnregisteredChannel() {
        // Channel should not be registered at first, not before we add pipeline handlers
        // Otherwise, events like "channel active" won't be propagated to the handlers
        return new EmbeddedChannel(false, false);
    }

    private UserObjectSerializationContext createUserObjectSerializationContext() {
        var userObjectDescriptorRegistry = new ClassDescriptorRegistry();
        var userObjectDescriptorFactory = new ClassDescriptorFactory(userObjectDescriptorRegistry);

        var userObjectMarshaller = new DefaultUserObjectMarshaller(userObjectDescriptorRegistry, userObjectDescriptorFactory);

        return new UserObjectSerializationContext(userObjectDescriptorRegistry, userObjectDescriptorFactory,
                userObjectMarshaller);
    }

    private RecoveryClientHandshakeManager createRecoveryClientHandshakeManager(
            Channel clientSideChannel,
            RecoveryDescriptorProvider provider
    ) {
        return createRecoveryClientHandshakeManager(clientSideChannel, "client", UUID.randomUUID(), provider);
    }

    private RecoveryClientHandshakeManager createRecoveryClientHandshakeManager(
            Channel clientSideChannel,
            String consistentId,
            UUID launchId,
            RecoveryDescriptorProvider provider
    ) {
        return createRecoveryClientHandshakeManager(clientSideChannel, consistentId, launchId, provider, new AllIdsAreFresh());
    }

    private RecoveryClientHandshakeManager createRecoveryClientHandshakeManager(
            Channel clientSideChannel,
            String consistentId,
            UUID launchId,
            RecoveryDescriptorProvider provider,
            StaleIdDetector staleIdDetector
    ) {
        return new RecoveryClientHandshakeManager(
                new ClusterNodeImpl(launchId.toString(), consistentId, new NetworkAddress(CLIENT_HOST, PORT)),
                CONNECTION_ID,
                provider,
                () -> List.of(clientSideChannel.eventLoop()),
                staleIdDetector,
                channel -> {},
                () -> false,
                mock(FailureProcessor.class)
        );
    }

    private RecoveryServerHandshakeManager createRecoveryServerHandshakeManager(
            Channel serverSideChannel,
            RecoveryDescriptorProvider provider
    ) {
        return createRecoveryServerHandshakeManager(serverSideChannel, "server", UUID.randomUUID(), provider);
    }

    private RecoveryServerHandshakeManager createRecoveryServerHandshakeManager(
            Channel serverSideChannel,
            String consistentId,
            UUID launchId,
            RecoveryDescriptorProvider provider
    ) {
        return createRecoveryServerHandshakeManager(serverSideChannel, consistentId, launchId, provider, new AllIdsAreFresh());
    }

    private RecoveryServerHandshakeManager createRecoveryServerHandshakeManager(
            Channel serverSideChannel,
            String consistentId,
            UUID launchId,
            RecoveryDescriptorProvider provider,
            StaleIdDetector staleIdDetector
    ) {
        return new RecoveryServerHandshakeManager(
                new ClusterNodeImpl(launchId.toString(), consistentId, new NetworkAddress(SERVER_HOST, PORT)),
                MESSAGE_FACTORY,
                provider,
                () -> List.of(serverSideChannel.eventLoop()),
                staleIdDetector,
                channel -> {},
                () -> false,
                mock(FailureProcessor.class)
        );
    }

    private RecoveryDescriptorProvider createRecoveryDescriptorProvider() {
        return new DefaultRecoveryDescriptorProvider();
    }
}
