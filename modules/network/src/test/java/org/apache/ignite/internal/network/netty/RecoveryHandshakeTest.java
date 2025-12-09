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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.getFieldValue;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.AbstractScheduledEventExecutor;
import java.util.Collections;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.handlers.NoOpFailureHandler;
import org.apache.ignite.internal.network.ClusterIdSupplier;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ConstantClusterIdSupplier;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.OutNetworkObject;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.handshake.NoOpHandshakeEventLoopSwitcher;
import org.apache.ignite.internal.network.messages.TestMessage;
import org.apache.ignite.internal.network.messages.TestMessagesFactory;
import org.apache.ignite.internal.network.recovery.AllIdsAreFresh;
import org.apache.ignite.internal.network.recovery.AllIdsAreStale;
import org.apache.ignite.internal.network.recovery.RecoveryAcceptorHandshakeManager;
import org.apache.ignite.internal.network.recovery.RecoveryDescriptor;
import org.apache.ignite.internal.network.recovery.RecoveryDescriptorProvider;
import org.apache.ignite.internal.network.recovery.RecoveryInitiatorHandshakeManager;
import org.apache.ignite.internal.network.recovery.StaleIdDetector;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.network.serialization.PerSessionSerializationService;
import org.apache.ignite.internal.network.serialization.SerializationService;
import org.apache.ignite.internal.network.serialization.UserObjectSerializationContext;
import org.apache.ignite.internal.network.serialization.marshal.DefaultUserObjectMarshaller;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.version.DefaultIgniteProductVersionSource;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Recovery protocol handshake flow test.
 */
@ExtendWith(ConfigurationExtension.class)
public class RecoveryHandshakeTest extends BaseIgniteAbstractTest {
    /** Connection id. */
    private static final short CONNECTION_ID = 1337;

    private static final UUID LOWER_UUID = new UUID(100, 200);
    private static final UUID HIGHER_UUID = new UUID(300, 400);

    private static final String INITIATOR = "initiator";
    private static final String ACCEPTOR = "acceptor";

    private static final String ACCEPTOR_HOST = "acceptor-host";
    private static final String INITIATOR_HOST = "initiator-host";

    private static final int PORT = 1000;

    /** Serialization registry. */
    private static final MessageSerializationRegistry MESSAGE_REGISTRY = defaultSerializationRegistry();

    /** Message factory. */
    private static final NetworkMessagesFactory MESSAGE_FACTORY = new NetworkMessagesFactory();

    /** Test message factory. */
    private static final TestMessagesFactory TEST_MESSAGES_FACTORY = new TestMessagesFactory();

    private final ClusterIdSupplier clusterIdSupplier = new ConstantClusterIdSupplier(UUID.randomUUID());

    protected final TopologyService topologyService = mock(TopologyService.class);

    @Test
    public void testHandshake() throws Exception {
        RecoveryDescriptorProvider initiatorRecovery = createRecoveryDescriptorProvider();
        RecoveryDescriptorProvider acceptorRecovery = createRecoveryDescriptorProvider();

        EmbeddedChannel initiatorSideChannel = createUnregisteredChannel();
        EmbeddedChannel acceptorSideChannel = createUnregisteredChannel();

        RecoveryInitiatorHandshakeManager initiatorHandshakeManager = createRecoveryInitiatorHandshakeManager(
                initiatorRecovery
        );
        RecoveryAcceptorHandshakeManager acceptorHandshakeManager = createRecoveryAcceptorHandshakeManager(
                acceptorRecovery
        );

        setupChannel(initiatorSideChannel, initiatorHandshakeManager, noMessageListener);
        setupChannel(acceptorSideChannel, acceptorHandshakeManager, noMessageListener);

        assertTrue(acceptorSideChannel.isActive());

        exchangeInitiatorToAcceptor(acceptorSideChannel, initiatorSideChannel);
        exchangeAcceptorToInitiator(acceptorSideChannel, initiatorSideChannel);
        exchangeInitiatorToAcceptor(acceptorSideChannel, initiatorSideChannel);
        exchangeAcceptorToInitiator(acceptorSideChannel, initiatorSideChannel);

        assertNull(initiatorSideChannel.readOutbound());
        assertNull(acceptorSideChannel.readOutbound());

        checkHandshakeCompleted(acceptorHandshakeManager);
        checkHandshakeCompleted(initiatorHandshakeManager);

        checkPipelineAfterHandshake(acceptorSideChannel);
        checkPipelineAfterHandshake(initiatorSideChannel);

        assertFalse(acceptorSideChannel.finish());
        assertFalse(initiatorSideChannel.finish());
    }

    @Test
    public void testHandshakeWithUnacknowledgedAcceptorMessage() throws Exception {
        RecoveryDescriptorProvider initiatorRecovery = createRecoveryDescriptorProvider();
        RecoveryDescriptorProvider acceptorRecovery = createRecoveryDescriptorProvider();

        EmbeddedChannel initiatorSideChannel = createUnregisteredChannel();
        EmbeddedChannel acceptorSideChannel = createUnregisteredChannel();

        UUID initiatorLaunchId = UUID.randomUUID();
        RecoveryDescriptor acceptorRecoveryDescriptor = acceptorRecovery.getRecoveryDescriptor(
                INITIATOR,
                initiatorLaunchId,
                CONNECTION_ID
        );
        addUnacknowledgedMessages(acceptorRecoveryDescriptor);

        RecoveryInitiatorHandshakeManager initiatorHandshakeManager = createRecoveryInitiatorHandshakeManager(
                INITIATOR,
                initiatorLaunchId,
                initiatorRecovery
        );
        RecoveryAcceptorHandshakeManager acceptorHandshakeManager = createRecoveryAcceptorHandshakeManager(
                acceptorRecovery
        );

        var messageCaptor = new AtomicReference<TestMessage>();
        setupChannel(initiatorSideChannel, initiatorHandshakeManager, (inObject) -> {
            NetworkMessage msg = inObject.message();

            assertInstanceOf(TestMessage.class, msg);

            messageCaptor.set((TestMessage) msg);
        });

        setupChannel(acceptorSideChannel, acceptorHandshakeManager, noMessageListener);

        assertTrue(acceptorSideChannel.isActive());

        exchangeInitiatorToAcceptor(acceptorSideChannel, initiatorSideChannel);
        exchangeAcceptorToInitiator(acceptorSideChannel, initiatorSideChannel);
        exchangeInitiatorToAcceptor(acceptorSideChannel, initiatorSideChannel);
        exchangeAcceptorToInitiator(acceptorSideChannel, initiatorSideChannel);

        assertNull(initiatorSideChannel.readOutbound());

        exchangeAcceptorToInitiator(acceptorSideChannel, initiatorSideChannel);
        assertNull(acceptorSideChannel.readOutbound());

        TestMessage ackedMessage = messageCaptor.get();
        assertNotNull(ackedMessage);

        checkHandshakeNotCompleted(acceptorHandshakeManager);
        checkHandshakeCompleted(initiatorHandshakeManager);

        exchangeInitiatorToAcceptor(acceptorSideChannel, initiatorSideChannel);

        checkHandshakeCompleted(acceptorHandshakeManager);
        checkHandshakeCompleted(initiatorHandshakeManager);

        checkPipelineAfterHandshake(acceptorSideChannel);
        checkPipelineAfterHandshake(initiatorSideChannel);

        assertFalse(acceptorSideChannel.finish());
        assertFalse(initiatorSideChannel.finish());
    }

    @Test
    public void testHandshakeWithUnacknowledgedInitiatorMessage() throws Exception {
        RecoveryDescriptorProvider initiatorRecovery = createRecoveryDescriptorProvider();
        RecoveryDescriptorProvider acceptorRecovery = createRecoveryDescriptorProvider();

        EmbeddedChannel initiatorSideChannel = createUnregisteredChannel();
        EmbeddedChannel acceptorSideChannel = createUnregisteredChannel();

        UUID acceptorLaunchId = UUID.randomUUID();
        RecoveryDescriptor initiatorRecoveryDescriptor = initiatorRecovery.getRecoveryDescriptor(
                ACCEPTOR,
                acceptorLaunchId,
                CONNECTION_ID
        );
        addUnacknowledgedMessages(initiatorRecoveryDescriptor);

        RecoveryInitiatorHandshakeManager initiatorHandshakeManager = createRecoveryInitiatorHandshakeManager(
                initiatorRecovery
        );
        RecoveryAcceptorHandshakeManager acceptorHandshakeManager = createRecoveryAcceptorHandshakeManager(
                ACCEPTOR,
                acceptorLaunchId,
                acceptorRecovery
        );

        var messageCaptor = new AtomicReference<TestMessage>();
        setupChannel(initiatorSideChannel, initiatorHandshakeManager, noMessageListener);

        setupChannel(acceptorSideChannel, acceptorHandshakeManager, (inObject) -> {
            NetworkMessage msg = inObject.message();

            assertInstanceOf(TestMessage.class, msg);

            messageCaptor.set((TestMessage) msg);
        });

        assertTrue(acceptorSideChannel.isActive());

        exchangeInitiatorToAcceptor(acceptorSideChannel, initiatorSideChannel);
        exchangeAcceptorToInitiator(acceptorSideChannel, initiatorSideChannel);
        exchangeInitiatorToAcceptor(acceptorSideChannel, initiatorSideChannel);
        exchangeAcceptorToInitiator(acceptorSideChannel, initiatorSideChannel);

        assertNull(acceptorSideChannel.readOutbound());

        exchangeInitiatorToAcceptor(acceptorSideChannel, initiatorSideChannel);
        assertNull(initiatorSideChannel.readOutbound());

        TestMessage ackedMessage = messageCaptor.get();
        assertNotNull(ackedMessage);

        checkHandshakeCompleted(acceptorHandshakeManager);
        checkHandshakeNotCompleted(initiatorHandshakeManager);

        exchangeAcceptorToInitiator(acceptorSideChannel, initiatorSideChannel);

        checkHandshakeCompleted(acceptorHandshakeManager);
        checkHandshakeCompleted(initiatorHandshakeManager);

        checkPipelineAfterHandshake(acceptorSideChannel);
        checkPipelineAfterHandshake(initiatorSideChannel);

        assertFalse(acceptorSideChannel.finish());
        assertFalse(initiatorSideChannel.finish());
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

        RecoveryInitiatorHandshakeManager chm1 = createRecoveryInitiatorHandshakeManager(
                INITIATOR,
                node1Uuid,
                node1Recovery
        );
        RecoveryAcceptorHandshakeManager shm1 = createRecoveryAcceptorHandshakeManager(INITIATOR, node1Uuid, node1Recovery);

        RecoveryInitiatorHandshakeManager chm2 = createRecoveryInitiatorHandshakeManager(ACCEPTOR, node2Uuid, node2Recovery);
        RecoveryAcceptorHandshakeManager shm2 = createRecoveryAcceptorHandshakeManager(ACCEPTOR, node2Uuid, node2Recovery);

        // Channel opened from node1 to node2 is channel 1.
        // Channel opened from node2 to node1 is channel 2.

        // Channel 1.
        setupChannel(channel1Src, chm1, noMessageListener);
        setupChannel(channel1Dst, shm2, noMessageListener);

        // Channel 2.
        setupChannel(channel2Src, chm2, noMessageListener);
        setupChannel(channel2Dst, shm1, noMessageListener);

        exchangeInitiatorToAcceptor(channel2Dst, channel2Src);
        exchangeInitiatorToAcceptor(channel1Dst, channel1Src);

        exchangeAcceptorToInitiator(channel2Dst, channel2Src);
        exchangeAcceptorToInitiator(channel1Dst, channel1Src);

        exchangeInitiatorToAcceptor(channel2Dst, channel2Src);
        exchangeInitiatorToAcceptor(channel1Dst, channel1Src);

        // 2 -> 1 (Channel 2) is alive, while 1 -> 2 (Channel 1) closes because of the tie-breaking.
        exchangeAcceptorToInitiator(channel1Dst, channel1Src);
        exchangeAcceptorToInitiator(channel2Dst, channel2Src);
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
     * Handshake 1 is faster and it takes both initiator-side and acceptor-side locks (using recovery descriptors
     * as locks), and only then Handshake 2 tries to take the first lock (the one on the initiator side).
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

        RecoveryInitiatorHandshakeManager chm1 = createRecoveryInitiatorHandshakeManager(
                INITIATOR,
                node1Uuid,
                node1Recovery
        );
        RecoveryAcceptorHandshakeManager shm1 = createRecoveryAcceptorHandshakeManager(INITIATOR, node1Uuid, node1Recovery);

        RecoveryInitiatorHandshakeManager chm2 = createRecoveryInitiatorHandshakeManager(ACCEPTOR, node2Uuid, node2Recovery);
        RecoveryAcceptorHandshakeManager shm2 = createRecoveryAcceptorHandshakeManager(ACCEPTOR, node2Uuid, node2Recovery);

        // Channel opened from node1 to node2 is channel 1.
        // Channel opened from node2 to node1 is channel 2.

        // Channel 1.
        setupChannel(channel1Src, chm1, noMessageListener);
        setupChannel(channel1Dst, shm2, noMessageListener);

        // Channel 2.
        setupChannel(channel2Src, chm2, noMessageListener);
        setupChannel(channel2Dst, shm1, noMessageListener);

        // Channel 2's handshake acquires both locks.
        exchangeInitiatorToAcceptor(channel2Dst, channel2Src);
        exchangeAcceptorToInitiator(channel2Dst, channel2Src);
        exchangeInitiatorToAcceptor(channel2Dst, channel2Src);

        // Now Channel 1's handshake cannot acquire even first lock.
        exchangeInitiatorToAcceptor(channel1Dst, channel1Src);
        exchangeAcceptorToInitiator(channel1Dst, channel1Src);

        // 2 -> 1 is alive, while 1 -> 2 closes because it is late.
        exchangeAcceptorToInitiator(channel2Dst, channel2Src);
        assertFalse(channel1Src.isOpen());

        assertTrue(channel2Src.isOpen());
        assertTrue(channel2Dst.isOpen());

        assertFalse(channel1Src.finish());
        assertFalse(channel2Dst.finish());
        assertFalse(channel2Src.finish());
        assertFalse(channel1Dst.finish());
    }

    @Test
    public void testExactlyOnceAcceptor() throws Exception {
        testExactlyOnce(true);
    }

    @Test
    public void testExactlyOnceInitiator() throws Exception {
        testExactlyOnce(false);
    }

    /**
     * Tests that message was received exactly once in case of network failure during acknowledgement.
     *
     * @param acceptorDidntReceiveAck {@code true} if acceptor didn't receive the acknowledgement, {@code false} if initiator didn't receive
     *                              the acknowledgement.
     * @throws Exception If failed.
     */
    private void testExactlyOnce(boolean acceptorDidntReceiveAck) throws Exception {
        var acceptor = ACCEPTOR;
        UUID acceptorLaunchId = UUID.randomUUID();
        var initiator = INITIATOR;
        UUID initiatorLaunchId = UUID.randomUUID();

        RecoveryDescriptorProvider initiatorRecovery = createRecoveryDescriptorProvider();
        RecoveryDescriptorProvider acceptorRecovery = createRecoveryDescriptorProvider();

        EmbeddedChannel initiatorSideChannel = createUnregisteredChannel();
        EmbeddedChannel acceptorSideChannel = createUnregisteredChannel();

        RecoveryInitiatorHandshakeManager initiatorHandshakeManager = createRecoveryInitiatorHandshakeManager(
                initiator,
                initiatorLaunchId,
                initiatorRecovery
        );
        RecoveryAcceptorHandshakeManager acceptorHandshakeManager = createRecoveryAcceptorHandshakeManager(
                acceptor,
                acceptorLaunchId,
                acceptorRecovery
        );

        var receivedFirst = new AtomicBoolean();

        var listener1 = new MessageListener("1", receivedFirst);

        setupChannel(initiatorSideChannel, initiatorHandshakeManager, acceptorDidntReceiveAck ? listener1 : noMessageListener);
        setupChannel(acceptorSideChannel, acceptorHandshakeManager, acceptorDidntReceiveAck ?  noMessageListener : listener1);

        // Normal handshake
        exchangeInitiatorToAcceptor(acceptorSideChannel, initiatorSideChannel);
        exchangeAcceptorToInitiator(acceptorSideChannel, initiatorSideChannel);
        exchangeInitiatorToAcceptor(acceptorSideChannel, initiatorSideChannel);
        exchangeAcceptorToInitiator(acceptorSideChannel, initiatorSideChannel);

        EmbeddedChannel ch = acceptorDidntReceiveAck ? acceptorSideChannel : initiatorSideChannel;

        // Add two messages to the outbound
        ch.writeOutbound(new OutNetworkObject(TEST_MESSAGES_FACTORY.testMessage().msg("1").build(), Collections.emptyList()));
        ch.writeOutbound(new OutNetworkObject(TEST_MESSAGES_FACTORY.testMessage().msg("2").build(), Collections.emptyList()));

        // Send one of the messages
        if (acceptorDidntReceiveAck) {
            exchangeAcceptorToInitiator(acceptorSideChannel, initiatorSideChannel);
        } else {
            exchangeInitiatorToAcceptor(acceptorSideChannel, initiatorSideChannel);
        }

        // Message should be received
        assertTrue(receivedFirst.get());

        // Transfer only one acknowledgement, don't transfer the second one (simulates network failure on acknowledgement)
        if (acceptorDidntReceiveAck) {
            exchangeInitiatorToAcceptor(acceptorSideChannel, initiatorSideChannel);
        } else {
            exchangeAcceptorToInitiator(acceptorSideChannel, initiatorSideChannel);
        }

        // Simulate reconnection
        initiatorHandshakeManager = createRecoveryInitiatorHandshakeManager(
                initiator,
                initiatorLaunchId,
                initiatorRecovery
        );
        acceptorHandshakeManager = createRecoveryAcceptorHandshakeManager(
                acceptor,
                acceptorLaunchId,
                acceptorRecovery
        );

        var receivedSecond = new AtomicBoolean();

        var listener2 = new MessageListener("2", receivedSecond);

        initiatorSideChannel.finishAndReleaseAll();
        acceptorSideChannel.finishAndReleaseAll();

        initiatorSideChannel = createUnregisteredChannel();
        acceptorSideChannel = createUnregisteredChannel();

        setupChannel(initiatorSideChannel, initiatorHandshakeManager, acceptorDidntReceiveAck ? listener2 : noMessageListener);
        setupChannel(acceptorSideChannel, acceptorHandshakeManager, acceptorDidntReceiveAck ? noMessageListener : listener2);

        // Handshake
        exchangeInitiatorToAcceptor(acceptorSideChannel, initiatorSideChannel);
        exchangeAcceptorToInitiator(acceptorSideChannel, initiatorSideChannel);
        exchangeInitiatorToAcceptor(acceptorSideChannel, initiatorSideChannel);
        exchangeAcceptorToInitiator(acceptorSideChannel, initiatorSideChannel);

        // Resending message
        if (acceptorDidntReceiveAck) {
            exchangeAcceptorToInitiator(acceptorSideChannel, initiatorSideChannel);
        } else {
            exchangeInitiatorToAcceptor(acceptorSideChannel, initiatorSideChannel);
        }

        // Send another acknowledgement
        if (acceptorDidntReceiveAck) {
            exchangeInitiatorToAcceptor(acceptorSideChannel, initiatorSideChannel);
        } else {
            exchangeAcceptorToInitiator(acceptorSideChannel, initiatorSideChannel);
        }

        assertNull(acceptorSideChannel.readOutbound());
        assertNull(initiatorSideChannel.readOutbound());

        assertTrue(receivedSecond.get());

        assertFalse(acceptorSideChannel.finish());
        assertFalse(initiatorSideChannel.finish());
    }

    @Test
    public void acceptorFailsHandshakeIfInitiatorIdIsAlreadySeen() throws Exception {
        RecoveryDescriptorProvider initiatorRecovery = createRecoveryDescriptorProvider();
        RecoveryDescriptorProvider acceptorRecovery = createRecoveryDescriptorProvider();

        EmbeddedChannel initiatorSideChannel = createUnregisteredChannel();
        EmbeddedChannel acceptorSideChannel = createUnregisteredChannel();

        RecoveryInitiatorHandshakeManager initiatorHandshakeManager = createRecoveryInitiatorHandshakeManager(
                initiatorRecovery
        );
        RecoveryAcceptorHandshakeManager acceptorHandshakeManager = createRecoveryAcceptorHandshakeManager(
                ACCEPTOR,
                UUID.randomUUID(),
                acceptorRecovery,
                new AllIdsAreStale()
        );

        setupChannel(initiatorSideChannel, initiatorHandshakeManager, noMessageListener);
        setupChannel(acceptorSideChannel, acceptorHandshakeManager, noMessageListener);

        assertTrue(acceptorSideChannel.isActive());

        exchangeInitiatorToAcceptor(acceptorSideChannel, initiatorSideChannel);
        exchangeAcceptorToInitiator(acceptorSideChannel, initiatorSideChannel);
        exchangeInitiatorToAcceptor(acceptorSideChannel, initiatorSideChannel);
        exchangeAcceptorToInitiator(acceptorSideChannel, initiatorSideChannel);

        assertNull(initiatorSideChannel.readOutbound());
        assertNull(acceptorSideChannel.readOutbound());

        checkHandshakeCompletedExceptionally(acceptorHandshakeManager);
        checkHandshakeCompletedExceptionally(initiatorHandshakeManager);

        checkPipelineAfterHandshake(acceptorSideChannel);
        checkPipelineAfterHandshake(initiatorSideChannel);

        assertFalse(acceptorSideChannel.finish());
        assertFalse(initiatorSideChannel.finish());
    }

    @Test
    public void initiatorFailsHandshakeIfAcceptorIdIsAlreadySeen() throws Exception {
        RecoveryDescriptorProvider initiatorRecovery = createRecoveryDescriptorProvider();
        RecoveryDescriptorProvider acceptorRecovery = createRecoveryDescriptorProvider();

        EmbeddedChannel initiatorSideChannel = createUnregisteredChannel();
        EmbeddedChannel acceptorSideChannel = createUnregisteredChannel();

        RecoveryInitiatorHandshakeManager initiatorHandshakeManager = createRecoveryInitiatorHandshakeManager(
                INITIATOR,
                UUID.randomUUID(),
                initiatorRecovery,
                new AllIdsAreStale()
        );
        RecoveryAcceptorHandshakeManager acceptorHandshakeManager = createRecoveryAcceptorHandshakeManager(
                acceptorRecovery
        );

        setupChannel(initiatorSideChannel, initiatorHandshakeManager, noMessageListener);
        setupChannel(acceptorSideChannel, acceptorHandshakeManager, noMessageListener);

        assertTrue(acceptorSideChannel.isActive());

        exchangeInitiatorToAcceptor(acceptorSideChannel, initiatorSideChannel);
        exchangeAcceptorToInitiator(acceptorSideChannel, initiatorSideChannel);
        exchangeInitiatorToAcceptor(acceptorSideChannel, initiatorSideChannel);

        assertNull(initiatorSideChannel.readOutbound());
        assertNull(acceptorSideChannel.readOutbound());

        checkHandshakeCompletedExceptionally(acceptorHandshakeManager);
        checkHandshakeCompletedExceptionally(initiatorHandshakeManager);

        checkPipelineAfterHandshake(acceptorSideChannel);
        checkPipelineAfterHandshake(initiatorSideChannel);

        assertFalse(acceptorSideChannel.finish());
        assertFalse(initiatorSideChannel.finish());
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

    private void exchangeAcceptorToInitiator(EmbeddedChannel acceptorSideChannel, EmbeddedChannel initiatorSideChannel) {
        runPendingTasks(acceptorSideChannel, initiatorSideChannel);

        ByteBuf outgoingMessageBuffer = acceptorSideChannel.readOutbound();
        // No need to release buffer because inbound buffers are released by InboundDecoder
        initiatorSideChannel.writeInbound(outgoingMessageBuffer);
    }

    private void exchangeInitiatorToAcceptor(EmbeddedChannel acceptorSideChannel, EmbeddedChannel initiatorSideChannel) {
        runPendingTasks(acceptorSideChannel, initiatorSideChannel);

        ByteBuf outgoingMessageBuffer = initiatorSideChannel.readOutbound();
        // No need to release buffer because inbound buffers are released by InboundDecoder
        acceptorSideChannel.writeInbound(outgoingMessageBuffer);
    }

    private void runPendingTasks(EmbeddedChannel channel1, EmbeddedChannel channel2) {
        Queue<?> evtLoop1Queue = getFieldValue(channel1.eventLoop(), AbstractScheduledEventExecutor.class, "scheduledTaskQueue");
        Queue<?> evtLoop2Queue = getFieldValue(channel2.eventLoop(), AbstractScheduledEventExecutor.class, "scheduledTaskQueue");

        try {
            assertTrue(IgniteTestUtils.waitForCondition(() -> {
                channel1.runPendingTasks();
                channel2.runPendingTasks();

                return nullOrEmpty(evtLoop1Queue) && nullOrEmpty(evtLoop2Queue);
            }, 10_000));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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

    private RecoveryInitiatorHandshakeManager createRecoveryInitiatorHandshakeManager(
            RecoveryDescriptorProvider provider
    ) {
        return createRecoveryInitiatorHandshakeManager(INITIATOR, UUID.randomUUID(), provider);
    }

    private RecoveryInitiatorHandshakeManager createRecoveryInitiatorHandshakeManager(
            String consistentId,
            UUID launchId,
            RecoveryDescriptorProvider provider
    ) {
        return createRecoveryInitiatorHandshakeManager(consistentId, launchId, provider, new AllIdsAreFresh());
    }

    private RecoveryInitiatorHandshakeManager createRecoveryInitiatorHandshakeManager(
            String consistentId,
            UUID launchId,
            RecoveryDescriptorProvider provider,
            StaleIdDetector staleIdDetector
    ) {
        return new RecoveryInitiatorHandshakeManager(
                new ClusterNodeImpl(launchId, consistentId, new NetworkAddress(INITIATOR_HOST, PORT)),
                CONNECTION_ID,
                provider,
                new NoOpHandshakeEventLoopSwitcher(),
                staleIdDetector,
                clusterIdSupplier,
                channel -> {},
                () -> false,
                new DefaultIgniteProductVersionSource(),
                topologyService,
                new FailureManager(new NoOpFailureHandler())
        );
    }

    private RecoveryAcceptorHandshakeManager createRecoveryAcceptorHandshakeManager(
            RecoveryDescriptorProvider provider
    ) {
        return createRecoveryAcceptorHandshakeManager(ACCEPTOR, UUID.randomUUID(), provider);
    }

    private RecoveryAcceptorHandshakeManager createRecoveryAcceptorHandshakeManager(
            String consistentId,
            UUID launchId,
            RecoveryDescriptorProvider provider
    ) {
        return createRecoveryAcceptorHandshakeManager(consistentId, launchId, provider, new AllIdsAreFresh());
    }

    private RecoveryAcceptorHandshakeManager createRecoveryAcceptorHandshakeManager(
            String consistentId,
            UUID launchId,
            RecoveryDescriptorProvider provider,
            StaleIdDetector staleIdDetector
    ) {
        return new RecoveryAcceptorHandshakeManager(
                new ClusterNodeImpl(launchId, consistentId, new NetworkAddress(ACCEPTOR_HOST, PORT)),
                MESSAGE_FACTORY,
                provider,
                new NoOpHandshakeEventLoopSwitcher(),
                staleIdDetector,
                clusterIdSupplier,
                channel -> {},
                () -> false,
                new DefaultIgniteProductVersionSource(),
                topologyService
        );
    }

    private RecoveryDescriptorProvider createRecoveryDescriptorProvider() {
        return new DefaultRecoveryDescriptorProvider();
    }
}
