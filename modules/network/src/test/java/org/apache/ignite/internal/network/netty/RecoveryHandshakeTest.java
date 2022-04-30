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

package org.apache.ignite.internal.network.netty;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.recovery.RecoveryClientHandshakeManager;
import org.apache.ignite.internal.network.recovery.RecoveryDescriptor;
import org.apache.ignite.internal.network.recovery.RecoveryDescriptorProvider;
import org.apache.ignite.internal.network.recovery.RecoveryServerHandshakeManager;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.PerSessionSerializationService;
import org.apache.ignite.internal.network.serialization.SerializationService;
import org.apache.ignite.internal.network.serialization.UserObjectSerializationContext;
import org.apache.ignite.internal.network.serialization.marshal.DefaultUserObjectMarshaller;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.OutNetworkObject;
import org.apache.ignite.network.TestMessage;
import org.apache.ignite.network.TestMessageSerializationRegistryImpl;
import org.apache.ignite.network.TestMessagesFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.junit.jupiter.api.Test;

/**
 * Recovery protocol handshake flow test.
 */
public class RecoveryHandshakeTest {
    /** Connection id. */
    private static final short CONNECTION_ID = 1337;

    /** Serialization registry. */
    private static final MessageSerializationRegistry MESSAGE_REGISTRY = new TestMessageSerializationRegistryImpl();

    /** Message factory. */
    private static final NetworkMessagesFactory MESSAGE_FACTORY = new NetworkMessagesFactory();

    /** Test message factory. */
    private static final TestMessagesFactory TEST_MESSAGES_FACTORY = new TestMessagesFactory();

    @Test
    public void testHandshake() throws Exception {
        RecoveryDescriptorProvider clientRecovery = createRecoveryDescriptorProvider();
        RecoveryDescriptorProvider serverRecovery = createRecoveryDescriptorProvider();

        var clientHandshakeManager = createRecoveryClientHandshakeManager(clientRecovery);
        var serverHandshakeManager = createRecoveryServerHandshakeManager(serverRecovery);

        EmbeddedChannel clientSideChannel = setupChannel(clientHandshakeManager, noMessageListener);

        EmbeddedChannel serverSideChannel = setupChannel(serverHandshakeManager, noMessageListener);

        assertTrue(serverSideChannel.isActive());

        exchangeServerToClient(serverSideChannel, clientSideChannel);
        exchangeClientToServer(serverSideChannel, clientSideChannel);
        exchangeServerToClient(serverSideChannel, clientSideChannel);

        assertNull(clientSideChannel.readOutbound());
        assertNull(serverSideChannel.readOutbound());

        checkHandshakeCompleted(serverHandshakeManager);
        checkHandshakeCompleted(clientHandshakeManager);

        checkPipelineAfterHandshake(serverSideChannel);
        checkPipelineAfterHandshake(clientSideChannel);
    }

    @Test
    public void testHandshakeWithUnacknowledgedServerMessage() throws Exception {
        RecoveryDescriptorProvider clientRecovery = createRecoveryDescriptorProvider();
        RecoveryDescriptorProvider serverRecovery = createRecoveryDescriptorProvider();

        UUID clientLaunchId = UUID.randomUUID();
        RecoveryDescriptor serverRecoveryDescriptor = serverRecovery.getRecoveryDescriptor("client", clientLaunchId, CONNECTION_ID, true);
        addUnacknowledgedMessages(serverRecoveryDescriptor);

        var clientHandshakeManager = createRecoveryClientHandshakeManager("client", clientLaunchId, clientRecovery);
        var serverHandshakeManager = createRecoveryServerHandshakeManager(serverRecovery);

        var messageCaptor = new AtomicReference<TestMessage>();
        EmbeddedChannel clientSideChannel = setupChannel(clientHandshakeManager, (inObject) -> {
            NetworkMessage msg = inObject.message();

            assertInstanceOf(TestMessage.class, msg);

            messageCaptor.set((TestMessage) msg);
        });

        EmbeddedChannel serverSideChannel = setupChannel(serverHandshakeManager, noMessageListener);

        assertTrue(serverSideChannel.isActive());

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
    }

    @Test
    public void testHandshakeWithUnacknowledgedClientMessage() throws Exception {
        RecoveryDescriptorProvider clientRecovery = createRecoveryDescriptorProvider();
        RecoveryDescriptorProvider serverRecovery = createRecoveryDescriptorProvider();

        UUID serverLaunchId = UUID.randomUUID();
        RecoveryDescriptor clientRecoveryDescriptor = clientRecovery.getRecoveryDescriptor("server", serverLaunchId, CONNECTION_ID, false);
        addUnacknowledgedMessages(clientRecoveryDescriptor);

        var clientHandshakeManager = createRecoveryClientHandshakeManager(clientRecovery);
        var serverHandshakeManager = createRecoveryServerHandshakeManager("server", serverLaunchId, serverRecovery);

        var messageCaptor = new AtomicReference<TestMessage>();
        EmbeddedChannel clientSideChannel = setupChannel(clientHandshakeManager, noMessageListener);

        EmbeddedChannel serverSideChannel = setupChannel(serverHandshakeManager, (inObject) -> {
            NetworkMessage msg = inObject.message();

            assertInstanceOf(TestMessage.class, msg);

            messageCaptor.set((TestMessage) msg);
        });

        assertTrue(serverSideChannel.isActive());

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
    }

    @Test
    public void testPairedRecoveryDescriptors() throws Exception {
        RecoveryDescriptorProvider node1Recovery = createRecoveryDescriptorProvider();
        RecoveryDescriptorProvider node2Recovery = createRecoveryDescriptorProvider();

        var node1Uuid = UUID.randomUUID();
        var node2Uuid = UUID.randomUUID();

        var chm1 = createRecoveryClientHandshakeManager("client", node1Uuid, node1Recovery);
        var shm1 = createRecoveryServerHandshakeManager("client", node1Uuid, node1Recovery);

        var chm2 = createRecoveryClientHandshakeManager("server", node2Uuid, node2Recovery);
        var shm2 = createRecoveryServerHandshakeManager("server", node2Uuid, node2Recovery);

        EmbeddedChannel out1to2 = setupChannel(chm1, noMessageListener);
        EmbeddedChannel in1to2 = setupChannel(shm1, noMessageListener);
        EmbeddedChannel out2to1 = setupChannel(chm2, noMessageListener);
        EmbeddedChannel in2to1 = setupChannel(shm2, noMessageListener);

        exchangeServerToClient(in1to2, out2to1);
        exchangeServerToClient(in2to1, out1to2);

        exchangeClientToServer(in1to2, out2to1);
        exchangeClientToServer(in2to1, out1to2);

        assertNotSame(chm1.recoveryDescriptor(), shm1.recoveryDescriptor());
        assertNotSame(chm2.recoveryDescriptor(), shm2.recoveryDescriptor());
    }

    private void checkPipelineAfterHandshake(EmbeddedChannel channel) {
        assertNull(channel.pipeline().get(HandshakeHandler.NAME));
    }

    private void checkHandshakeNotCompleted(HandshakeManager manager) {
        CompletableFuture<NettySender> handshakeFuture = manager.handshakeFuture();
        assertFalse(handshakeFuture.isDone());
        assertFalse(handshakeFuture.isCompletedExceptionally());
        assertFalse(handshakeFuture.isCancelled());
    }

    private void checkHandshakeCompleted(HandshakeManager manager) {
        CompletableFuture<NettySender> handshakeFuture = manager.handshakeFuture();
        assertTrue(handshakeFuture.isDone());
        assertFalse(handshakeFuture.isCompletedExceptionally());
        assertFalse(handshakeFuture.isCancelled());
    }

    private void addUnacknowledgedMessages(RecoveryDescriptor recoveryDescriptor) {
        TestMessage msg = TEST_MESSAGES_FACTORY.testMessage().msg("test").build();
        recoveryDescriptor.add(new OutNetworkObject(msg, Collections.emptyList()));
    }

    private void exchangeServerToClient(EmbeddedChannel serverSideChannel, EmbeddedChannel clientSideChannel) {
        ByteBuf handshakeStartMessage = serverSideChannel.readOutbound();
        clientSideChannel.writeInbound(handshakeStartMessage);
    }

    private void exchangeClientToServer(EmbeddedChannel serverSideChannel, EmbeddedChannel clientSideChannel) {
        ByteBuf handshakeStartMessage = clientSideChannel.readOutbound();
        serverSideChannel.writeInbound(handshakeStartMessage);
    }

    private final Consumer<InNetworkObject> noMessageListener = inNetworkObject -> {
        fail("Received message while shouldn't have, [" + inNetworkObject.message() + "]");
    };

    private EmbeddedChannel setupChannel(HandshakeManager handshakeManager, Consumer<InNetworkObject> messageListener) throws Exception {
        // Channel should not be registered at first, not before we add pipeline handlers
        // Otherwise, events like "channel active" won't be propagated to the handlers
        var channel = new EmbeddedChannel(false, false);

        var serializationService = new SerializationService(MESSAGE_REGISTRY, createUserObjectSerializationContext());
        var sessionSerializationService = new PerSessionSerializationService(serializationService);

        PipelineUtils.setup(channel.pipeline(), sessionSerializationService, handshakeManager, messageListener);

        channel.register();

        return channel;
    }

    private UserObjectSerializationContext createUserObjectSerializationContext() {
        var userObjectDescriptorRegistry = new ClassDescriptorRegistry();
        var userObjectDescriptorFactory = new ClassDescriptorFactory(userObjectDescriptorRegistry);

        var userObjectMarshaller = new DefaultUserObjectMarshaller(userObjectDescriptorRegistry, userObjectDescriptorFactory);

        return new UserObjectSerializationContext(userObjectDescriptorRegistry, userObjectDescriptorFactory,
                userObjectMarshaller);
    }

    private RecoveryClientHandshakeManager createRecoveryClientHandshakeManager(RecoveryDescriptorProvider provider) {
        return createRecoveryClientHandshakeManager("client", UUID.randomUUID(), provider);
    }

    private RecoveryClientHandshakeManager createRecoveryClientHandshakeManager(String consistentId, UUID launchId,
            RecoveryDescriptorProvider provider) {
        return new RecoveryClientHandshakeManager(launchId, consistentId, CONNECTION_ID, MESSAGE_FACTORY, provider);
    }

    private RecoveryServerHandshakeManager createRecoveryServerHandshakeManager(RecoveryDescriptorProvider provider) {
        return createRecoveryServerHandshakeManager("server", UUID.randomUUID(), provider);
    }

    private RecoveryServerHandshakeManager createRecoveryServerHandshakeManager(String consistentId, UUID launchId,
            RecoveryDescriptorProvider provider) {
        return new RecoveryServerHandshakeManager(launchId, consistentId, MESSAGE_FACTORY, provider);
    }

    private RecoveryDescriptorProvider createRecoveryDescriptorProvider() {
        return new DefaultRecoveryDescriptorProvider();
    }
}
