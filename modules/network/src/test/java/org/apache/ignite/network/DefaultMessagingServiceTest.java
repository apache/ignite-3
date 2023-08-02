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

package org.apache.ignite.network;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.utils.ClusterServiceTestUtils.defaultSerializationRegistry;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.messages.AllTypesMessageImpl;
import org.apache.ignite.internal.network.messages.TestMessage;
import org.apache.ignite.internal.network.messages.TestMessageImpl;
import org.apache.ignite.internal.network.messages.TestMessageSerializationFactory;
import org.apache.ignite.internal.network.messages.TestMessageTypes;
import org.apache.ignite.internal.network.messages.TestMessagesFactory;
import org.apache.ignite.internal.network.netty.ConnectionManager;
import org.apache.ignite.internal.network.recovery.AllIdsAreFresh;
import org.apache.ignite.internal.network.recovery.RecoveryClientHandshakeManager;
import org.apache.ignite.internal.network.recovery.RecoveryClientHandshakeManagerFactory;
import org.apache.ignite.internal.network.recovery.RecoveryDescriptorProvider;
import org.apache.ignite.internal.network.recovery.StaleIdDetector;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.SerializationService;
import org.apache.ignite.internal.network.serialization.UserObjectSerializationContext;
import org.apache.ignite.internal.network.serialization.marshal.DefaultUserObjectMarshaller;
import org.apache.ignite.internal.network.serialization.marshal.UserObjectMarshaller;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.serialization.MessageDeserializer;
import org.apache.ignite.network.serialization.MessageSerializationFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.network.serialization.MessageSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(ConfigurationExtension.class)
class DefaultMessagingServiceTest {
    private static final int SENDER_PORT = 2001;
    private static final int RECEIVER_PORT = 2002;

    private static final ChannelType TEST_CHANNEL = ChannelType.register(Short.MAX_VALUE, "Test");

    @Mock
    private TopologyService topologyService;

    @InjectConfiguration("mock.port=" + SENDER_PORT)
    private NetworkConfiguration senderNetworkConfig;

    @InjectConfiguration("mock.port=" + RECEIVER_PORT)
    private NetworkConfiguration receiverNetworkConfig;

    private final NetworkMessagesFactory networkMessagesFactory = new NetworkMessagesFactory();
    private final TestMessagesFactory testMessagesFactory = new TestMessagesFactory();
    private final MessageSerializationRegistry messageSerializationRegistry = defaultSerializationRegistry();

    private final ClusterNode senderNode = new ClusterNode(
            "sender",
            "sender",
            new NetworkAddress("localhost", SENDER_PORT)
    );

    private final ClusterNode receiverNode = new ClusterNode(
            "receiver",
            "receiver",
            new NetworkAddress("localhost", RECEIVER_PORT)
    );

    @BeforeEach
    void setUp() {
        lenient().when(topologyService.getByConsistentId(eq(senderNode.name()))).thenReturn(senderNode);
    }

    @Test
    void messagesSentBeforeChannelStartAreDeliveredInCorrectOrder() throws Exception {
        CountDownLatch allowSendLatch = new CountDownLatch(1);

        try (
                Services senderServices = createMessagingService(senderNode, senderNetworkConfig, () -> awaitQuietly(allowSendLatch));
                Services receiverServices = createMessagingService(receiverNode, receiverNetworkConfig, () -> {})
        ) {
            List<String> payloads = new CopyOnWriteArrayList<>();
            CountDownLatch messagesDeliveredLatch = new CountDownLatch(2);

            receiverServices.messagingService.addMessageHandler(
                    TestMessageTypes.class,
                    (message, sender, correlationId) -> {
                        payloads.add(((TestMessage) message).msg());
                        messagesDeliveredLatch.countDown();
                    }
            );

            senderServices.messagingService.send(receiverNode, testMessage("one"));
            senderServices.messagingService.send(receiverNode, testMessage("two"));

            allowSendLatch.countDown();

            assertTrue(messagesDeliveredLatch.await(1, TimeUnit.SECONDS));

            assertThat(payloads, contains("one", "two"));
        }
    }

    @Test
    void respondingWhenSenderIsNotInTopologyResultsInFailingFuture() throws Exception {
        try (Services services = createMessagingService(senderNode, senderNetworkConfig, () -> {})) {
            CompletableFuture<Void> resultFuture = services.messagingService.respond("no-such-node", mock(NetworkMessage.class), 123);

            assertThat(resultFuture, willThrow(UnresolvableConsistentIdException.class));
        }
    }

    @Test
    public void sendMessagesOneChannel() throws Exception {
        AtomicBoolean release = new AtomicBoolean(false);
        MessageSerializer<TestMessage> serializer = new TestMessageSerializationFactory(
                new TestMessagesFactory()).createSerializer();
        Serializer longWaitSerializer = new Serializer(TestMessageImpl.GROUP_TYPE, TestMessageImpl.TYPE,
                (message, writer) -> release.get()
                        && serializer.writeMessage((TestMessage) message, writer));
        try (Services services = createMessagingService(
                senderNode,
                senderNetworkConfig,
                () -> {},
                mockSerializationRegistry(longWaitSerializer));
                Services receiverServices = createMessagingService(receiverNode, receiverNetworkConfig, () -> {})
        ) {
            CountDownLatch latch = new CountDownLatch(2);
            receiverServices.messagingService.addMessageHandler(
                    TestMessageTypes.class,
                    (message, sender, correlationId) -> latch.countDown()
            );

            services.messagingService.send(receiverNode, TestMessageImpl.builder().build());
            services.messagingService.send(receiverNode, AllTypesMessageImpl.builder().build());

            assertThat(latch.getCount(), is(2L));
            release.set(true);
            assertTrue(latch.await(1, TimeUnit.SECONDS));
        }
    }

    @Test
    public void sendMessagesTwoChannels() throws Exception {
        AtomicBoolean release = new AtomicBoolean(false);
        MessageSerializer<TestMessage> serializer = new TestMessageSerializationFactory(
                new TestMessagesFactory()).createSerializer();
        Serializer longWaitSerializer = new Serializer(TestMessageImpl.GROUP_TYPE, TestMessageImpl.TYPE,
                (message, writer) -> release.get()
                        && serializer.writeMessage((TestMessage) message, writer));
        try (Services services = createMessagingService(
                senderNode,
                senderNetworkConfig,
                () -> {},
                mockSerializationRegistry(longWaitSerializer));
                Services receiverServices = createMessagingService(receiverNode, receiverNetworkConfig, () -> {})
        ) {
            CountDownLatch latch = new CountDownLatch(2);
            receiverServices.messagingService.addMessageHandler(
                    TestMessageTypes.class,
                    (message, sender, correlationId) -> latch.countDown()
            );

            services.messagingService.send(receiverNode, TestMessageImpl.builder().build());
            services.messagingService.send(receiverNode, TEST_CHANNEL, AllTypesMessageImpl.builder().build());

            await().timeout(1, TimeUnit.SECONDS)
                    .until(() -> latch.getCount() == 1);

            release.set(true);
            assertTrue(latch.await(1, TimeUnit.SECONDS));
        }
    }

    private static MessageSerializationRegistry mockSerializationRegistry(Serializer... serializers) {
        MessageSerializationRegistry defaultRegistry = defaultSerializationRegistry();

        MessageSerializationRegistry wrapper = new MessageSerializationRegistry() {
            @Override
            public MessageSerializationRegistry registerFactory(short groupType, short messageType,
                    MessageSerializationFactory<?> factory) {
                return this;
            }

            @Override
            public <T extends NetworkMessage> MessageSerializer<T> createSerializer(short groupType, short messageType) {
                for (Serializer serializer : serializers) {
                    if (serializer.groupType == groupType && serializer.messageType == messageType) {
                        return (MessageSerializer<T>) serializer.serializer;
                    }
                }
                return defaultRegistry.createSerializer(groupType, messageType);
            }

            @Override
            public <T extends NetworkMessage> MessageDeserializer<T> createDeserializer(short groupType, short messageType) {
                return defaultRegistry.createDeserializer(groupType, messageType);
            }
        };

        return wrapper;
    }

    private static class Serializer {
        private final short groupType;
        private final short messageType;
        private final MessageSerializer<? extends NetworkMessage> serializer;

        private Serializer(short groupType, short messageType, MessageSerializer<? extends NetworkMessage> serializer) {
            this.groupType = groupType;
            this.messageType = messageType;
            this.serializer = serializer;
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private TestMessage testMessage(String message) {
        return testMessagesFactory.testMessage().msg(message).build();
    }

    private Services createMessagingService(ClusterNode node, NetworkConfiguration networkConfig, Runnable beforeHandshake) {
        return createMessagingService(node, networkConfig, beforeHandshake, messageSerializationRegistry);
    }

    private Services createMessagingService(
            ClusterNode node,
            NetworkConfiguration networkConfig,
            Runnable beforeHandshake,
            MessageSerializationRegistry registry
    ) {
        ClassDescriptorRegistry classDescriptorRegistry = new ClassDescriptorRegistry();
        ClassDescriptorFactory classDescriptorFactory = new ClassDescriptorFactory(classDescriptorRegistry);
        UserObjectMarshaller marshaller = new DefaultUserObjectMarshaller(classDescriptorRegistry, classDescriptorFactory);

        DefaultMessagingService messagingService = new DefaultMessagingService(
                node.name(),
                networkMessagesFactory,
                topologyService,
                classDescriptorRegistry,
                marshaller
        );

        SerializationService serializationService = new SerializationService(
                registry,
                new UserObjectSerializationContext(classDescriptorRegistry, classDescriptorFactory, marshaller)
        );

        String eventLoopGroupNamePrefix = node.name() + "-event-loop";

        NettyBootstrapFactory bootstrapFactory = new NettyBootstrapFactory(networkConfig, eventLoopGroupNamePrefix);
        bootstrapFactory.start();

        StaleIdDetector staleIdDetector = new AllIdsAreFresh();

        UUID launchId = UUID.randomUUID();
        ConnectionManager connectionManager = new ConnectionManager(
                networkConfig.value(),
                serializationService,
                launchId,
                node.name(),
                bootstrapFactory,
                staleIdDetector,
                clientHandshakeManagerFactoryAdding(beforeHandshake, staleIdDetector)
        );
        connectionManager.start();

        messagingService.setConnectionManager(connectionManager);

        return new Services(connectionManager, messagingService);
    }

    private static RecoveryClientHandshakeManagerFactory clientHandshakeManagerFactoryAdding(Runnable beforeHandshake,
            StaleIdDetector staleIdDetector) {
        return new RecoveryClientHandshakeManagerFactory() {
            @Override
            public RecoveryClientHandshakeManager create(
                    UUID launchId,
                    String consistentId,
                    short connectionId,
                    RecoveryDescriptorProvider recoveryDescriptorProvider) {
                return new RecoveryClientHandshakeManager(
                        launchId,
                        consistentId,
                        connectionId,
                        recoveryDescriptorProvider,
                        staleIdDetector,
                        channel -> {},
                        new AtomicBoolean(false)
                ) {
                    @Override
                    protected void finishHandshake() {
                        beforeHandshake.run();

                        super.finishHandshake();
                    }
                };
            }
        };
    }

    private static class Services implements AutoCloseable {
        private final ConnectionManager connectionManager;
        private final DefaultMessagingService messagingService;

        private Services(ConnectionManager connectionManager, DefaultMessagingService messagingService) {
            this.connectionManager = connectionManager;
            this.messagingService = messagingService;
        }

        @Override
        public void close() throws Exception {
            IgniteUtils.closeAll(connectionManager::stop, messagingService::stop);
        }
    }
}
