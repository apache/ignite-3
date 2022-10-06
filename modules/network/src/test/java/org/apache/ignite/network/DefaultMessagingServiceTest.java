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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.schemas.network.InboundView;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.configuration.schemas.network.NetworkView;
import org.apache.ignite.configuration.schemas.network.OutboundView;
import org.apache.ignite.internal.future.OrderedCompletableFuture;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.messages.TestMessage;
import org.apache.ignite.internal.network.messages.TestMessageSerializationFactory;
import org.apache.ignite.internal.network.messages.TestMessageTypes;
import org.apache.ignite.internal.network.messages.TestMessagesFactory;
import org.apache.ignite.internal.network.netty.ConnectionManager;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.SerializationService;
import org.apache.ignite.internal.network.serialization.UserObjectSerializationContext;
import org.apache.ignite.internal.network.serialization.marshal.DefaultUserObjectMarshaller;
import org.apache.ignite.internal.network.serialization.marshal.UserObjectMarshaller;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultMessagingServiceTest {
    private static final int SENDER_PORT = 2001;
    private static final int RECEIVER_PORT = 2002;

    @Mock
    private TopologyService topologyService;

    @Mock
    private NetworkConfiguration senderNetworkConfig;
    @Mock
    private NetworkView senderNetworkConfigView;
    @Mock
    private OutboundView senderOutboundConfig;
    @Mock
    private InboundView senderInboundConfig;

    @Mock
    private NetworkConfiguration receiverNetworkConfig;
    @Mock
    private NetworkView receiverNetworkConfigView;
    @Mock
    private OutboundView receiverOutboundConfig;
    @Mock
    private InboundView receiverInboundConfig;

    private final NetworkMessagesFactory networkMessagesFactory = new NetworkMessagesFactory();
    private final TestMessagesFactory testMessagesFactory = new TestMessagesFactory();
    private final MessageSerializationRegistryImpl messageSerializationRegistry = new MessageSerializationRegistryImpl();

    private final ClusterNode receiverNode = new ClusterNode(
            "receiver",
            "receiver",
            new NetworkAddress("localhost", RECEIVER_PORT, "receiver")
    );

    @BeforeEach
    void initSerializationRegistry() {
        messageSerializationRegistry.registerFactory(
                (short) 2,
                TestMessageTypes.TEST,
                new TestMessageSerializationFactory(testMessagesFactory)
        );
    }

    @Test
    void messagesSentBeforeChannelStartAreDeliveredInCorrectOrder() throws Exception {
        configureSender();
        configureReceiver();

        try (
                Services senderServices = createMessagingService("sender", "sender-network", senderNetworkConfig);
                Services receiverServices = createMessagingService("receiver", "receiver-network", receiverNetworkConfig)
        ) {
            List<String> payloads = new CopyOnWriteArrayList<>();
            CountDownLatch messagesDeliveredLatch = new CountDownLatch(2);

            receiverServices.messagingService.addMessageHandler(
                    TestMessageTypes.class,
                    (message, senderAddr, correlationId) -> {
                        payloads.add(((TestMessage) message).msg());
                        messagesDeliveredLatch.countDown();
                    }
            );

            CountDownLatch allowSendLatch = new CountDownLatch(1);
            senderServices.connectionManager.setBeforeHandshakeComplete(() -> awaitQuietly(allowSendLatch));

            senderServices.messagingService.send(receiverNode, testMessage("one"));
            senderServices.messagingService.send(receiverNode, testMessage("two"));

            allowSendLatch.countDown();

            assertTrue(messagesDeliveredLatch.await(1, TimeUnit.SECONDS));

            assertThat(payloads, contains("one", "two"));
        }
    }

    @Test
    void messageSendFutureIsOrdered() throws Exception {
        configureSender();

        try (Services senderServices = createMessagingService("sender", "sender-network", senderNetworkConfig)) {
            CompletableFuture<Void> sendFuture = senderServices.messagingService.send(receiverNode, testMessage("one"));

            assertThat(sendFuture, is(instanceOf(OrderedCompletableFuture.class)));
        }
    }

    private void configureSender() {
        when(senderNetworkConfigView.port()).thenReturn(SENDER_PORT);
        configureNetworkDefaults(senderNetworkConfig, senderNetworkConfigView, senderOutboundConfig, senderInboundConfig);
    }

    private void configureReceiver() {
        when(receiverNetworkConfigView.port()).thenReturn(RECEIVER_PORT);
        configureNetworkDefaults(receiverNetworkConfig, receiverNetworkConfigView, receiverOutboundConfig, receiverInboundConfig);
    }

    private static void configureNetworkDefaults(
            NetworkConfiguration networkConfig,
            NetworkView networkConfigView,
            OutboundView outboundConfig,
            InboundView inboundConfig
    ) {
        when(networkConfig.value()).thenReturn(networkConfigView);
        when(networkConfigView.portRange()).thenReturn(0);
        when(networkConfigView.outbound()).thenReturn(outboundConfig);
        when(networkConfigView.inbound()).thenReturn(inboundConfig);
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

    private Services createMessagingService(String consistentId, String senderEventLoopGroupNamePrefix,
            NetworkConfiguration networkConfig) {
        ClassDescriptorRegistry classDescriptorRegistry = new ClassDescriptorRegistry();
        ClassDescriptorFactory classDescriptorFactory = new ClassDescriptorFactory(classDescriptorRegistry);
        UserObjectMarshaller marshaller = new DefaultUserObjectMarshaller(classDescriptorRegistry, classDescriptorFactory);
        DefaultMessagingService messagingService = new DefaultMessagingService(
                networkMessagesFactory,
                topologyService,
                classDescriptorRegistry,
                marshaller
        );

        SerializationService serializationService = new SerializationService(
                messageSerializationRegistry,
                new UserObjectSerializationContext(classDescriptorRegistry, classDescriptorFactory, marshaller)
        );
        NettyBootstrapFactory bootstrapFactory = new NettyBootstrapFactory(networkConfig, senderEventLoopGroupNamePrefix);
        bootstrapFactory.start();

        ConnectionManager connectionManager = new ConnectionManager(networkConfig.value(), serializationService, UUID.randomUUID(),
                consistentId, bootstrapFactory);
        connectionManager.start();

        messagingService.setConnectionManager(connectionManager);

        return new Services(connectionManager, messagingService);
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
