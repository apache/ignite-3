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

package org.apache.ignite.internal.network;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.defaultSerializationRegistry;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.messages.AllTypesMessageImpl;
import org.apache.ignite.internal.network.messages.InstantContainer;
import org.apache.ignite.internal.network.messages.MessageWithInstant;
import org.apache.ignite.internal.network.messages.TestMessage;
import org.apache.ignite.internal.network.messages.TestMessageImpl;
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
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.network.serialization.SerializationService;
import org.apache.ignite.internal.network.serialization.UserObjectSerializationContext;
import org.apache.ignite.internal.network.serialization.marshal.DefaultUserObjectMarshaller;
import org.apache.ignite.internal.network.serialization.marshal.UserObjectMarshaller;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.worker.CriticalWorkerRegistry;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(ConfigurationExtension.class)
class DefaultMessagingServiceTest extends BaseIgniteAbstractTest {
    private static final int SENDER_PORT = 2001;
    private static final int RECEIVER_PORT = 2002;

    private static final ChannelType TEST_CHANNEL = ChannelType.register(Short.MAX_VALUE, "Test");

    @Mock
    private TopologyService topologyService;

    @Mock
    private CriticalWorkerRegistry criticalWorkerRegistry;

    @Mock
    private FailureProcessor failureProcessor;

    @InjectConfiguration("mock.port=" + SENDER_PORT)
    private NetworkConfiguration senderNetworkConfig;

    @InjectConfiguration("mock.port=" + RECEIVER_PORT)
    private NetworkConfiguration receiverNetworkConfig;

    private final NetworkMessagesFactory networkMessagesFactory = new NetworkMessagesFactory();
    private final TestMessagesFactory testMessagesFactory = new TestMessagesFactory();
    private final MessageSerializationRegistry messageSerializationRegistry = defaultSerializationRegistry();

    private final ClusterNode senderNode = new ClusterNodeImpl(
            UUID.randomUUID().toString(),
            "sender",
            new NetworkAddress("localhost", SENDER_PORT)
    );

    private final ClusterNode receiverNode = new ClusterNodeImpl(
            UUID.randomUUID().toString(),
            "receiver",
            new NetworkAddress("localhost", RECEIVER_PORT)
    );

    @BeforeEach
    void setUp() {
        lenient().when(topologyService.getByConsistentId(eq(senderNode.name()))).thenReturn(senderNode);
        lenient().when(topologyService.getByConsistentId(eq(receiverNode.name()))).thenReturn(receiverNode);
    }

    @Test
    void messagesSentBeforeChannelStartAreDeliveredInCorrectOrder() throws Exception {
        CountDownLatch allowSendLatch = new CountDownLatch(1);

        try (
                Services senderServices = createMessagingService(senderNode, senderNetworkConfig, () -> awaitQuietly(allowSendLatch));
                Services receiverServices = createMessagingService(receiverNode, receiverNetworkConfig)
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

            assertTrue(messagesDeliveredLatch.await(1, SECONDS));

            assertThat(payloads, contains("one", "two"));
        }
    }

    @Test
    void respondingWhenSenderIsNotInTopologyResultsInFailingFuture() throws Exception {
        try (Services services = createMessagingService(senderNode, senderNetworkConfig)) {
            CompletableFuture<Void> resultFuture = services.messagingService.respond("no-such-node", mock(NetworkMessage.class), 123);

            assertThat(resultFuture, willThrow(UnresolvableConsistentIdException.class));
        }
    }

    @Test
    public void sendMessagesTwoChannels() throws Exception {
        try (Services senderServices = createMessagingService(senderNode, senderNetworkConfig);
                Services receiverServices = createMessagingService(receiverNode, receiverNetworkConfig)
        ) {
            assertThat(receiverServices.connectionManager.channels(), is(anEmptyMap()));

            senderServices.messagingService.send(receiverNode, TestMessageImpl.builder().build());
            senderServices.messagingService.send(receiverNode, TEST_CHANNEL, AllTypesMessageImpl.builder().build());

            assertTrue(waitForCondition(() -> receiverServices.connectionManager.channels().size() == 2, 10_000));
        }
    }

    @Test
    public void differentChannelsAreHandledByDifferentInboundThreads() throws Exception {
        try (
                Services senderServices = createMessagingService(senderNode, senderNetworkConfig);
                Services receiverServices = createMessagingService(receiverNode, receiverNetworkConfig)
        ) {
            CountDownLatch bothDelivered = new CountDownLatch(2);
            CyclicBarrier barrier = new CyclicBarrier(2);

            receiverServices.messagingService.addMessageHandler(
                    TestMessageTypes.class,
                    (message, sender, correlationId) -> {
                        try {
                            barrier.await(10, SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        } catch (BrokenBarrierException | TimeoutException e) {
                            throw new RuntimeException(e);
                        }

                        bothDelivered.countDown();
                    }
            );

            senderServices.messagingService.send(receiverNode, TestMessageImpl.builder().build());

            senderServices.messagingService.send(receiverNode, TEST_CHANNEL, TestMessageImpl.builder().build());

            assertTrue(
                    bothDelivered.await(1, SECONDS),
                    "Did not see both messages delivered in time (probably, they ended up in the same inbound thread)"
            );
        }
    }

    @ParameterizedTest
    @EnumSource(SendOperation.class)
    void messageSendIsHandledInThreadCorrespondingToChannel(SendOperation operation) throws Exception {
        try (
                Services senderServices = createMessagingService(senderNode, senderNetworkConfig);
                Services receiverServices = createMessagingService(receiverNode, receiverNetworkConfig)
        ) {
            AtomicInteger channelId = new AtomicInteger(Integer.MAX_VALUE);

            receiverServices.messagingService.addMessageHandler(
                    TestMessageTypes.class,
                    (message, sender, correlationId) -> channelId.set(extractChannelIdFromThreadName(Thread.currentThread()))
            );

            operation.sendAction.send(senderServices.messagingService, TestMessageImpl.builder().build(), receiverNode);

            assertTrue(
                    waitForCondition(() -> channelId.get() != Integer.MAX_VALUE, SECONDS.toMillis(10)),
                    "Did not get any message in time"
            );

            assertThat(channelId.get(), is((int) operation.expectedChannelType.id()));
        }
    }

    @ParameterizedTest
    @EnumSource(RespondOperation.class)
    void respondIsHandledInThreadCorrespondingToChannel(RespondOperation operation) throws Exception {
        try (
                Services senderServices = createMessagingService(senderNode, senderNetworkConfig);
                Services receiverServices = createMessagingService(receiverNode, receiverNetworkConfig)
        ) {
            CountDownLatch invokeFuturePrepared = new CountDownLatch(1);

            receiverServices.messagingService.addMessageHandler(
                    TestMessageTypes.class,
                    (message, sender, correlationId) -> {
                        try {
                            invokeFuturePrepared.await(10, SECONDS);
                        } catch (InterruptedException ignored) {
                            // No-op.
                        }

                        operation.respondAction.respond(
                                receiverServices.messagingService,
                                message,
                                sender,
                                Objects.requireNonNull(correlationId)
                        );
                    }
            );

            AtomicInteger responseChannelId = new AtomicInteger(Integer.MAX_VALUE);

            CompletableFuture<NetworkMessage> invokeFuture = senderServices.messagingService.invoke(
                    receiverNode,
                    operation.expectedChannelType,
                    TestMessageImpl.builder().build(),
                    10_000
            );
            CompletableFuture<NetworkMessage> finalFuture = invokeFuture.whenComplete((res, ex) -> {
                responseChannelId.set(extractChannelIdFromThreadName(Thread.currentThread()));
            });

            // Only now allow the invoke response to be sent to make sure that our completion stage executes in the channel's
            // inbound thread.
            invokeFuturePrepared.countDown();

            assertThat(finalFuture, willCompleteSuccessfully());

            assertThat(responseChannelId.get(), is((int) operation.expectedChannelType.id()));
        }
    }

    private static int extractChannelIdFromThreadName(Thread thread) {
        Pattern pattern = Pattern.compile("^.+-(\\d+)-\\d+");

        Matcher matcher = pattern.matcher(thread.getName());

        boolean matches = matcher.matches();
        assert matches : thread.getName() + " does not match the pattern";

        return Integer.parseInt(matcher.group(1));
    }

    @Test
    void messageWithInstantInAnotherObjectSerializesAndDeserializesCorrectly() throws Exception {
        try (
                Services senderServices = createMessagingService(senderNode, senderNetworkConfig);
                Services receiverServices = createMessagingService(receiverNode, receiverNetworkConfig)
        ) {
            Instant sentInstant = Instant.now();
            CompletableFuture<Instant> receivedInstant = new CompletableFuture<>();

            receiverServices.messagingService.addMessageHandler(TestMessageTypes.class, (message, sender, correlationId) -> {
                if (message instanceof MessageWithInstant) {
                    receivedInstant.complete(((MessageWithInstant) message).instantContainer().instant());
                }
            });

            senderServices.messagingService.send(
                    receiverNode,
                    testMessagesFactory.messageWithInstant().instantContainer(new InstantContainer(sentInstant)).build()
            );

            assertThat(receivedInstant, willBe(sentInstant));
        }
    }

    @Test
    void handlersSubscribingWithoutExecutorChooserGetNotifiedInInboundThreads() throws Exception {
        try (
                Services senderServices = createMessagingService(senderNode, senderNetworkConfig);
                Services receiverServices = createMessagingService(receiverNode, receiverNetworkConfig)
        ) {
            CompletableFuture<Thread> handlerThreadFuture = new CompletableFuture<>();

            receiverServices.messagingService.addMessageHandler(TestMessageTypes.class, (message, sender, correlationId) -> {
                handlerThreadFuture.complete(Thread.currentThread());
            });

            senderServices.messagingService.send(receiverNode, testMessage("test"));

            assertThat(handlerThreadFuture, willCompleteSuccessfully());
            assertThat(handlerThreadFuture.join().getName(), containsString("MessagingService-inbound"));
        }
    }

    @Test
    void executorChooserChoosesHandlingThread() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor(NamedThreadFactory.create("test", "custom-pool", log));

        try (
                Services senderServices = createMessagingService(senderNode, senderNetworkConfig);
                Services receiverServices = createMessagingService(receiverNode, receiverNetworkConfig)
        ) {
            CompletableFuture<Thread> handlerThreadFuture = new CompletableFuture<>();

            receiverServices.messagingService.addMessageHandler(
                    TestMessageTypes.class,
                    message -> executor,
                    (message, sender, correlationId) -> handlerThreadFuture.complete(Thread.currentThread())
            );

            senderServices.messagingService.send(receiverNode, testMessage("test"));

            assertThat(handlerThreadFuture, willCompleteSuccessfully());
            assertThat(handlerThreadFuture.join().getName(), containsString("custom-pool"));
        } finally {
            IgniteUtils.shutdownAndAwaitTermination(executor, 10, SECONDS);
        }
    }

    @Test
    void multipleHandlersChooseExecutors() throws Exception {
        ExecutorService customExecutor = Executors.newSingleThreadExecutor(NamedThreadFactory.create("test", "custom-pool", log));

        try (
                Services senderServices = createMessagingService(senderNode, senderNetworkConfig);
                Services receiverServices = createMessagingService(receiverNode, receiverNetworkConfig)
        ) {
            CompletableFuture<Thread> handler1ThreadFuture = new CompletableFuture<>();
            CompletableFuture<Thread> handler2ThreadFuture = new CompletableFuture<>();
            CompletableFuture<Thread> handler3ThreadFuture = new CompletableFuture<>();

            receiverServices.messagingService.addMessageHandler(
                    TestMessageTypes.class,
                    (message, sender, correlationId) -> handler1ThreadFuture.complete(Thread.currentThread())
            );
            receiverServices.messagingService.addMessageHandler(
                    TestMessageTypes.class,
                    (message, sender, correlationId) -> {
                        if (!handler1ThreadFuture.isDone()) {
                            handler2ThreadFuture.completeExceptionally(new AssertionError("Second handler invoked before first handler"));
                        } else {
                            handler2ThreadFuture.complete(Thread.currentThread());
                        }
                    }
            );
            receiverServices.messagingService.addMessageHandler(
                    TestMessageTypes.class,
                    message -> customExecutor,
                    (message, sender, correlationId) -> handler3ThreadFuture.complete(Thread.currentThread())
            );

            senderServices.messagingService.send(receiverNode, testMessage("test"));

            assertThat(handler1ThreadFuture, willCompleteSuccessfully());
            assertThat(handler1ThreadFuture.join().getName(), containsString("MessagingService-inbound"));

            assertThat(handler2ThreadFuture, willCompleteSuccessfully());
            assertThat(handler2ThreadFuture.join(), is(sameInstance(handler1ThreadFuture.join())));

            assertThat(handler3ThreadFuture, willCompleteSuccessfully());
            assertThat(handler3ThreadFuture.join().getName(), containsString("custom-pool"));
        } finally {
            IgniteUtils.shutdownAndAwaitTermination(customExecutor, 10, SECONDS);
        }
    }

    @Test
    void invokeResponseIsProcessedInInboundThreadIfNoChooserSupplied() throws Exception {
        try (
                Services senderServices = createMessagingService(senderNode, senderNetworkConfig);
                Services receiverServices = createMessagingService(receiverNode, receiverNetworkConfig)
        ) {
            CompletableFuture<Thread> responseHandlingThreadFuture = new CompletableFuture<>();

            receiverServices.messagingService.addMessageHandler(
                    TestMessageTypes.class,
                    (message, sender, correlationId) -> {
                        if (correlationId != null) {
                            receiverServices.messagingService.respond(sender, message, correlationId);
                        }
                    }
            );

            senderServices.messagingService.invoke(receiverNode, testMessage("test"), 10_000)
                    .whenComplete((res, ex) -> responseHandlingThreadFuture.complete(Thread.currentThread()));

            assertThat(responseHandlingThreadFuture, willCompleteSuccessfully());
            assertThat(responseHandlingThreadFuture.join().getName(), containsString("MessagingService-inbound"));
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

    private Services createMessagingService(ClusterNode node, NetworkConfiguration networkConfig) {
        return createMessagingService(node, networkConfig, () -> {});
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
        StaleIdDetector staleIdDetector = new AllIdsAreFresh();

        ClassDescriptorRegistry classDescriptorRegistry = new ClassDescriptorRegistry();
        ClassDescriptorFactory classDescriptorFactory = new ClassDescriptorFactory(classDescriptorRegistry);
        UserObjectMarshaller marshaller = new DefaultUserObjectMarshaller(classDescriptorRegistry, classDescriptorFactory);

        DefaultMessagingService messagingService = new DefaultMessagingService(
                node.name(),
                networkMessagesFactory,
                topologyService,
                staleIdDetector,
                classDescriptorRegistry,
                marshaller,
                criticalWorkerRegistry
        );

        SerializationService serializationService = new SerializationService(
                registry,
                new UserObjectSerializationContext(classDescriptorRegistry, classDescriptorFactory, marshaller)
        );

        String eventLoopGroupNamePrefix = node.name() + "-event-loop";

        NettyBootstrapFactory bootstrapFactory = new NettyBootstrapFactory(networkConfig, eventLoopGroupNamePrefix);
        assertThat(bootstrapFactory.startAsync(ForkJoinPool.commonPool()), willCompleteSuccessfully());

        ConnectionManager connectionManager = new ConnectionManager(
                networkConfig.value(),
                serializationService,
                node.name(),
                bootstrapFactory,
                staleIdDetector,
                clientHandshakeManagerFactoryAdding(beforeHandshake, bootstrapFactory, staleIdDetector),
                failureProcessor
        );
        connectionManager.start();
        connectionManager.setLocalNode(node);

        messagingService.setConnectionManager(connectionManager);

        return new Services(connectionManager, messagingService, bootstrapFactory);
    }

    private RecoveryClientHandshakeManagerFactory clientHandshakeManagerFactoryAdding(
            Runnable beforeHandshake,
            NettyBootstrapFactory bootstrapFactory,
            StaleIdDetector staleIdDetector
    ) {
        return new RecoveryClientHandshakeManagerFactory() {
            @Override
            public RecoveryClientHandshakeManager create(
                    ClusterNode localNode,
                    short connectionId,
                    RecoveryDescriptorProvider recoveryDescriptorProvider
            ) {
                return new RecoveryClientHandshakeManager(
                        localNode,
                        connectionId,
                        recoveryDescriptorProvider,
                        bootstrapFactory,
                        staleIdDetector,
                        channel -> {},
                        () -> false,
                        failureProcessor
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
        private final NettyBootstrapFactory bootstrapFactory;

        private Services(
                ConnectionManager connectionManager,
                DefaultMessagingService messagingService,
                NettyBootstrapFactory bootstrapFactory
        ) {
            this.connectionManager = connectionManager;
            this.messagingService = messagingService;
            this.bootstrapFactory = bootstrapFactory;
        }

        @Override
        public void close() throws Exception {
            closeAll(
                    connectionManager::initiateStopping, connectionManager::stop,
                    messagingService::stop,
                    bootstrapFactory::beforeNodeStop,
                    () -> assertThat(bootstrapFactory.stopAsync(), willCompleteSuccessfully())
            );
        }
    }

    @FunctionalInterface
    private interface SendAction {
        void send(MessagingService service, TestMessage message, ClusterNode recipient);
    }

    @SuppressWarnings("NonSerializableFieldInSerializableClass")
    private enum SendOperation {
        WEAK_SEND((service, message, to) -> service.weakSend(to, message), ChannelType.DEFAULT),
        SEND_DEFAULT_CHANNEL((service, message, to) -> service.send(to, message), ChannelType.DEFAULT),
        SEND_SPECIFIC_CHANNEL((service, message, to) -> service.send(to, TEST_CHANNEL, message), TEST_CHANNEL),
        SEND_CONSISTENT_ID_SPECIFIC_CHANNEL((service, message, to) -> service.send(to.name(), TEST_CHANNEL, message), TEST_CHANNEL),
        INVOKE_DEFAULT_CHANNEL((service, message, to) -> service.invoke(to, message, 10_000), ChannelType.DEFAULT),
        INVOKE_CONSISTENT_ID_DEFAULT_CHANNEL((service, message, to) -> service.invoke(to.name(), message, 10_000), ChannelType.DEFAULT),
        INVOKE_SPECIFIC_CHANNEL((service, message, to) -> service.invoke(to, TEST_CHANNEL, message, 10_000), TEST_CHANNEL),
        INVOKE_CONSISTENT_ID_SPECIFIC_CHANNEL((service, message, to) -> service.invoke(to.name(), TEST_CHANNEL, message, 10_000),
                TEST_CHANNEL);

        private final SendAction sendAction;
        private final ChannelType expectedChannelType;

        SendOperation(SendAction sendAction, ChannelType expectedChannelType) {
            this.sendAction = sendAction;
            this.expectedChannelType = expectedChannelType;
        }
    }

    @FunctionalInterface
    private interface RespondAction {
        void respond(MessagingService service, NetworkMessage message, ClusterNode recipient, long correlationId);
    }

    @SuppressWarnings("NonSerializableFieldInSerializableClass")
    private enum RespondOperation {
        RESPOND_DEFAULT_CHANNEL((service, message, to, corrId) -> service.respond(to, message, corrId), ChannelType.DEFAULT),
        RESPOND_CONSISTENT_ID_DEFAULT_CHANNEL((service, message, to, corrId) -> service.respond(to.name(), message, corrId),
                ChannelType.DEFAULT),
        RESPOND_SPECIFIC_CHANNEL((service, message, to, corrId) -> service.respond(to, TEST_CHANNEL, message, corrId), TEST_CHANNEL),
        RESPOND_CONSISTENT_ID_SPECIFIC_CHANNEL((service, message, to, corrId) -> service.respond(to.name(), TEST_CHANNEL, message, corrId),
                TEST_CHANNEL);

        private final RespondAction respondAction;
        private final ChannelType expectedChannelType;

        RespondOperation(RespondAction respondAction, ChannelType expectedChannelType) {
            this.respondAction = respondAction;
            this.expectedChannelType = expectedChannelType;
        }
    }
}
