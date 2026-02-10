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

import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.defaultChannelTypeRegistry;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.defaultSerializationRegistry;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.asserts.CompletableFutureAssert.assertWillThrow;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.manager.ComponentContext;
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
import org.apache.ignite.internal.network.recovery.RecoveryInitiatorHandshakeManager;
import org.apache.ignite.internal.network.recovery.StaleIdDetector;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.network.serialization.SerializationService;
import org.apache.ignite.internal.network.serialization.UserObjectSerializationContext;
import org.apache.ignite.internal.network.serialization.marshal.DefaultUserObjectMarshaller;
import org.apache.ignite.internal.network.serialization.marshal.UserObjectMarshaller;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.version.DefaultIgniteProductVersionSource;
import org.apache.ignite.internal.worker.CriticalWorkerRegistry;
import org.apache.ignite.network.NetworkAddress;
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

    static final ChannelType TEST_CHANNEL = new ChannelType(Short.MAX_VALUE, "Test");

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

    private final ChannelTypeRegistry channelTypeRegistry = defaultChannelTypeRegistry();

    private final InternalClusterNode senderNode = new ClusterNodeImpl(
            randomUUID(),
            "sender",
            new NetworkAddress("localhost", SENDER_PORT)
    );

    private final InternalClusterNode receiverNode = new ClusterNodeImpl(
            randomUUID(),
            "receiver",
            new NetworkAddress("localhost", RECEIVER_PORT)
    );

    private final UUID clusterId = randomUUID();

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
        ExecutorService executor = Executors.newSingleThreadExecutor(IgniteThreadFactory.create("test", "custom-pool", log));

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
        ExecutorService customExecutor = Executors.newSingleThreadExecutor(IgniteThreadFactory.create("test", "custom-pool", log));

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

    @Test
    void invokeTimesOut() throws Exception {
        try (
                Services senderServices = createMessagingService(senderNode, senderNetworkConfig);
                Services ignoredReceiverServices = createMessagingService(receiverNode, receiverNetworkConfig)
        ) {
            // There is no message handler, so invocations will time out.
            CompletableFuture<NetworkMessage> future = senderServices.messagingService.invoke(receiverNode, testMessage("test"), 1);
            TimeoutException ex = assertWillThrow(future, TimeoutException.class);
            assertThat(ex.getMessage(), is("Invocation timed out [message=org.apache.ignite.internal.network.messages.TestMessageImpl]"));
        }
    }

    @ParameterizedTest
    @EnumSource(ClusterNodeChanger.class)
    void testResolveRecipientAddressToSelf(ClusterNodeChanger clusterNodeChanger) throws Exception {
        InternalClusterNode node = senderNode;
        NetworkConfiguration senderNetworkConfig = this.senderNetworkConfig;

        try (Services services = createMessagingService(node, senderNetworkConfig)) {
            InternalClusterNode nodeToCheck = clusterNodeChanger.changer.apply(node, services);
            InternalClusterNode nodeToCheckWithoutName = copyWithoutName(nodeToCheck);

            DefaultMessagingService messagingService = services.messagingService;

            assertNull(messagingService.resolveRecipientAddress(nodeToCheck));
            assertNull(messagingService.resolveRecipientAddress(nodeToCheckWithoutName));
        }
    }

    @Test
    void testResolveRecipientAddressToOther() throws Exception {
        try (Services services = createMessagingService(senderNode, senderNetworkConfig)) {
            InternalClusterNode nodeToCheck = receiverNode;
            InternalClusterNode nodeToCheckWithoutName = copyWithoutName(nodeToCheck);

            DefaultMessagingService messagingService = services.messagingService;

            assertNotNull(messagingService.resolveRecipientAddress(nodeToCheck));
            assertNotNull(messagingService.resolveRecipientAddress(nodeToCheckWithoutName));
        }
    }

    @ParameterizedTest
    @EnumSource(SendByClusterNodeOperation.class)
    void sendByClusterNodeFailsIfActualNodeIdIsDifferent(SendByClusterNodeOperation operation) throws Exception {
        try (
                Services senderServices = createMessagingService(senderNode, senderNetworkConfig);
                Services unused = createMessagingService(receiverNode, receiverNetworkConfig)
        ) {
            InternalClusterNode receiverWithAnotherId = copyWithDifferentId();

            assertThat(
                    operation.sendAction.send(senderServices.messagingService, testMessage("test"), receiverWithAnotherId),
                    willThrow(RecipientLeftException.class)
            );
        }
    }

    private ClusterNodeImpl copyWithDifferentId() {
        return new ClusterNodeImpl(
                randomUUID(),
                receiverNode.name(),
                receiverNode.address(),
                receiverNode.nodeMetadata()
        );
    }

    @ParameterizedTest
    @EnumSource(SendByConsistentCoordinateOperation.class)
    void sendByConsistentCoordinateSucceedsIfActualNodeIdIsDifferent(SendByConsistentCoordinateOperation operation) throws Exception {
        InternalClusterNode receiverWithAnotherId = copyWithDifferentId();

        lenient().when(topologyService.getByConsistentId(receiverWithAnotherId.name())).thenReturn(receiverWithAnotherId);
        lenient().when(topologyService.getByAddress(receiverWithAnotherId.address())).thenReturn(receiverWithAnotherId);

        try (
                Services senderServices = createMessagingService(senderNode, senderNetworkConfig);
                Services receiverServices = createMessagingService(receiverNode, receiverNetworkConfig)
        ) {
            CompletableFuture<Void> messageDelivered = new CompletableFuture<>();

            receiverServices.messagingService.addMessageHandler(
                    TestMessageTypes.class,
                    (message, sender, correlationId) -> {
                        if (message instanceof TestMessage) {
                            messageDelivered.complete(null);

                            if (correlationId != null) {
                                receiverServices.messagingService.respond(sender, message, correlationId);
                            }
                        }
                    }
            );

            assertThat(
                    operation.sendAction.send(senderServices.messagingService, testMessage("test"), receiverWithAnotherId),
                    willCompleteSuccessfully()
            );
            if (operation.notRespondOperation()) {
                assertThat(messageDelivered, willCompleteSuccessfully());
            }
        }
    }

    private static InternalClusterNode copyWithoutName(InternalClusterNode node) {
        return new ClusterNodeImpl(node.id(), null, node.address());
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

    private Services createMessagingService(InternalClusterNode node, NetworkConfiguration networkConfig) {
        return createMessagingService(node, networkConfig, () -> {});
    }

    private Services createMessagingService(InternalClusterNode node, NetworkConfiguration networkConfig, Runnable beforeHandshake) {
        return createMessagingService(node, networkConfig, beforeHandshake, messageSerializationRegistry);
    }

    private Services createMessagingService(
            InternalClusterNode node,
            NetworkConfiguration networkConfig,
            Runnable beforeHandshake,
            MessageSerializationRegistry registry
    ) {
        StaleIdDetector staleIdDetector = new AllIdsAreFresh();
        ClusterIdSupplier clusterIdSupplier = new ConstantClusterIdSupplier(clusterId);

        ClassDescriptorRegistry classDescriptorRegistry = new ClassDescriptorRegistry();
        ClassDescriptorFactory classDescriptorFactory = new ClassDescriptorFactory(classDescriptorRegistry);
        UserObjectMarshaller marshaller = new DefaultUserObjectMarshaller(classDescriptorRegistry, classDescriptorFactory);

        SerializationService serializationService = new SerializationService(
                registry,
                new UserObjectSerializationContext(classDescriptorRegistry, classDescriptorFactory, marshaller)
        );

        String eventLoopGroupNamePrefix = node.name() + "-event-loop";

        NettyBootstrapFactory bootstrapFactory = new NettyBootstrapFactory(networkConfig, eventLoopGroupNamePrefix);

        assertThat(bootstrapFactory.startAsync(new ComponentContext()), willCompleteSuccessfully());

        ConnectionManager connectionManager = new TestConnectionManager(
                networkConfig,
                serializationService,
                node,
                bootstrapFactory,
                staleIdDetector,
                clusterIdSupplier,
                beforeHandshake
        );

        DefaultMessagingService messagingService = new DefaultMessagingService(
                node.name(),
                networkMessagesFactory,
                topologyService,
                staleIdDetector,
                classDescriptorRegistry,
                marshaller,
                criticalWorkerRegistry,
                failureProcessor,
                connectionManager,
                channelTypeRegistry
        );

        connectionManager.start();

        messagingService.start();

        return new Services(connectionManager, messagingService, bootstrapFactory);
    }

    private class TestConnectionManager extends ConnectionManager {
        private final Runnable beforeHandshake;

        private TestConnectionManager(
                NetworkConfiguration networkConfig,
                SerializationService serializationService,
                InternalClusterNode node,
                NettyBootstrapFactory bootstrapFactory,
                StaleIdDetector staleIdDetector,
                ClusterIdSupplier clusterIdSupplier,
                Runnable beforeHandshake
        ) {
            super(
                    networkConfig.value(),
                    serializationService,
                    new InetSocketAddress(node.address().host(), node.address().port()),
                    node,
                    bootstrapFactory,
                    staleIdDetector,
                    clusterIdSupplier,
                    DefaultMessagingServiceTest.this.channelTypeRegistry,
                    new DefaultIgniteProductVersionSource(),
                    DefaultMessagingServiceTest.this.topologyService,
                    DefaultMessagingServiceTest.this.failureProcessor
            );

            this.beforeHandshake = beforeHandshake;
        }

        @Override
        protected RecoveryInitiatorHandshakeManager newRecoveryInitiatorHandshakeManager(
                short connectionId,
                InternalClusterNode localNode
        ) {
            return new RecoveryInitiatorHandshakeManager(
                    localNode,
                    connectionId,
                    descriptorProvider,
                    bootstrapFactory.handshakeEventLoopSwitcher(),
                    staleIdDetector,
                    clusterIdSupplier,
                    channel -> {},
                    () -> false,
                    new DefaultIgniteProductVersionSource(),
                    this.topologyService,
                    new NoOpFailureManager()
            ) {
                @Override
                protected void finishHandshake() {
                    beforeHandshake.run();

                    super.finishHandshake();
                }
            };
        }
    }

    private static String localHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
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
                    () -> assertThat(bootstrapFactory.stopAsync(new ComponentContext()), willCompleteSuccessfully())
            );
        }
    }

    @FunctionalInterface
    private interface SendAction {
        void send(MessagingService service, TestMessage message, InternalClusterNode recipient);
    }

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
        void respond(MessagingService service, NetworkMessage message, InternalClusterNode recipient, long correlationId);
    }

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

    private enum ClusterNodeChanger {
        NOT_CHANGE((node, services) -> node),
        CHANGE_ID_ONLY((node, services) -> new ClusterNodeImpl(randomUUID(), node.name(), node.address())),
        CHANGE_NAME_ONLY((node, services) -> new ClusterNodeImpl(node.id(), node.name() + "_", node.address())),
        CHANGE_NAME((node, services) -> new ClusterNodeImpl(randomUUID(), node.name() + "_", node.address())),
        SET_IPV4_LOOPBACK((node, services) -> new ClusterNodeImpl(
                randomUUID(),
                node.name(),
                new NetworkAddress("127.0.0.1", node.address().port())
        )),
        SET_IPV6_LOOPBACK((node, services) -> new ClusterNodeImpl(
                randomUUID(),
                node.name(),
                new NetworkAddress("::1", node.address().port())
        )),
        SET_IPV4_ANYLOCAL((node, services) -> new ClusterNodeImpl(
                randomUUID(),
                node.name(),
                new NetworkAddress("0.0.0.0", node.address().port())
        )),
        SET_IPV6_ANYLOCAL((node, services) -> new ClusterNodeImpl(
                randomUUID(),
                node.name(),
                new NetworkAddress("0:0:0:0:0:0:0:0", node.address().port())
        )),
        SET_LOCALHOST((node, services) -> new ClusterNodeImpl(
                randomUUID(),
                node.name(),
                new NetworkAddress(localHostName(), node.address().port())
        ));

        private final BiFunction<InternalClusterNode, Services, InternalClusterNode> changer;

        ClusterNodeChanger(BiFunction<InternalClusterNode, Services, InternalClusterNode> changer) {
            this.changer = changer;
        }
    }

    @FunctionalInterface
    private interface AsyncSendAction {
        CompletableFuture<?> send(MessagingService service, TestMessage message, InternalClusterNode recipient);
    }

    private enum SendByClusterNodeOperation {
        SEND_DEFAULT_CHANNEL((service, message, to) -> service.send(to, message)),
        SEND_SPECIFIC_CHANNEL((service, message, to) -> service.send(to, TEST_CHANNEL, message)),
        RESPOND_DEFAULT_CHANNEL((service, message, to) -> service.respond(to, message, 123L)),
        RESPOND_SPECIFIC_CHANNEL((service, message, to) -> service.respond(to, TEST_CHANNEL, message, 123L)),
        INVOKE_DEFAULT_CHANNEL((service, message, to) -> service.invoke(to, message, 10_000)),
        INVOKE_SPECIFIC_CHANNEL((service, message, to) -> service.invoke(to, TEST_CHANNEL, message, 10_000));

        private final AsyncSendAction sendAction;

        SendByClusterNodeOperation(AsyncSendAction sendAction) {
            this.sendAction = sendAction;
        }
    }

    private enum SendByConsistentCoordinateOperation {
        SEND_BY_CONSISTENT_ID((service, message, to) -> service.send(to.name(), ChannelType.DEFAULT, message)),
        SEND_BY_ADDRESS((service, message, to) -> service.send(to.address(), ChannelType.DEFAULT, message)),
        RESPOND_BY_CONSISTENT_ID_DEFAULT_CHANNEL((service, message, to) -> service.respond(to.name(), message, 123L)),
        RESPOND_BY_CONSISTENT_ID_SPECIFIC_CHANNEL((service, message, to) -> service.respond(to.name(), ChannelType.DEFAULT, message, 123L)),
        INVOKE_BY_CONSISTENT_ID_DEFAULT_CHANNEL((service, message, to) -> service.invoke(to.name(), message, 10000)),
        INVOKE_BY_CONSISTENT_ID_SPECIFIC_CHANNEL((service, message, to) -> service.invoke(to.name(), ChannelType.DEFAULT, message, 10000));

        private final AsyncSendAction sendAction;

        SendByConsistentCoordinateOperation(AsyncSendAction sendAction) {
            this.sendAction = sendAction;
        }

        private boolean notRespondOperation() {
            return this != RESPOND_BY_CONSISTENT_ID_DEFAULT_CHANNEL && this != RESPOND_BY_CONSISTENT_ID_SPECIFIC_CHANNEL;
        }
    }
}
