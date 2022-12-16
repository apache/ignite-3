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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.network.serialization.PerSessionSerializationService.createClassDescriptorsMessages;
import static org.apache.ignite.network.NettyBootstrapFactory.isInNetworkThread;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.Function;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.message.ClassDescriptorMessage;
import org.apache.ignite.internal.network.message.InvokeRequest;
import org.apache.ignite.internal.network.message.InvokeResponse;
import org.apache.ignite.internal.network.message.ScaleCubeMessage;
import org.apache.ignite.internal.network.netty.ConnectionManager;
import org.apache.ignite.internal.network.netty.InNetworkObject;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.DescriptorRegistry;
import org.apache.ignite.internal.network.serialization.marshal.UserObjectMarshaller;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/** Default messaging service implementation. */
public class DefaultMessagingService extends AbstractMessagingService {
    private static final IgniteLogger LOG = Loggers.forClass(DefaultMessagingService.class);

    /** Network messages factory. */
    private final NetworkMessagesFactory factory;

    /** Topology service. */
    private final TopologyService topologyService;

    /** User object marshaller. */
    private final UserObjectMarshaller marshaller;

    /** Class descriptor registry. */
    private final ClassDescriptorRegistry classDescriptorRegistry;

    /** Connection manager that provides access to {@link NettySender}. */
    private volatile ConnectionManager connectionManager;

    /** Collection that maps correlation id to the future for an invocation request. */
    private final ConcurrentMap<Long, CompletableFuture<NetworkMessage>> requestsMap = new ConcurrentHashMap<>();

    /** Correlation id generator. */
    private final AtomicLong correlationIdGenerator = new AtomicLong();

    /** Executor for outbound messages. */
    private final ExecutorService outboundExecutor = Executors.newSingleThreadExecutor(
            new NamedThreadFactory("MessagingService-outbound-", LOG)
    );

    /** Executor for inbound messages. */
    private final ExecutorService inboundExecutor = Executors.newSingleThreadExecutor(
            new NamedThreadFactory("MessagingService-inbound-", LOG)
    );

    @Nullable
    private volatile BiPredicate<String, NetworkMessage> dropMessagePredicate;

    /**
     * Constructor.
     *
     * @param factory Network messages factory.
     * @param topologyService Topology service.
     * @param classDescriptorRegistry Descriptor registry.
     * @param marshaller Marshaller.
     */
    public DefaultMessagingService(NetworkMessagesFactory factory, TopologyService topologyService,
            ClassDescriptorRegistry classDescriptorRegistry, UserObjectMarshaller marshaller) {
        this.factory = factory;
        this.topologyService = topologyService;
        this.classDescriptorRegistry = classDescriptorRegistry;
        this.marshaller = marshaller;
    }

    /**
     * Resolves cyclic dependency and sets up the connection manager.
     *
     * @param connectionManager Connection manager.
     */
    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        connectionManager.addListener(this::onMessage);
    }

    @Override
    public void weakSend(ClusterNode recipient, NetworkMessage msg) {
        send(recipient, msg);
    }

    @Override
    public CompletableFuture<Void> send(ClusterNode recipient, NetworkMessage msg) {
        return send0(recipient, msg, null);
    }

    @Override
    public CompletableFuture<Void> respond(ClusterNode recipient, NetworkMessage msg, long correlationId) {
        return send0(recipient, msg, correlationId);
    }

    @Override
    public CompletableFuture<Void> respond(String recipientConsistentId, NetworkMessage msg, long correlationId) {
        ClusterNode recipient = topologyService.getByConsistentId(recipientConsistentId);

        if (recipient == null) {
            return failedFuture(
                    new UnresolvableConsistentIdException("Recipient consistent ID cannot be resolved: " + recipientConsistentId)
            );
        }

        return respond(recipient, msg, correlationId);
    }

    @Override
    public CompletableFuture<NetworkMessage> invoke(ClusterNode recipient, NetworkMessage msg, long timeout) {
        return invoke0(recipient, msg, timeout);
    }

    /**
     * Sends a message. If the target is the current node, then message will be delivered immediately.
     *
     * @param recipient Target cluster node.
     * @param msg Message.
     * @param correlationId Correlation id. Not null iff the message is a response to a {@link #invoke} request.
     * @return Future of the send operation.
     */
    private CompletableFuture<Void> send0(ClusterNode recipient, NetworkMessage msg, @Nullable Long correlationId) {
        if (connectionManager.isStopped()) {
            return failedFuture(new NodeStoppingException());
        }

        BiPredicate<String, NetworkMessage> dropMessage = dropMessagePredicate;
        if (dropMessage != null && dropMessage.test(recipient.name(), msg)) {
            return new CompletableFuture<>();
        }

        InetSocketAddress recipientAddress = new InetSocketAddress(recipient.address().host(), recipient.address().port());

        if (isSelf(recipient.name(), recipientAddress)) {
            if (correlationId != null) {
                onInvokeResponse(msg, correlationId);
            } else {
                sendToSelf(msg, null);
            }

            return CompletableFuture.completedFuture(null);
        }

        NetworkMessage message = correlationId != null ? responseFromMessage(msg, correlationId) : msg;

        return sendMessage0(recipient.name(), recipientAddress, message);
    }

    /**
     * Sends an invocation request. If the target is the current node, then message will be delivered immediately.
     *
     * @param recipient Target cluster node.
     * @param msg Message.
     * @param timeout Invocation timeout.
     * @return A future holding the response or error if the expected response was not received.
     */
    private CompletableFuture<NetworkMessage> invoke0(ClusterNode recipient, NetworkMessage msg, long timeout) {
        if (connectionManager.isStopped()) {
            return failedFuture(new NodeStoppingException());
        }

        BiPredicate<String, NetworkMessage> dropMessage = dropMessagePredicate;
        if (dropMessage != null && dropMessage.test(recipient.name(), msg)) {
            return new CompletableFuture<NetworkMessage>().orTimeout(10, TimeUnit.MILLISECONDS);
        }

        long correlationId = createCorrelationId();

        CompletableFuture<NetworkMessage> responseFuture = new CompletableFuture<NetworkMessage>()
                .orTimeout(timeout, TimeUnit.MILLISECONDS);

        requestsMap.put(correlationId, responseFuture);

        InetSocketAddress recipientAddress = new InetSocketAddress(recipient.address().host(), recipient.address().port());

        if (isSelf(recipient.name(), recipientAddress)) {
            sendToSelf(msg, correlationId);

            return responseFuture;
        }

        InvokeRequest message = requestFromMessage(msg, correlationId);

        return sendMessage0(recipient.name(), recipientAddress, message).thenCompose(unused -> responseFuture);
    }

    /**
     * Sends network object.
     *
     * @param consistentId Target consistent ID. Can be {@code null} if the node has not been added to the topology.
     * @param addr Target address.
     * @param message Message.
     *
     * @return Future of the send operation.
     */
    private CompletableFuture<Void> sendMessage0(@Nullable String consistentId, InetSocketAddress addr, NetworkMessage message) {
        if (isInNetworkThread()) {
            return CompletableFuture.supplyAsync(() -> sendMessage0(consistentId, addr, message), outboundExecutor)
                    .thenCompose(Function.identity());
        }

        List<ClassDescriptorMessage> descriptors;

        try {
            descriptors = beforeRead(message);
        } catch (Exception e) {
            return failedFuture(new IgniteException("Failed to marshal message: " + e.getMessage(), e));
        }

        return connectionManager.channel(consistentId, addr)
                .thenComposeToCompletable(sender -> sender.send(new OutNetworkObject(message, descriptors)));
    }

    private List<ClassDescriptorMessage> beforeRead(NetworkMessage msg) throws Exception {
        IntSet ids = new IntOpenHashSet();

        msg.prepareMarshal(ids, marshaller);

        return createClassDescriptorsMessages(ids, classDescriptorRegistry);
    }

    /**
     * Sends a message to the current node.
     *
     * @param msg Message.
     * @param correlationId Correlation id.
     */
    private void sendToSelf(NetworkMessage msg, @Nullable Long correlationId) {
        for (NetworkMessageHandler networkMessageHandler : getMessageHandlers(msg.groupType())) {
            networkMessageHandler.onReceived(msg, topologyService.localMember().name(), correlationId);
        }
    }

    /**
     * Handles an incoming message.
     *
     * @param obj Incoming message wrapper.
     */
    private void onMessage(InNetworkObject obj) {
        if (isInNetworkThread()) {
            inboundExecutor.execute(() -> {
                try {
                    onMessage(obj);
                } catch (RuntimeException e) {
                    LOG.warn("onMessage() failed while processing " + obj.message() + " from " + obj.consistentId(), e);
                }
            });

            return;
        }

        NetworkMessage msg = obj.message();
        DescriptorRegistry registry = obj.registry();
        try {
            msg.unmarshal(marshaller, registry);
        } catch (Exception e) {
            throw new IgniteException("Failed to unmarshal message: " + e.getMessage(), e);
        }
        if (msg instanceof InvokeResponse) {
            InvokeResponse response = (InvokeResponse) msg;
            onInvokeResponse(response.message(), response.correlationId());
            return;
        }

        Long correlationId = null;
        NetworkMessage message = msg;

        if (msg instanceof InvokeRequest) {
            // Unwrap invocation request
            InvokeRequest messageWithCorrelation = (InvokeRequest) msg;
            correlationId = messageWithCorrelation.correlationId();
            message = messageWithCorrelation.message();
        }

        String senderConsistentId = obj.consistentId();

        // Unfortunately, since the Messaging Service is used by ScaleCube itself, some messages can be sent
        // before the node is added to the topology. ScaleCubeMessage handler guarantees to handle null sender consistent ID
        // without throwing an exception.
        assert message instanceof ScaleCubeMessage || senderConsistentId != null;

        for (NetworkMessageHandler networkMessageHandler : getMessageHandlers(message.groupType())) {
            networkMessageHandler.onReceived(message, senderConsistentId, correlationId);
        }
    }

    /**
     * Handles a response to an invocation request.
     *
     * @param response Response message.
     * @param correlationId Request's correlation id.
     */
    private void onInvokeResponse(NetworkMessage response, Long correlationId) {
        CompletableFuture<NetworkMessage> responseFuture = requestsMap.remove(correlationId);
        if (responseFuture != null) {
            responseFuture.complete(response);
        }
    }

    /**
     * Creates an {@link InvokeRequest} from a message and a correlation id.
     *
     * @param message Message.
     * @param correlationId Correlation id.
     * @return Invoke request message.
     */
    private InvokeRequest requestFromMessage(NetworkMessage message, long correlationId) {
        return factory.invokeRequest().correlationId(correlationId).message(message).build();
    }

    /**
     * Creates an {@link InvokeResponse} from a message and a correlation id.
     *
     * @param message Message.
     * @param correlationId Correlation id.
     * @return Invoke response message.
     */
    private InvokeResponse responseFromMessage(NetworkMessage message, long correlationId) {
        return factory.invokeResponse().correlationId(correlationId).message(message).build();
    }

    /**
     * Creates a correlation id for an invocation request.
     *
     * @return New correlation id.
     */
    private long createCorrelationId() {
        return correlationIdGenerator.getAndIncrement();
    }

    /**
     * Checks if the target is the current node.
     *
     * @param consistentId Target consistent ID. Can be {@code null} if the node has not been added to the topology.
     * @param targetAddress Target address.
     * @return {@code true} if the target is the current node, {@code false} otherwise.
     */
    private boolean isSelf(@Nullable String consistentId, InetSocketAddress targetAddress) {
        if (consistentId != null) {
            return connectionManager.consistentId().equals(consistentId);
        }

        InetSocketAddress localAddress = connectionManager.localAddress();

        if (Objects.equals(localAddress, targetAddress)) {
            return true;
        }

        InetAddress targetInetAddress = targetAddress.getAddress();

        if (targetInetAddress.isAnyLocalAddress() || targetInetAddress.isLoopbackAddress()) {
            return targetAddress.getPort() == localAddress.getPort();
        }

        return false;
    }

    /**
     * Stops the messaging service.
     */
    public void stop() {
        var exception = new NodeStoppingException();

        requestsMap.values().forEach(fut -> fut.completeExceptionally(exception));

        requestsMap.clear();

        IgniteUtils.shutdownAndAwaitTermination(inboundExecutor, 10, TimeUnit.SECONDS);
        IgniteUtils.shutdownAndAwaitTermination(outboundExecutor, 10, TimeUnit.SECONDS);
    }

    /**
     * Installs a predicate, it will be consulted with for each message being sent; when it returns {@code true}, the
     * message will be silently dropped (it will not be sent, the corresponding future will never complete).
     *
     * @param predicate Predicate that will decide whether a message should be dropped. Its first argument is the recipient
     *     node's consistent ID.
     */
    @TestOnly
    public void dropMessages(BiPredicate<String, NetworkMessage> predicate) {
        dropMessagePredicate = predicate;
    }

    /**
     * Stops dropping messages.
     *
     * @see #dropMessages(BiPredicate)
     */
    @TestOnly
    public void stopDroppingMessages() {
        dropMessagePredicate = null;
    }
}
