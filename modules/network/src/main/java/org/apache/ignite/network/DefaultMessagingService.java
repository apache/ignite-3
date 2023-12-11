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
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.network.NettyBootstrapFactory.isInNetworkThread;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.Function;
import org.apache.ignite.internal.lang.NodeStoppingException;
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
    private final ExecutorService outboundExecutor;

    /** Executor for inbound messages. */
    private final ExecutorService inboundExecutor;

    // TODO: IGNITE-18493 - remove/move this
    @Nullable
    private volatile BiPredicate<String, NetworkMessage> dropMessagesPredicate;

    private static final ConcurrentMap<ChannelKey, AtomicInteger> sent = new ConcurrentHashMap<>();
    private static final ConcurrentMap<ChannelKey, AtomicInteger> received = new ConcurrentHashMap<>();
    private static final ConcurrentMap<ChannelKey, AtomicInteger> inFlight = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, Long> sendNanoTimes = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, AtomicLong> lastReceiveStarts = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicLong> lastSendStarts = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicLong> lastSendEnds = new ConcurrentHashMap<>();

    private AtomicInteger sent(String fromConsistentId, String toConsistentId) {
        return sent.computeIfAbsent(new ChannelKey(fromConsistentId, toConsistentId), key -> new AtomicInteger());
    }

    private AtomicInteger received(String fromConsistentId, String toConsistentId) {
        return received.computeIfAbsent(new ChannelKey(fromConsistentId, toConsistentId), key -> new AtomicInteger());
    }

    private AtomicInteger inFlight(String fromConsistentId, String toConsistentId) {
        return inFlight.computeIfAbsent(new ChannelKey(fromConsistentId, toConsistentId), key -> new AtomicInteger());
    }

    private AtomicLong lastReceiveStart(String consistentId) {
        return lastReceiveStarts.computeIfAbsent(consistentId, k -> new AtomicLong(-1));
    }

    private AtomicLong lastSendStart(String consistentId) {
        return lastSendStarts.computeIfAbsent(consistentId, k -> new AtomicLong(-1));
    }

    private AtomicLong lastSendEnd(String consistentId) {
        return lastSendEnds.computeIfAbsent(consistentId, k -> new AtomicLong(-1));
    }

    /**
     * Constructor.
     *
     * @param factory Network messages factory.
     * @param topologyService Topology service.
     * @param classDescriptorRegistry Descriptor registry.
     * @param marshaller Marshaller.
     */
    public DefaultMessagingService(
            String nodeName,
            NetworkMessagesFactory factory,
            TopologyService topologyService,
            ClassDescriptorRegistry classDescriptorRegistry,
            UserObjectMarshaller marshaller
    ) {
        this.factory = factory;
        this.topologyService = topologyService;
        this.classDescriptorRegistry = classDescriptorRegistry;
        this.marshaller = marshaller;

        this.outboundExecutor = Executors.newSingleThreadExecutor(NamedThreadFactory.create(nodeName, "MessagingService-outbound-", LOG));
        // TODO asch the implementation of delayed acks relies on absence of reordering on subsequent messages delivery.
        // TODO asch This invariant should be preserved while working on IGNITE-20373
        this.inboundExecutor = Executors.newSingleThreadExecutor(NamedThreadFactory.create(nodeName, "MessagingService-inbound-", LOG));
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
    public CompletableFuture<Void> send(ClusterNode recipient, ChannelType channelType, NetworkMessage msg) {
        return send0(recipient, channelType, msg, null);
    }

    @Override
    public CompletableFuture<Void> send(String recipientConsistentId, ChannelType channelType, NetworkMessage msg) {
        ClusterNode recipient = topologyService.getByConsistentId(recipientConsistentId);

        if (recipient == null) {
            return failedFuture(
                    new UnresolvableConsistentIdException("Recipient consistent ID cannot be resolved: " + recipientConsistentId)
            );
        }

        return send0(recipient, channelType, msg, null);
    }

    @Override
    public CompletableFuture<Void> respond(ClusterNode recipient, ChannelType type, NetworkMessage msg, long correlationId) {
        return send0(recipient, type, msg, correlationId);
    }

    @Override
    public CompletableFuture<Void> respond(String recipientConsistentId, ChannelType type, NetworkMessage msg, long correlationId) {
        ClusterNode recipient = topologyService.getByConsistentId(recipientConsistentId);

        if (recipient == null) {
            return failedFuture(
                    new UnresolvableConsistentIdException("Recipient consistent ID cannot be resolved: " + recipientConsistentId)
            );
        }

        return respond(recipient, msg, correlationId);
    }

    @Override
    public CompletableFuture<NetworkMessage> invoke(ClusterNode recipient, ChannelType type, NetworkMessage msg, long timeout) {
        return invoke0(recipient, type, msg, timeout);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NetworkMessage> invoke(String recipientConsistentId, ChannelType type, NetworkMessage msg, long timeout) {
        ClusterNode recipient = topologyService.getByConsistentId(recipientConsistentId);

        if (recipient == null) {
            return failedFuture(
                    new UnresolvableConsistentIdException("Recipient consistent ID cannot be resolved: " + recipientConsistentId)
            );
        }

        return invoke0(recipient, type, msg, timeout);
    }

    /**
     * Sends a message. If the target is the current node, then message will be delivered immediately.
     *
     * @param recipient Target cluster node.
     * @param msg Message.
     * @param correlationId Correlation id. Not null iff the message is a response to a {@link #invoke} request.
     * @return Future of the send operation.
     */
    private CompletableFuture<Void> send0(ClusterNode recipient, ChannelType type, NetworkMessage msg, @Nullable Long correlationId) {
        if (connectionManager.isStopped()) {
            return failedFuture(new NodeStoppingException());
        }

        // TODO: IGNITE-18493 - remove/move this
        if (shouldDropMessage(recipient, msg)) {
            return nullCompletedFuture();
        }

        InetSocketAddress recipientAddress = new InetSocketAddress(recipient.address().host(), recipient.address().port());

        if (isSelf(recipient.name(), recipientAddress)) {
            if (correlationId != null) {
                onInvokeResponse(msg, correlationId);
            } else {
                sendToSelf(msg, null);
            }

            return nullCompletedFuture();
        }

        NetworkMessage message = correlationId != null ? responseFromMessage(msg, correlationId) : msg;

        return sendMessage0(recipient.name(), type, recipientAddress, message);
    }

    private boolean shouldDropMessage(ClusterNode recipient, NetworkMessage msg) {
        BiPredicate<String, NetworkMessage> predicate = dropMessagesPredicate;

        return predicate != null && predicate.test(recipient.name(), msg);
    }

    /**
     * Sends an invocation request. If the target is the current node, then message will be delivered immediately.
     *
     * @param recipient Target cluster node.
     * @param msg Message.
     * @param timeout Invocation timeout.
     * @return A future holding the response or error if the expected response was not received.
     */
    private CompletableFuture<NetworkMessage> invoke0(ClusterNode recipient, ChannelType type, NetworkMessage msg, long timeout) {
        if (connectionManager.isStopped()) {
            return failedFuture(new NodeStoppingException());
        }

        // TODO: IGNITE-18493 - remove/move this
        if (shouldDropMessage(recipient, msg)) {
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

        return sendMessage0(recipient.name(), type, recipientAddress, message).thenCompose(unused -> responseFuture);
    }

    /**
     * Sends network object.
     *
     * @param consistentId Target consistent ID. Can be {@code null} if the node has not been added to the topology.
     * @param type Channel type for send.
     * @param addr Target address.
     * @param message Message.
     *
     * @return Future of the send operation.
     */
    private CompletableFuture<Void> sendMessage0(
            @Nullable String consistentId,
            ChannelType type,
            InetSocketAddress addr,
            NetworkMessage message
    ) {
        if (isInNetworkThread()) {
            return CompletableFuture.supplyAsync(() -> sendMessage0(consistentId, type, addr, message), outboundExecutor)
                    .thenCompose(Function.identity());
        }

        if (message.messageId() == null) {
            message.messageId(UUID.randomUUID().toString());
        }

        sent(topologyService.localMember().name(), consistentId).incrementAndGet();
        inFlight(topologyService.localMember().name(), consistentId).incrementAndGet();
        //sentMessages.put(message.messageId(), message);
        long beforeSendingNanos = System.nanoTime();
        sendNanoTimes.put(message.messageId(), beforeSendingNanos);

        long beforeSentNanos = System.nanoTime();
        long lastSendStartNanos = lastSendStart(topologyService.localMember().name()).getAndSet(beforeSentNanos);

        if (lastSendStartNanos > 0) {
            long nanosSinceLastSendStart = beforeSentNanos - lastSendStartNanos;
            long millisSinceLastSendStart = TimeUnit.NANOSECONDS.toMillis(nanosSinceLastSendStart);

            if (millisSinceLastSendStart > 1000) {
                LOG.info("DDD {} ms since last send start, message is {}", millisSinceLastSendStart, message);
            }
        }

        if (message instanceof ScaleCubeMessage) {
            ScaleCubeMessage scaleCubeMessage = (ScaleCubeMessage) message;
            if (scaleCubeMessage.data() != null && scaleCubeMessage.data().getClass().getName().endsWith(".PingData")) {
                LOG.info(
                        "Sending ping to {}, cid {}, data {}",
                        consistentId,
                        scaleCubeMessage.headers().get("cid"),
                        scaleCubeMessage.data()
                );
            }
        }

        List<ClassDescriptorMessage> descriptors;

        long beforeMarshalling = System.nanoTime();
        try {
            descriptors = beforeRead(message);
        } catch (Exception e) {
            return failedFuture(new IgniteException("Failed to marshal message: " + e.getMessage(), e));
        }
        long marshallingMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - beforeMarshalling);
        if (marshallingMillis > 1000) {
            LOG.info("CCC2 Marshalling took {} ms for {}", marshallingMillis, message);
        }

        return connectionManager.channel(consistentId, type, addr)
                .thenComposeToCompletable(sender -> sender.send(new OutNetworkObject(message, descriptors)))
                .whenComplete((res, ex) -> {
                    long afterSentNanos = System.nanoTime();
                    long lastSendEndNanos = lastSendEnd(topologyService.localMember().name()).getAndSet(afterSentNanos);

                    if (lastSendEndNanos > 0) {
                        long nanosSinceLastSendEnd = afterSentNanos - lastSendEndNanos;
                        long millisSinceLastSendEnd = TimeUnit.NANOSECONDS.toMillis(nanosSinceLastSendEnd);

                        if (millisSinceLastSendEnd > 1000) {
                            LOG.info("EEE {} ms since last send end, message is {}", millisSinceLastSendEnd, message);
                        }
                    }
                });
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
            CompletableFuture.runAsync(() -> {
                long started = System.currentTimeMillis();

                try {
                    onMessage(obj);
                } catch (Throwable e) {
                    logAndRethrowIfError(obj, e);
                } finally {
                    long took = System.currentTimeMillis() - started;
                    if (took > 100) {
                        LOG.info("XXX Processing of {} from {} took {} ms", obj.message(), obj.consistentId(), took);
                    }
                }
            }, inboundExecutor).orTimeout(5, TimeUnit.SECONDS).whenComplete((res, ex) -> {
                if (ex instanceof TimeoutException) {
                    LOG.info("YYY Timeout while processing {} from {}", ex, obj.message(), obj.consistentId());
                }
            });

            return;
        }

        long nowNanos = System.nanoTime();
        long lastReceiveStartNanos = lastReceiveStart(obj.consistentId()).getAndSet(nowNanos);
        if (lastReceiveStartNanos > 0) {
            long nanosSinceLastReceive = nowNanos - lastReceiveStartNanos;
            long millisSinceLastReceive = TimeUnit.NANOSECONDS.toMillis(nanosSinceLastReceive);

            if (millisSinceLastReceive > 1000) {
                LOG.info("BBB {} ms since last receive, message is {}", millisSinceLastReceive, obj.message());
            }
        }

        NetworkMessage msg = obj.message();
        DescriptorRegistry registry = obj.registry();
        long unmarshalStartedNanos = System.nanoTime();
        try {
            msg.unmarshal(marshaller, registry);
        } catch (Exception e) {
            throw new IgniteException("Failed to unmarshal message: " + e.getMessage(), e);
        }
        long unmarshalMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - unmarshalStartedNanos);
        if (unmarshalMillis > 1000) {
            LOG.info("CCC {} ms to unmarshal, message is {}", unmarshalMillis, msg);
        }

        assert msg.messageId() != null;

        int sent = sent(obj.consistentId(), topologyService.localMember().name()).get();
        int received = received(obj.consistentId(), topologyService.localMember().name()).incrementAndGet();
        int inFlight = inFlight(obj.consistentId(), topologyService.localMember().name()).decrementAndGet();

        Long sentNanoTime = sendNanoTimes.remove(msg.messageId());
        if (sentNanoTime != null) {
            long nanosPassed = System.nanoTime() - sentNanoTime;
            long millisPassed = TimeUnit.NANOSECONDS.toMillis(nanosPassed);
            if (millisPassed > 1000) {
                LOG.info("AAA {} ms passed for {}", millisPassed, msg);
            }
        }

        if (msg instanceof ScaleCubeMessage) {
            ScaleCubeMessage scaleCubeMessage = (ScaleCubeMessage) msg;
            if (scaleCubeMessage.data() != null && scaleCubeMessage.data().getClass().getName().endsWith(".PingData")) {
                LOG.info(
                        "{}/{}/{} Receiving ping from {}, cid {}, data {}",
                        sent,
                        received,
                        inFlight,
                        obj.consistentId(),
                        scaleCubeMessage.headers().get("cid"),
                        scaleCubeMessage.data()
                );
            }
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

    private static void logAndRethrowIfError(InNetworkObject obj, Throwable e) {
        if (e instanceof UnresolvableConsistentIdException && obj.message() instanceof InvokeRequest) {
            LOG.info("onMessage() failed while processing {} from {} as the sender has left the topology",
                    obj.message(), obj.consistentId());
        } else {
            LOG.error("onMessage() failed while processing {} from {}", e, obj.message(), obj.consistentId());
        }

        if (e instanceof Error) {
            throw (Error) e;
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

    // TODO: IGNITE-18493 - remove/move this
    /**
     * Installs a predicate, it will be consulted with for each message being sent; when it returns {@code true}, the
     * message will be dropped (it will not be sent; the corresponding future will time out soon for {@code invoke()} methods
     * and will never complete for methods different from {@code invoke()}).
     *
     * @param predicate Predicate that will decide whether a message should be dropped. Its first argument is the recipient
     *     node's consistent ID.
     */
    @TestOnly
    public void dropMessages(BiPredicate<String, NetworkMessage> predicate) {
        dropMessagesPredicate = predicate;
    }

    /**
     * Returns a predicate used to decide whether a message should be dropped, or {@code null} if message dropping is disabled.
     */
    @TestOnly
    @Nullable
    public BiPredicate<String, NetworkMessage> dropMessagesPredicate() {
        return dropMessagesPredicate;
    }

    // TODO: IGNITE-18493 - remove/move this
    /**
     * Stops dropping messages.
     *
     * @see #dropMessages(BiPredicate)
     */
    @TestOnly
    public void stopDroppingMessages() {
        dropMessagesPredicate = null;
    }

    @TestOnly
    public ConnectionManager connectionManager() {
        return connectionManager;
    }

    private static class ChannelKey {
        private final String fromConsistentId;
        private final String toConsistentId;

        private ChannelKey(String fromConsistentId, String toConsistentId) {
            this.fromConsistentId = fromConsistentId;
            this.toConsistentId = toConsistentId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ChannelKey that = (ChannelKey) o;

            if (fromConsistentId != null ? !fromConsistentId.equals(that.fromConsistentId) : that.fromConsistentId != null) {
                return false;
            }
            return toConsistentId != null ? toConsistentId.equals(that.toConsistentId) : that.toConsistentId == null;
        }

        @Override
        public int hashCode() {
            int result = fromConsistentId != null ? fromConsistentId.hashCode() : 0;
            result = 31 * result + (toConsistentId != null ? toConsistentId.hashCode() : 0);
            return result;
        }
    }
}
