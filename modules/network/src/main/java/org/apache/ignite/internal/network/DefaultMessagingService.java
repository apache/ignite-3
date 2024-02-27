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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.network.NettyBootstrapFactory.isInNetworkThread;
import static org.apache.ignite.internal.network.serialization.PerSessionSerializationService.createClassDescriptorsMessages;
import static org.apache.ignite.internal.thread.ThreadOperation.NOTHING_ALLOWED;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.Function;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.message.ClassDescriptorMessage;
import org.apache.ignite.internal.network.message.InvokeRequest;
import org.apache.ignite.internal.network.message.InvokeResponse;
import org.apache.ignite.internal.network.message.ScaleCubeMessage;
import org.apache.ignite.internal.network.netty.ConnectionManager;
import org.apache.ignite.internal.network.netty.InNetworkObject;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.internal.network.recovery.StaleIdDetector;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.marshal.UserObjectMarshaller;
import org.apache.ignite.internal.thread.ExecutorChooser;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.worker.CriticalSingleThreadExecutor;
import org.apache.ignite.internal.worker.CriticalWorker;
import org.apache.ignite.internal.worker.CriticalWorkerRegistry;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyService;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/** Default messaging service implementation. */
public class DefaultMessagingService extends AbstractMessagingService {
    private static final IgniteLogger LOG = Loggers.forClass(DefaultMessagingService.class);

    /** Network messages factory. */
    private final NetworkMessagesFactory factory;

    /** Topology service. */
    private final TopologyService topologyService;

    private final StaleIdDetector staleIdDetector;

    /** User object marshaller. */
    private final UserObjectMarshaller marshaller;

    /** Class descriptor registry. */
    private final ClassDescriptorRegistry classDescriptorRegistry;

    private final CriticalWorkerRegistry criticalWorkerRegistry;

    /** Connection manager that provides access to {@link NettySender}. */
    private volatile ConnectionManager connectionManager;

    /** Collection that maps correlation id to the future for an invocation request. */
    private final ConcurrentMap<Long, CompletableFuture<NetworkMessage>> requestsMap = new ConcurrentHashMap<>();

    /** Correlation id generator. */
    private final AtomicLong correlationIdGenerator = new AtomicLong();

    /** Executor for outbound messages. */
    private final CriticalSingleThreadExecutor outboundExecutor;

    /** Executors for inbound messages. */
    private final LazyStripedExecutor inboundExecutors;

    // TODO: IGNITE-18493 - remove/move this
    @Nullable
    private volatile BiPredicate<String, NetworkMessage> dropMessagesPredicate;

    /**
     * Constructor.
     *
     * @param factory Network messages factory.
     * @param topologyService Topology service.
     * @param staleIdDetector Used to detect stale node IDs.
     * @param classDescriptorRegistry Descriptor registry.
     * @param marshaller Marshaller.
     */
    public DefaultMessagingService(
            String nodeName,
            NetworkMessagesFactory factory,
            TopologyService topologyService,
            StaleIdDetector staleIdDetector,
            ClassDescriptorRegistry classDescriptorRegistry,
            UserObjectMarshaller marshaller,
            CriticalWorkerRegistry criticalWorkerRegistry
    ) {
        this.factory = factory;
        this.topologyService = topologyService;
        this.staleIdDetector = staleIdDetector;
        this.classDescriptorRegistry = classDescriptorRegistry;
        this.marshaller = marshaller;
        this.criticalWorkerRegistry = criticalWorkerRegistry;

        outboundExecutor = new CriticalSingleThreadExecutor(
                IgniteThreadFactory.create(nodeName, "MessagingService-outbound", LOG, NOTHING_ALLOWED)
        );
        inboundExecutors = new CriticalLazyStripedExecutor(nodeName, "MessagingService-inbound", criticalWorkerRegistry);
    }

    /**
     * Resolves cyclic dependency and sets up the connection manager.
     *
     * @param connectionManager Connection manager.
     */
    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        connectionManager.addListener(this::handleMessageFromNetwork);
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

        return respond(recipient, type, msg, correlationId);
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

        return sendViaNetwork(recipient.name(), type, recipientAddress, message);
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

        return sendViaNetwork(recipient.name(), type, recipientAddress, message).thenCompose(unused -> responseFuture);
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
    private CompletableFuture<Void> sendViaNetwork(
            @Nullable String consistentId,
            ChannelType type,
            InetSocketAddress addr,
            NetworkMessage message
    ) {
        if (isInNetworkThread()) {
            return CompletableFuture.supplyAsync(() -> sendViaNetwork(consistentId, type, addr, message), outboundExecutor)
                    .thenCompose(Function.identity());
        }

        List<ClassDescriptorMessage> descriptors;

        try {
            descriptors = prepareMarshal(message);
        } catch (Exception e) {
            return failedFuture(new IgniteException("Failed to marshal message: " + e.getMessage(), e));
        }

        return connectionManager.channel(consistentId, type, addr)
                .thenComposeToCompletable(sender -> sender.send(
                        new OutNetworkObject(message, descriptors),
                        () -> triggerChannelCreation(consistentId, type, addr)
                ));
    }

    private void triggerChannelCreation(@Nullable String consistentId, ChannelType type, InetSocketAddress addr) {
        connectionManager.channel(consistentId, type, addr);
    }

    private List<ClassDescriptorMessage> prepareMarshal(NetworkMessage msg) throws Exception {
        IntSet ids = new IntOpenHashSet();

        msg.prepareMarshal(ids, marshaller);

        return createClassDescriptorsMessages(ids, classDescriptorRegistry);
    }

    /**
     * Sends a message to the current node.
     *
     * @param message Message.
     * @param correlationId Correlation id.
     */
    private void sendToSelf(NetworkMessage message, @Nullable Long correlationId) {
        for (HandlerContext context : getHandlerContexts(message.groupType())) {
            // Invoking on the same thread, ignoring the executor chooser registered with the handler.
            context.handler().onReceived(message, topologyService.localMember().name(), correlationId);
        }
    }

    /**
     * Handles a message coming from the network (not from the same node).
     *
     * @param inNetworkObject Incoming message wrapper.
     */
    private void handleMessageFromNetwork(InNetworkObject inNetworkObject) {
        assert isInNetworkThread() : Thread.currentThread().getName();

        if (senderIdIsStale(inNetworkObject)) {
            logMessageSkipDueToSenderLeft(inNetworkObject);
            return;
        }

        if (inNetworkObject.message() instanceof InvokeResponse) {
            Executor executor = chooseExecutorInInboundPool(inNetworkObject);
            executor.execute(() -> handleInvokeResponse(inNetworkObject));
            return;
        }

        NetworkMessage payload;
        Long correlationId = null;
        if (inNetworkObject.message() instanceof InvokeRequest) {
            InvokeRequest invokeRequest = (InvokeRequest) inNetworkObject.message();
            payload = invokeRequest.message();
            correlationId = invokeRequest.correlationId();
        } else {
            payload = inNetworkObject.message();
        }

        Iterator<HandlerContext> handlerContexts = getHandlerContexts(payload.groupType()).iterator();
        if (!handlerContexts.hasNext()) {
            // No need to handle this.
            return;
        }

        HandlerContext firstHandlerContext = handlerContexts.next();
        Executor firstHandlerExecutor = chooseExecutorFor(payload, inNetworkObject, firstHandlerContext.executorChooser());

        Long finalCorrelationId = correlationId;
        firstHandlerExecutor.execute(() -> {
            long startedNanos = System.nanoTime();

            try {
                handleStartingWithFirstHandler(payload, finalCorrelationId, inNetworkObject, firstHandlerContext, handlerContexts);
            } catch (Throwable e) {
                logAndRethrowIfError(inNetworkObject, e);
            } finally {
                long tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
                if (tookMillis > 100) {
                    LOG.warn("Processing of {} from {} took {} ms", inNetworkObject.message(), inNetworkObject.consistentId(), tookMillis);
                }
            }
        });
    }

    private static void logMessageSkipDueToSenderLeft(InNetworkObject inNetworkObject) {
        LOG.info("Sender ID {} ({}) is stale, so skipping message handling: {}",
                inNetworkObject.launchId(), inNetworkObject.consistentId(), inNetworkObject.message()
        );
    }

    private boolean senderIdIsStale(InNetworkObject obj) {
        return staleIdDetector.isIdStale(obj.launchId());
    }

    private void handleInvokeResponse(InNetworkObject inNetworkObject) {
        unmarshalMessage(inNetworkObject);

        InvokeResponse response = (InvokeResponse) inNetworkObject.message();
        onInvokeResponse(response.message(), response.correlationId());
    }

    private void unmarshalMessage(InNetworkObject obj) {
        try {
            obj.message().unmarshal(marshaller, obj.registry());
        } catch (Exception e) {
            throw new IgniteException("Failed to unmarshal message: " + e.getMessage(), e);
        }
    }

    private Executor chooseExecutorFor(NetworkMessage payload, InNetworkObject obj, ExecutorChooser<NetworkMessage> chooser) {
        if (wantsInboundPool(chooser)) {
            return chooseExecutorInInboundPool(obj);
        } else {
            return chooser.choose(payload);
        }
    }

    private Executor chooseExecutorInInboundPool(InNetworkObject obj) {
        return inboundExecutors.stripeFor(obj.connectionIndex());
    }

    /**
     * Finishes unmarshalling the message and handles it on current thread on first handler. Also handles it with other
     * handlers (second and so on) on executors chosen by their choosers.
     */
    private void handleStartingWithFirstHandler(
            NetworkMessage payload,
            @Nullable Long correlationId,
            InNetworkObject obj,
            HandlerContext firstHandlerContext,
            Iterator<HandlerContext> remainingContexts
    ) {
        if (senderIdIsStale(obj)) {
            logMessageSkipDueToSenderLeft(obj);
            return;
        }

        unmarshalMessage(obj);

        String senderConsistentId = obj.consistentId();

        // Unfortunately, since the Messaging Service is used by ScaleCube itself, some messages can be sent
        // before the node is added to the topology. ScaleCubeMessage handler guarantees to handle null sender consistent ID
        // without throwing an exception.
        assert payload instanceof ScaleCubeMessage || senderConsistentId != null;

        // If first handler chose an inbound pool, AND some other handlers also want to be executed on an inbound pool, this means that
        // they will be guaranteed to be executed on the same thread (as the inbound pool is striped, the stripe key is sender+channelId).
        // Hence these handlers will be executed on the current thread, so, to avoid additional latency, we'll collect them to a list
        // and execute here directly after executing the first handler, instead of submitting them to the inbound pool.
        List<NetworkMessageHandler> handlersWantingSameThreadAsFirst = List.of();

        while (remainingContexts.hasNext()) {
            HandlerContext handlerContext = remainingContexts.next();

            if (wantSamePool(firstHandlerContext, handlerContext)) {
                if (handlersWantingSameThreadAsFirst.isEmpty()) {
                    handlersWantingSameThreadAsFirst = new ArrayList<>();
                }
                handlersWantingSameThreadAsFirst.add(handlerContext.handler());
            } else {
                Executor executor = chooseExecutorFor(payload, obj, handlerContext.executorChooser());
                executor.execute(() -> handlerContext.handler().onReceived(payload, senderConsistentId, correlationId));
            }
        }

        firstHandlerContext.handler().onReceived(payload, senderConsistentId, correlationId);

        // Now execute those handlers that are guaranteed to be executed on the same thread as the first one.
        for (NetworkMessageHandler handler : handlersWantingSameThreadAsFirst) {
            handler.onReceived(payload, senderConsistentId, correlationId);
        }
    }

    private static boolean wantSamePool(HandlerContext handlerContext1, HandlerContext handlerContext2) {
        return wantsInboundPool(handlerContext1) && wantsInboundPool(handlerContext2);
    }

    private static boolean wantsInboundPool(HandlerContext handlerContext) {
        return wantsInboundPool(handlerContext.executorChooser());
    }

    private static boolean wantsInboundPool(ExecutorChooser<NetworkMessage> executorChooser) {
        return executorChooser == IN_INBOUND_POOL;
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
     * Starts the service.
     */
    public void start() {
        criticalWorkerRegistry.register(outboundExecutor);
    }

    /**
     * Stops the messaging service.
     */
    public void stop() {
        var exception = new NodeStoppingException();

        requestsMap.values().forEach(fut -> fut.completeExceptionally(exception));

        requestsMap.clear();

        criticalWorkerRegistry.unregister(outboundExecutor);

        inboundExecutors.close();
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

    private static class CriticalLazyStripedExecutor extends LazyStripedExecutor {
        private final String nodeName;
        private final String poolName;

        private final CriticalWorkerRegistry workerRegistry;

        private final List<CriticalWorker> registeredWorkers = new CopyOnWriteArrayList<>();

        CriticalLazyStripedExecutor(String nodeName, String poolName, CriticalWorkerRegistry workerRegistry) {
            this.nodeName = nodeName;
            this.poolName = poolName;
            this.workerRegistry = workerRegistry;
        }

        @Override
        protected ExecutorService newSingleThreadExecutor(int stripeIndex) {
            ThreadFactory threadFactory = IgniteThreadFactory.create(nodeName, poolName + "-" + stripeIndex, LOG, NOTHING_ALLOWED);
            CriticalSingleThreadExecutor executor = new CriticalSingleThreadExecutor(threadFactory);

            workerRegistry.register(executor);

            registeredWorkers.add(executor);

            return executor;
        }

        @Override
        protected void onStoppingInitiated() {
            super.onStoppingInitiated();

            for (CriticalWorker worker : registeredWorkers) {
                workerRegistry.unregister(worker);
            }
        }
    }
}
