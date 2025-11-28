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
import static org.apache.ignite.internal.lang.IgniteSystemProperties.LONG_HANDLING_LOGGING_ENABLED;
import static org.apache.ignite.internal.network.NettyBootstrapFactory.isInNetworkThread;
import static org.apache.ignite.internal.network.serialization.PerSessionSerializationService.createClassDescriptorsMessages;
import static org.apache.ignite.internal.thread.ThreadOperation.NOTHING_ALLOWED;
import static org.apache.ignite.internal.tostring.IgniteToStringBuilder.includeSensitive;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.apache.ignite.internal.util.IgniteUtils.awaitForWorkersStop;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.safeAbs;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.Function;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.future.timeout.TimeoutObject;
import org.apache.ignite.internal.future.timeout.TimeoutWorker;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.handshake.CriticalHandshakeException;
import org.apache.ignite.internal.network.message.ClassDescriptorMessage;
import org.apache.ignite.internal.network.message.InvokeRequest;
import org.apache.ignite.internal.network.message.InvokeResponse;
import org.apache.ignite.internal.network.message.ScaleCubeMessage;
import org.apache.ignite.internal.network.netty.ConnectionManager;
import org.apache.ignite.internal.network.netty.InNetworkObject;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.internal.network.handshake.NodeStaleException;
import org.apache.ignite.internal.network.recovery.StaleIdDetector;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.marshal.UserObjectMarshaller;
import org.apache.ignite.internal.thread.ExecutorChooser;
import org.apache.ignite.internal.thread.IgniteThread;
import org.apache.ignite.internal.worker.CriticalSingleThreadExecutor;
import org.apache.ignite.internal.worker.CriticalWorkerRegistry;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/** Default messaging service implementation. */
public class DefaultMessagingService extends AbstractMessagingService {
    private static final IgniteLogger LOG = Loggers.forClass(DefaultMessagingService.class);

    private final boolean longHandlingLoggingEnabled = IgniteSystemProperties.getBoolean(LONG_HANDLING_LOGGING_ENABLED, false);

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

    private final FailureProcessor failureProcessor;

    /** Connection manager that provides access to {@link NettySender}. */
    private volatile ConnectionManager connectionManager;

    /** Collection that maps correlation id to the future for an invocation request. */
    private final ConcurrentMap<Long, TimeoutObjectImpl> requestsMap = new ConcurrentHashMap<>();

    /** Correlation id generator. */
    private final AtomicLong correlationIdGenerator = new AtomicLong();

    /** Executor for outbound messages. */
    private final CriticalSingleThreadExecutor outboundExecutor;

    /** Executors for inbound messages. */
    private final CriticalStripedExecutors inboundExecutors;

    /** Network timeout worker thread. */
    private final TimeoutWorker timeoutWorker;

    // TODO: IGNITE-18493 - remove/move this
    @Nullable
    private volatile BiPredicate<String, NetworkMessage> dropMessagesPredicate;

    private final LocalIpAddresses localIpAddresses = new LocalIpAddresses();

    /**
     * Cache of {@link RecipientInetAddress} of recipient nodes ({@link InternalClusterNode}) by {@link InternalClusterNode#id} that are in
     * the topology and not stale.
     *
     * <p>Introduced for optimization - reducing the number of address resolving for the same nodes.</p>
     */
    private final Map<UUID, RecipientInetAddress> recipientInetAddrByNodeId = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param nodeName Consistent ID (aka name) of the local node associated with the service to create.
     * @param factory Network messages factory.
     * @param topologyService Topology service.
     * @param staleIdDetector Used to detect stale node IDs.
     * @param classDescriptorRegistry Descriptor registry.
     * @param marshaller Marshaller.
     * @param criticalWorkerRegistry Used to register critical threads managed by the new service and its components.
     * @param failureProcessor Failure processor.
     * @param channelTypeRegistry {@link ChannelType} registry.
     */
    public DefaultMessagingService(
            String nodeName,
            NetworkMessagesFactory factory,
            TopologyService topologyService,
            StaleIdDetector staleIdDetector,
            ClassDescriptorRegistry classDescriptorRegistry,
            UserObjectMarshaller marshaller,
            CriticalWorkerRegistry criticalWorkerRegistry,
            FailureProcessor failureProcessor,
            ChannelTypeRegistry channelTypeRegistry
    ) {
        this.factory = factory;
        this.topologyService = topologyService;
        this.staleIdDetector = staleIdDetector;
        this.classDescriptorRegistry = classDescriptorRegistry;
        this.marshaller = marshaller;
        this.criticalWorkerRegistry = criticalWorkerRegistry;
        this.failureProcessor = failureProcessor;

        outboundExecutor = new CriticalSingleThreadExecutor(
                IgniteMessageServiceThreadFactory.create(nodeName, "MessagingService-outbound", LOG, NOTHING_ALLOWED)
        );

        inboundExecutors = new CriticalStripedExecutors(
                nodeName,
                "MessagingService-inbound",
                criticalWorkerRegistry,
                channelTypeRegistry,
                LOG
        );

        timeoutWorker = new TimeoutWorker(
                LOG,
                nodeName,
                "MessagingService-timeout-worker",
                requestsMap,
                failureProcessor
        );
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
    public void weakSend(InternalClusterNode recipient, NetworkMessage msg) {
        send(recipient, msg);
    }

    @Override
    public CompletableFuture<Void> send(InternalClusterNode recipient, ChannelType channelType, NetworkMessage msg) {
        return send0(recipient, channelType, msg, null, true);
    }

    @Override
    public CompletableFuture<Void> send(String recipientConsistentId, ChannelType channelType, NetworkMessage msg) {
        InternalClusterNode recipient = topologyService.getByConsistentId(recipientConsistentId);

        if (recipient == null) {
            return failedFuture(
                    new UnresolvableConsistentIdException("Recipient consistent ID cannot be resolved: " + recipientConsistentId)
            );
        }

        return send0(recipient, channelType, msg, null, false);
    }

    @Override
    public CompletableFuture<Void> send(NetworkAddress recipientNetworkAddress, ChannelType channelType, NetworkMessage msg) {
        InternalClusterNode recipient = topologyService.getByAddress(recipientNetworkAddress);

        // Create a fake node for nodes that are not in the topology yet.
        if (recipient == null) {
            recipient = new ClusterNodeImpl(null, null, recipientNetworkAddress);
        }

        return send0(recipient, channelType, msg, null, false);
    }

    @Override
    public CompletableFuture<Void> respond(InternalClusterNode recipient, ChannelType type, NetworkMessage msg, long correlationId) {
        return send0(recipient, type, msg, correlationId, true);
    }

    @Override
    public CompletableFuture<Void> respond(String recipientConsistentId, ChannelType type, NetworkMessage msg, long correlationId) {
        InternalClusterNode recipient = topologyService.getByConsistentId(recipientConsistentId);

        if (recipient == null) {
            return failedFuture(
                    new UnresolvableConsistentIdException("Recipient consistent ID cannot be resolved: " + recipientConsistentId)
            );
        }

        return send0(recipient, type, msg, correlationId, false);
    }

    @Override
    public CompletableFuture<NetworkMessage> invoke(InternalClusterNode recipient, ChannelType type, NetworkMessage msg, long timeout) {
        return invoke0(recipient, type, msg, timeout, true);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NetworkMessage> invoke(String recipientConsistentId, ChannelType type, NetworkMessage msg, long timeout) {
        InternalClusterNode recipient = topologyService.getByConsistentId(recipientConsistentId);

        if (recipient == null) {
            return failedFuture(
                    new UnresolvableConsistentIdException("Recipient consistent ID cannot be resolved: " + recipientConsistentId)
            );
        }

        return invoke0(recipient, type, msg, timeout, false);
    }

    /**
     * Sends a message. If the target is the current node, then message will be delivered immediately.
     *
     * @param recipient Target cluster node.
     * @param msg Message.
     * @param correlationId Correlation id. Not null iff the message is a response to a {@link #invoke} request.
     * @param strictIdCheck Whether {@link RecipientLeftException} is to be thrown if the node at the other side of the channel
     *     actually has ID different from the ID in the recipient object (that is, that the recipient has been restarted).
     * @return Future of the send operation.
     */
    private CompletableFuture<Void> send0(
            InternalClusterNode recipient,
            ChannelType type,
            NetworkMessage msg,
            @Nullable Long correlationId,
            boolean strictIdCheck
    ) {
        if (connectionManager.isStopped()) {
            return failedFuture(new NodeStoppingException());
        }

        // TODO: IGNITE-18493 - remove/move this
        if (shouldDropMessage(recipient, msg)) {
            return nullCompletedFuture();
        }

        InetSocketAddress recipientAddress = resolveRecipientAddress(recipient);

        if (recipientAddress == null) {
            if (correlationId != null) {
                onInvokeResponse(msg, correlationId);
            } else {
                sendToSelf(msg, null);
            }

            return nullCompletedFuture();
        }

        NetworkMessage message = correlationId != null ? responseFromMessage(msg, correlationId) : msg;

        return sendViaNetwork(recipient.id(), type, recipientAddress, message, strictIdCheck);
    }

    private boolean shouldDropMessage(InternalClusterNode recipient, NetworkMessage msg) {
        BiPredicate<String, NetworkMessage> predicate = dropMessagesPredicate;

        return predicate != null && predicate.test(recipient.name(), msg);
    }

    /**
     * Sends an invocation request. If the target is the current node, then message will be delivered immediately.
     *
     * @param recipient Target cluster node.
     * @param msg Message.
     * @param timeout Invocation timeout.
     * @param strictIdCheck Whether {@link RecipientLeftException} is to be thrown if the node at the other side of the channel
     *     actually has ID different from the ID in the recipient object (that is, that the recipient has been restarted).
     * @return A future holding the response or error if the expected response was not received.
     */
    private CompletableFuture<NetworkMessage> invoke0(
            InternalClusterNode recipient,
            ChannelType type,
            NetworkMessage msg,
            long timeout,
            boolean strictIdCheck
    ) {
        if (connectionManager.isStopped()) {
            return failedFuture(new NodeStoppingException());
        }

        // TODO: IGNITE-18493 - remove/move this
        if (shouldDropMessage(recipient, msg)) {
            return new CompletableFuture<NetworkMessage>().orTimeout(10, TimeUnit.MILLISECONDS);
        }

        long correlationId = createCorrelationId();

        CompletableFuture<NetworkMessage> responseFuture = new CompletableFuture<>();

        requestsMap.put(correlationId, new TimeoutObjectImpl(timeout > 0 ? coarseCurrentTimeMillis() + timeout : 0, responseFuture, msg));

        InetSocketAddress recipientAddress = resolveRecipientAddress(recipient);

        if (recipientAddress == null) {
            sendToSelf(msg, correlationId);

            return responseFuture;
        }

        InvokeRequest message = requestFromMessage(msg, correlationId);

        return sendViaNetwork(recipient.id(), type, recipientAddress, message, strictIdCheck)
                .thenCompose(unused -> responseFuture);
    }

    /**
     * Sends network object.
     *
     * @param nodeId Target node ID.
     * @param type Channel type for send.
     * @param addr Target address.
     * @param message Message.
     * @param strictIdCheck Whether {@link RecipientLeftException} is to be thrown if the node at the other side of the channel
     *     actually has ID different from the ID in the recipient object (that is, that the recipient has been restarted).
     * @return Future of the send operation.
     */
    private CompletableFuture<Void> sendViaNetwork(
            UUID nodeId,
            ChannelType type,
            InetSocketAddress addr,
            NetworkMessage message,
            boolean strictIdCheck
    ) {
        if (isInNetworkThread()) {
            return CompletableFuture.supplyAsync(() -> sendViaNetwork(nodeId, type, addr, message, strictIdCheck), outboundExecutor)
                    .thenCompose(Function.identity());
        }

        List<ClassDescriptorMessage> descriptors;

        try {
            descriptors = prepareMarshal(message);
        } catch (Exception e) {
            return failedFuture(new IgniteException(INTERNAL_ERR, "Failed to marshal message: " + e.getMessage(), e));
        }

        return connectionManager.channel(nodeId, type, addr)
                .thenComposeToCompletable(sender -> {
                    if (strictIdCheck && nodeId != null && !sender.launchId().equals(nodeId)) {
                        // The destination node has been rebooted, so it's a different node instance.
                        throw new RecipientLeftException("Target node ID is " + nodeId + ", but " + sender.launchId() + " responded");
                    }

                    return sender.send(
                            new OutNetworkObject(message, descriptors),
                            () -> triggerChannelCreation(nodeId, type, addr)
                    );
                })
                .whenComplete((res, ex) -> handleHandshakeError(ex, nodeId, type, addr));
    }

    private void handleHandshakeError(Throwable ex, UUID nodeId, ChannelType type, InetSocketAddress addr) {
        if (ex != null) {
            if (hasCause(ex, CriticalHandshakeException.class)) {
                LOG.error(
                        "Handshake failed [destNodeId={}, channelType={}, destAddr={}, localBindAddr={}]", ex,
                        nodeId, type, addr, connectionManager.localBindAddress()
                );
            } else if (hasCause(ex, NodeStaleException.class) && LOG.isDebugEnabled()) {
                LOG.debug(
                        "Handshake failed [message={}, destNodeId={}, channelType={}, destAddr={}, localBindAddr={}]",
                        ex.getMessage(), nodeId, type, addr, connectionManager.localBindAddress()
                );
            } else if (!hasCause(ex, NodeStoppingException.class, NodeStaleException.class) && LOG.isInfoEnabled()) {
                // TODO IGNITE-25802 Detect a LOOP rejection reason and retry the connection.
                LOG.info(
                        "Handshake failed [message={}, destNodeId={}, channelType={}, destAddr={}, localBindAddr={}]",
                        ex.getMessage(), nodeId, type, addr, connectionManager.localBindAddress()
                );
            }
        }
    }

    private void triggerChannelCreation(UUID nodeId, ChannelType type, InetSocketAddress addr) {
        connectionManager.channel(nodeId, type, addr);
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
        List<HandlerContext> handlerContexts = getHandlerContexts(message.groupType());

        // Specially made by a classic loop for optimization.
        for (int i = 0; i < handlerContexts.size(); i++) {
            HandlerContext handlerContext = handlerContexts.get(i);

            // Invoking on the same thread, ignoring the executor chooser registered with the handler.
            handlerContext.handler().onReceived(message, topologyService.localMember(), correlationId);
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

        NetworkMessage message = inNetworkObject.message();

        if (message instanceof InvokeResponse) {
            Executor executor = chooseExecutorInInboundPool(inNetworkObject);
            executor.execute(() -> handleInvokeResponse(inNetworkObject));
            return;
        }

        NetworkMessage payload;
        Long correlationId = null;
        if (message instanceof InvokeRequest) {
            InvokeRequest invokeRequest = (InvokeRequest) message;
            payload = invokeRequest.message();
            correlationId = invokeRequest.correlationId();
        } else {
            payload = message;
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
            long startedNanos = longHandlingLoggingEnabled ? System.nanoTime() : 0;

            try {
                handleStartingWithFirstHandler(payload, finalCorrelationId, inNetworkObject, firstHandlerContext, handlerContexts);
            } catch (Throwable e) {
                handleAndRethrowIfError(inNetworkObject, e);
            } finally {
                if (longHandlingLoggingEnabled && LOG.isWarnEnabled()) {
                    long tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);

                    if (tookMillis > 100) {
                        LOG.warn(
                                "Processing of {} from {} took {} ms",
                                LOG.isDebugEnabled() && includeSensitive() ? message : message.toStringForLightLogging(),
                                inNetworkObject.sender(),
                                tookMillis
                        );
                    }
                }
            }
        });
    }

    private static void logMessageSkipDueToSenderLeft(InNetworkObject inNetworkObject) {
        if (LOG.isInfoEnabled()) {
            NetworkMessage message = inNetworkObject.message();

            LOG.info("Sender ID {} ({}) is stale, so skipping message handling: {}",
                    inNetworkObject.launchId(),
                    inNetworkObject.consistentId(),
                    LOG.isDebugEnabled() && includeSensitive() ? message : message.toStringForLightLogging()
            );
        }
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
            throw new IgniteException(INTERNAL_ERR, "Failed to unmarshal message: " + e.getMessage(), e);
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
        int stripeIndex = safeAbs(obj.sender().id().hashCode());

        return inboundExecutors.executorFor(obj.connectionIndex(), stripeIndex);
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

        // Unfortunately, since the Messaging Service is used by ScaleCube itself, some messages can be sent
        // before the node is added to the topology. ScaleCubeMessage handler guarantees to handle null sender consistent ID
        // without throwing an exception.
        assert payload instanceof ScaleCubeMessage || obj.consistentId() != null;

        // If other handlers have the same chooser as the first handler, this means that we can execute them on the same
        // executor that was chosen for the first one. This will save us some resubmissions: we'll just execute on the same
        // thread (it will be current thread which belongs to the executor chosen for the first handler).
        List<NetworkMessageHandler> handlersWithSameChooserAsFirst = List.of();

        while (remainingContexts.hasNext()) {
            HandlerContext handlerContext = remainingContexts.next();

            if (firstHandlerContext.executorChooser() == handlerContext.executorChooser()) {
                if (handlersWithSameChooserAsFirst.isEmpty()) {
                    handlersWithSameChooserAsFirst = new ArrayList<>();
                }
                handlersWithSameChooserAsFirst.add(handlerContext.handler());
            } else {
                Executor executor = chooseExecutorFor(payload, obj, handlerContext.executorChooser());
                executor.execute(() -> handlerContext.handler().onReceived(payload, obj.sender(), correlationId));
            }
        }

        firstHandlerContext.handler().onReceived(payload, obj.sender(), correlationId);

        // Now execute those handlers that have the same chooser as the first one.
        for (NetworkMessageHandler handler : handlersWithSameChooserAsFirst) {
            handler.onReceived(payload, obj.sender(), correlationId);
        }
    }

    private void handleAndRethrowIfError(InNetworkObject obj, Throwable e) {
        NetworkMessage message = obj.message();

        Object messageDetails = LOG.isDebugEnabled() && includeSensitive() ? message : message.toStringForLightLogging();
        if (e instanceof UnresolvableConsistentIdException && message instanceof InvokeRequest) {
            if (LOG.isInfoEnabled()) {
                LOG.info(
                        "onMessage() failed while processing {} from {} as the sender has left the topology",
                        messageDetails,
                        obj.sender()
                );
            }
        } else {
            String errorMessage = String.format("onMessage() failed while processing %s from %s", messageDetails, obj.sender());
            failureProcessor.process(new FailureContext(e, errorMessage));
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
        TimeoutObjectImpl responseFuture = requestsMap.remove(correlationId);

        if (responseFuture != null) {
            responseFuture.future().complete(response);
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
     * Starts the service.
     */
    public void start() {
        localIpAddresses.start();

        new IgniteThread(timeoutWorker).start();

        criticalWorkerRegistry.register(outboundExecutor);

        topologyService.addEventHandler(new TopologyEventHandler() {
            @Override
            public void onDisappeared(InternalClusterNode member) {
                recipientInetAddrByNodeId.remove(member.id());
            }
        });
    }

    /**
     * Stops the messaging service.
     */
    public void stop() throws Exception {
        var exception = new NodeStoppingException();

        requestsMap.values().forEach(fut -> fut.future().completeExceptionally(exception));

        requestsMap.clear();

        criticalWorkerRegistry.unregister(outboundExecutor);

        recipientInetAddrByNodeId.clear();

        closeAll(
                inboundExecutors::close,
                () -> shutdownAndAwaitTermination(outboundExecutor, 10, TimeUnit.SECONDS),
                () -> awaitForWorkersStop(List.of(timeoutWorker), true, LOG)
        );
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
    public void dropMessages(BiPredicate<@Nullable String, NetworkMessage> predicate) {
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

    /**
     * Timeout object wrapper for the completable future.
     */
    private static class TimeoutObjectImpl implements TimeoutObject<CompletableFuture<NetworkMessage>> {
        /** End time (milliseconds since Unix epoch). */
        private final long endTime;

        /** Target future. */
        private final CompletableFuture<NetworkMessage> fut;

        private final NetworkMessage request;

        /**
         * Constructor.
         *
         * @param endTime End timestamp in milliseconds.
         * @param fut Target future.
         * @param request Request that is being sent.
         */
        private TimeoutObjectImpl(long endTime, CompletableFuture<NetworkMessage> fut, NetworkMessage request) {
            this.endTime = endTime;
            this.fut = fut;
            this.request = request;
        }

        @Override
        public long endTime() {
            return endTime;
        }

        @Override
        public CompletableFuture<NetworkMessage> future() {
            return fut;
        }

        @Override
        public @Nullable String describe() {
            return "Invocation timed out [message=" + request.toStringForLightLogging() + "]";
        }
    }

    /**
     * Returns the resolved address of the target node, {@code null} if the target node is the current node.
     *
     * <p>NOTE: Method was written as a result of analyzing the performance of sending a message to yourself.</p>
     *
     * @param recipientNode Target cluster node.
     */
    @Nullable InetSocketAddress resolveRecipientAddress(InternalClusterNode recipientNode) {
        // Node ID is {@code null} if this is a Scalecube request when the node does not know yet whose this address is.
        if (recipientNode.id() != null) {
            return connectionManager.nodeId().equals(recipientNode.id()) ? null : getFromCacheOrCreateResolved(recipientNode);
        }

        return RecipientInetAddress.create(connectionManager.localBindAddress(), recipientNode.address(), localIpAddresses).address();
    }

    private @Nullable InetSocketAddress getFromCacheOrCreateResolved(InternalClusterNode recipientNode) {
        assert recipientNode.id() != null : "Node has not been added to the topology: " + recipientNode.id();

        InetSocketAddress localBindAddress = connectionManager.localBindAddress();
        NetworkAddress recipientAddress = recipientNode.address();

        RecipientInetAddress address = recipientInetAddrByNodeId.compute(recipientNode.id(), (nodeId, existingAddress) -> {
            if (staleIdDetector.isIdStale(nodeId)) {
                return null;
            }

            return existingAddress != null ? existingAddress
                    : RecipientInetAddress.create(localBindAddress, recipientAddress, localIpAddresses);
        });

        if (address == null) {
            address = RecipientInetAddress.create(localBindAddress, recipientAddress, localIpAddresses);
        }

        return address.address();
    }
}
