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

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.extractCodeFrom;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.handler.ssl.SslContext;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.ignite.internal.future.OrderingFuture;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.ChannelTypeRegistry;
import org.apache.ignite.internal.network.ClusterIdSupplier;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.network.configuration.NetworkView;
import org.apache.ignite.internal.network.configuration.SslConfigurationSchema;
import org.apache.ignite.internal.network.configuration.SslView;
import org.apache.ignite.internal.network.handshake.ChannelAlreadyExistsException;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.recovery.DescriptorAcquiry;
import org.apache.ignite.internal.network.recovery.RecoveryAcceptorHandshakeManager;
import org.apache.ignite.internal.network.recovery.RecoveryDescriptor;
import org.apache.ignite.internal.network.recovery.RecoveryDescriptorProvider;
import org.apache.ignite.internal.network.recovery.RecoveryInitiatorHandshakeManager;
import org.apache.ignite.internal.network.recovery.RecoveryInitiatorHandshakeManagerFactory;
import org.apache.ignite.internal.network.recovery.StaleIdDetector;
import org.apache.ignite.internal.network.serialization.SerializationService;
import org.apache.ignite.internal.network.ssl.SslContextProvider;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.version.IgniteProductVersionSource;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Class that manages connections both incoming and outgoing.
 */
public class ConnectionManager implements ChannelCreationListener {
    /** Message factory. */
    private static final NetworkMessagesFactory FACTORY = new NetworkMessagesFactory();

    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ConnectionManager.class);

    /** Latest version of the direct marshalling protocol. */
    public static final byte DIRECT_PROTOCOL_VERSION = 1;

    private static final int MAX_RETRIES_TO_OPEN_CHANNEL = 10;

    /** Client bootstrap. */
    private final Bootstrap clientBootstrap;

    /** Server. */
    private final NettyServer server;

    /** Channels map from nodeId to {@link NettySender}. */
    private final Map<ConnectorKey<UUID>, NettySender> channels = new ConcurrentHashMap<>();

    /** Clients. */
    private final Map<ConnectorKey<InetSocketAddress>, NettyClient> clients = new ConcurrentHashMap<>();

    /** Serialization service. */
    private final SerializationService serializationService;

    /** Message listeners. */
    private final List<Consumer<InNetworkObject>> listeners = new CopyOnWriteArrayList<>();

    /** Node ephemeral ID. */
    private final UUID nodeId;

    /**
     * Completed when local node is set; attempts to initiate a connection to this node from the outside will wait
     * till it's completed.
     */
    private final CompletableFuture<InternalClusterNode> localNodeFuture = new CompletableFuture<>();

    private final NettyBootstrapFactory bootstrapFactory;

    /** Used to detect that a peer uses a stale ID. */
    private final StaleIdDetector staleIdDetector;

    private final ClusterIdSupplier clusterIdSupplier;

    /** Factory producing {@link RecoveryInitiatorHandshakeManager} instances. */
    private final @Nullable RecoveryInitiatorHandshakeManagerFactory initiatorHandshakeManagerFactory;

    /** Start flag. */
    private final AtomicBoolean started = new AtomicBoolean(false);

    private final AtomicBoolean stopping = new AtomicBoolean(false);

    /** Stop flag. */
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    /** Recovery descriptor provider. */
    private final RecoveryDescriptorProvider descriptorProvider = new DefaultRecoveryDescriptorProvider();

    /** Thread pool used for connection management tasks (like disposing recovery descriptors on node left or on stop). */
    private final ExecutorService connectionMaintenanceExecutor;

    private final ChannelTypeRegistry channelTypeRegistry;

    private final IgniteProductVersionSource productVersionSource;

    /** {@code null} if SSL is not {@link SslConfigurationSchema#enabled}. */
    private final @Nullable SslContext clientSslContext;

    /**
     * Constructor.
     *
     * @param networkConfiguration Network configuration.
     * @param serializationService Serialization service.
     * @param nodeName Node name.
     * @param nodeId ID of this node.
     * @param bootstrapFactory Bootstrap factory.
     * @param staleIdDetector Detects stale member IDs.
     * @param clusterIdSupplier Supplier of cluster ID.
     * @param channelTypeRegistry {@link ChannelType} registry.
     * @param productVersionSource Source of product version.
     */
    public ConnectionManager(
            NetworkView networkConfiguration,
            SerializationService serializationService,
            String nodeName,
            UUID nodeId,
            NettyBootstrapFactory bootstrapFactory,
            StaleIdDetector staleIdDetector,
            ClusterIdSupplier clusterIdSupplier,
            ChannelTypeRegistry channelTypeRegistry,
            IgniteProductVersionSource productVersionSource
    ) {
        this(
                networkConfiguration,
                serializationService,
                nodeName,
                nodeId,
                bootstrapFactory,
                staleIdDetector,
                clusterIdSupplier,
                null,
                channelTypeRegistry,
                productVersionSource
        );
    }

    /**
     * Constructor.
     *
     * @param networkConfiguration Network configuration.
     * @param serializationService Serialization service.
     * @param nodeName Node name.
     * @param nodeId ID of this node.
     * @param bootstrapFactory Bootstrap factory.
     * @param staleIdDetector Detects stale member IDs.
     * @param clusterIdSupplier Supplier of cluster ID.
     * @param initiatorHandshakeManagerFactory Factory for {@link RecoveryInitiatorHandshakeManager} instances.
     * @param channelTypeRegistry {@link ChannelType} registry.
     * @param productVersionSource Source of product version.
     */
    public ConnectionManager(
            NetworkView networkConfiguration,
            SerializationService serializationService,
            String nodeName,
            UUID nodeId,
            NettyBootstrapFactory bootstrapFactory,
            StaleIdDetector staleIdDetector,
            ClusterIdSupplier clusterIdSupplier,
            @Nullable RecoveryInitiatorHandshakeManagerFactory initiatorHandshakeManagerFactory,
            ChannelTypeRegistry channelTypeRegistry,
            IgniteProductVersionSource productVersionSource
    ) {
        this.serializationService = serializationService;
        this.nodeId = nodeId;
        this.bootstrapFactory = bootstrapFactory;
        this.staleIdDetector = staleIdDetector;
        this.clusterIdSupplier = clusterIdSupplier;
        this.initiatorHandshakeManagerFactory = initiatorHandshakeManagerFactory;
        this.channelTypeRegistry = channelTypeRegistry;
        this.productVersionSource = productVersionSource;

        SslView ssl = networkConfiguration.ssl();

        clientSslContext = ssl.enabled() ? SslContextProvider.createClientSslContext(ssl) : null;

        this.server = new NettyServer(
                networkConfiguration,
                this::createAcceptorHandshakeManager,
                this::onMessage,
                serializationService,
                bootstrapFactory,
                ssl.enabled() ? SslContextProvider.createServerSslContext(ssl) : null
        );

        this.clientBootstrap = bootstrapFactory.createOutboundBootstrap();

        // We don't just use Executors#newSingleThreadExecutor() here because the maintenance thread will
        // be kept alive forever, and we only need it from time to time, so it seems a waste to keep the thread alive.
        ThreadPoolExecutor maintenanceExecutor = new ThreadPoolExecutor(
                1,
                1,
                1,
                SECONDS,
                new LinkedBlockingQueue<>(),
                IgniteThreadFactory.create(nodeName, "connection-maintenance", LOG)
        );
        maintenanceExecutor.allowCoreThreadTimeOut(true);

        connectionMaintenanceExecutor = maintenanceExecutor;
    }

    /**
     * Starts the server.
     *
     * @throws IgniteInternalException If failed to start.
     */
    public void start() throws IgniteInternalException {
        try {
            boolean wasStarted = started.getAndSet(true);

            if (wasStarted) {
                throw new IgniteInternalException("Attempted to start an already started connection manager");
            }

            if (stopped.get()) {
                throw new IgniteInternalException("Attempted to start an already stopped connection manager");
            }

            server.start().get();

            LOG.info("Server started [address={}]", server.address());
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();

            throw new IgniteInternalException(
                    extractCodeFrom(cause),
                    "Failed to start the connection manager: " + cause.getMessage(),
                    cause
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInternalException("Interrupted while starting the connection manager", e);
        }
    }

    /**
     * Returns server local bind address (might be an 'any local'/wildcard address if bound to all interfaces).
     *
     * @return Server local bind address.
     */
    public InetSocketAddress localBindAddress() {
        return (InetSocketAddress) server.address();
    }

    /**
     * Gets a {@link NettySender}, that sends data from this node to another node with the specified address.
     *
     * @param nodeId Another node's id.
     * @param address Another node's address.
     * @return Sender.
     */
    public OrderingFuture<NettySender> channel(UUID nodeId, ChannelType type, InetSocketAddress address) {
        return getChannelWithRetry(nodeId, type, address, 0);
    }

    private OrderingFuture<NettySender> getChannelWithRetry(
            UUID nodeId,
            ChannelType type,
            InetSocketAddress address,
            int attempt
    ) {
        if (attempt > MAX_RETRIES_TO_OPEN_CHANNEL) {
            return OrderingFuture.failedFuture(new IllegalStateException("Too many attempts to open channel to node \"" + nodeId
                    + "\", address=" + address));
        }

        return doGetChannel(nodeId, type, address)
                .handle((res, ex) -> {
                    if (ex instanceof ChannelAlreadyExistsException) {
                        return getChannelWithRetry(((ChannelAlreadyExistsException) ex).nodeId(), type, address, attempt + 1);
                    }
                    if (ex != null && ex.getCause() instanceof ChannelAlreadyExistsException) {
                        return getChannelWithRetry(
                                ((ChannelAlreadyExistsException) ex.getCause()).nodeId(),
                                type,
                                address,
                                attempt + 1
                        );
                    }
                    if (ex != null) {
                        return OrderingFuture.<NettySender>failedFuture(ex);
                    }

                    assert res != null;
                    if (res.isOpen()) {
                        return OrderingFuture.completedFuture(res);
                    } else {
                        return getChannelWithRetry(nodeId, type, address, attempt + 1);
                    }
                })
                .thenCompose(identity());
    }

    private OrderingFuture<NettySender> doGetChannel(
            UUID nodeId,
            ChannelType type,
            InetSocketAddress address
    ) {
        // Try looking up a channel by node id. There can be an outbound connection or an inbound connection associated with that
        // node id.
        NettySender channel = channels.compute(
                new ConnectorKey<>(nodeId, type),
                (key, sender) -> (sender == null || !sender.isOpen()) ? null : sender
        );

        if (channel != null) {
            return OrderingFuture.completedFuture(channel);
        }

        // Get an existing client or create a new one. NettyClient provides a future that resolves
        // when the client is ready for write operations, so previously started client, that didn't establish connection
        // or didn't perform the handshake operation, can be reused.
        @Nullable NettyClient client = clients.compute(
                new ConnectorKey<>(address, type),
                (key, existingClient) -> isClientConnected(existingClient) ? existingClient : connect(key.id(), key.type())
        );

        if (client == null) {
            return OrderingFuture.failedFuture(new NodeStoppingException("No outgoing connections are allowed as the node is stopping"));
        }

        return client.sender();
    }

    private static boolean isClientConnected(@Nullable NettyClient client) {
        return client != null && !client.failedToConnect() && !client.isDisconnected();
    }

    /**
     * Callback that is called upon receiving a new message.
     *
     * @param message New message.
     */
    private void onMessage(InNetworkObject message) {
        listeners.forEach(consumer -> consumer.accept(message));
    }

    /**
     * Callback that is called upon new initiator connected to the acceptor.
     *
     * @param channel Channel from initiator to this acceptor (represented with {@link #server}).
     */
    @Override
    public void handshakeFinished(NettySender channel) {
        ConnectorKey<UUID> key = new ConnectorKey<>(channel.launchId(), channelTypeRegistry.get(channel.channelId()));
        NettySender oldChannel = channels.put(key, channel);

        // Old channel can still be in the map, but it must be closed already by the tie breaker in the
        // handshake manager.
        assert oldChannel == null || !oldChannel.isOpen() : "Incorrect channel creation flow";

        // Preventing a race between calling handleNodeLeft() and putting a new channel that was just opened (with the node
        // which is already stale). If it's stale, then the stale detector already knows it (and it knows it before
        // handleNodeLeft() gets called as it subscribes first).
        // This is the only place where a new sender might be added to the map.
        if (staleIdDetector.isIdStale(channel.launchId())) {
            closeSenderAndDisposeDescriptor(channel, new RecipientLeftException("Recipient is stale [id=" + channel.launchId() + ']'));

            channels.remove(key, channel);
        } else if (stopping.get()) {
            // Same thing as above, but for stopping this node instead of handling a 'node left' for another node.
            closeSenderAndDisposeDescriptor(channel, new NodeStoppingException());

            channels.remove(key, channel);
        }
    }

    private void closeSenderAndDisposeDescriptor(NettySender sender, Exception exceptionToFailSendFutures) {
        connectionMaintenanceExecutor.submit(() -> {
            sender.closeAsync().whenCompleteAsync((res, ex) -> {
                RecoveryDescriptor recoveryDescriptor = descriptorProvider.getRecoveryDescriptor(
                        sender.consistentId(),
                        sender.launchId(),
                        sender.channelId()
                );

                blockAndDisposeDescriptor(recoveryDescriptor, exceptionToFailSendFutures);
            }, connectionMaintenanceExecutor);
        });
    }

    /**
     * Create new client from this node to specified address.
     *
     * @param address Target address.
     * @return New netty client or {@code null} if we are stopping.
     */
    private @Nullable NettyClient connect(InetSocketAddress address, ChannelType channelType) {
        if (stopping.get()) {
            return null;
        }

        var client = new NettyClient(
                address,
                serializationService,
                createInitiatorHandshakeManager(channelType.id()),
                this::onMessage,
                clientSslContext
        );

        client.start(clientBootstrap).whenComplete((sender, throwable) -> {
            if (throwable != null) {
                clients.remove(new ConnectorKey<>(address, channelType));
            }
        });

        return client;
    }

    /**
     * Add incoming message listener.
     *
     * @param listener Message listener.
     */
    public void addListener(Consumer<InNetworkObject> listener) {
        listeners.add(listener);
    }

    /**
     * Stops the server and all clients.
     */
    public void stop() {
        boolean wasStopped = this.stopped.getAndSet(true);

        if (wasStopped) {
            return;
        }

        assert stopping.get();

        //noinspection FuseStreamOperations
        List<CompletableFuture<Void>> stopFutures = new ArrayList<>(clients.values().stream().map(NettyClient::stop).collect(toList()));
        stopFutures.add(server.stop());
        stopFutures.addAll(channels.values().stream().map(NettySender::closeAsync).collect(toList()));
        stopFutures.add(disposeDescriptors());

        CompletableFuture<Void> finalStopFuture = allOf(stopFutures.toArray(CompletableFuture<?>[]::new));

        try {
            finalStopFuture.get();
        } catch (Exception e) {
            LOG.warn("Failed to stop connection manager [reason={}]", e.getMessage());
        }

        IgniteUtils.shutdownAndAwaitTermination(connectionMaintenanceExecutor, 10, SECONDS);
    }

    private CompletableFuture<Void> disposeDescriptors() {
        Exception exceptionToFailSendFutures = new NodeStoppingException();

        Collection<RecoveryDescriptor> descriptors = descriptorProvider.getAllRecoveryDescriptors();
        List<CompletableFuture<Void>> disposeFutures = new ArrayList<>(descriptors.size());
        for (RecoveryDescriptor descriptor : descriptors) {
            disposeFutures.add(blockAndDisposeDescriptor(descriptor, exceptionToFailSendFutures));
        }

        return allOf(disposeFutures.toArray(CompletableFuture[]::new));
    }

    /**
     * Returns {@code true} if the connection manager is stopped or is being stopped, {@code false} otherwise.
     *
     * @return {@code true} if the connection manager is stopped or is being stopped, {@code false} otherwise.
     */
    public boolean isStopped() {
        return stopped.get();
    }

    private HandshakeManager createInitiatorHandshakeManager(short connectionId) {
        InternalClusterNode localNode = Objects.requireNonNull(localNodeFuture.getNow(null), "localNode not set");

        if (initiatorHandshakeManagerFactory == null) {
            return new RecoveryInitiatorHandshakeManager(
                    localNode,
                    connectionId,
                    descriptorProvider,
                    bootstrapFactory.handshakeEventLoopSwitcher(),
                    staleIdDetector,
                    clusterIdSupplier,
                    this,
                    stopping::get,
                    productVersionSource
            );
        }

        return initiatorHandshakeManagerFactory.create(
                localNode,
                connectionId,
                descriptorProvider
        );
    }

    private HandshakeManager createAcceptorHandshakeManager() {
        // Do not just use localNodeFuture.join() to make sure the wait is time-limited.
        waitForLocalNodeToBeSet();

        return new RecoveryAcceptorHandshakeManager(
                localNodeFuture.join(),
                FACTORY,
                descriptorProvider,
                bootstrapFactory.handshakeEventLoopSwitcher(),
                staleIdDetector,
                clusterIdSupplier,
                this,
                stopping::get,
                productVersionSource
        );
    }

    private void waitForLocalNodeToBeSet() {
        try {
            localNodeFuture.get(10, SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new RuntimeException("Interrupted while waiting for local node to be set", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException("Could not finish awaiting for local node", e);
        }
    }

    /**
     * Returns connection manager's {@link #server}.
     *
     * @return Connection manager's {@link #server}.
     */
    @TestOnly
    public NettyServer server() {
        return server;
    }

    @TestOnly
    public SerializationService serializationService() {
        return serializationService;
    }

    /**
     * Returns this node's id.
     *
     * @return This node's id.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * Returns collection of all the clients started by this connection manager.
     *
     * @return Collection of all the clients started by this connection manager.
     */
    @TestOnly
    public Collection<NettyClient> clients() {
        return Collections.unmodifiableCollection(clients.values());
    }

    /**
     * Returns collection of all channels of this connection manager.
     *
     * @return Collection of all channels of this connection manager.
     */
    @TestOnly
    public Map<ConnectorKey<UUID>, NettySender> channels() {
        return Map.copyOf(channels);
    }

    /**
     * Marks this connection manager as being stopped. In this state, it does not make any new connections, does not accept any connections
     * and does not consider handshake rejections as critical events.
     */
    public void initiateStopping() {
        stopping.set(true);
    }

    /**
     * Closes physical connections with an Ignite node identified by the given ID (it's not consistentId,
     * it's ID that gets regenerated at each node restart) and recovery descriptors corresponding to it.
     *
     * @param id ID of the node (it must have already left the topology).
     * @return Future that completes when all the channels and descriptors are closed.
     */
    public CompletableFuture<Void> handleNodeLeft(UUID id) {
        // We rely on the fact that the node with the given ID has already left the physical topology.
        assert staleIdDetector.isIdStale(id) : id + " is not stale yet";

        CompletableFuture<Void> future = new CompletableFuture<>();

        // TODO: IGNITE-21207 - remove descriptors for good.

        connectionMaintenanceExecutor.execute(
                () -> closeChannelsWith(id).whenCompleteAsync((res, ex) -> {
                    // Closing descriptors separately (as some of them might not have an operating channel attached, but they
                    // still might have unacknowledged messages/futures).
                    disposeRecoveryDescriptorsOfLeftNode(id);

                    future.complete(null);
                }, connectionMaintenanceExecutor)
        );

        return future;
    }

    private CompletableFuture<Void> closeChannelsWith(UUID id) {
        List<Entry<ConnectorKey<UUID>, NettySender>> entriesToRemove = channels.entrySet().stream()
                .filter(entry -> entry.getValue().launchId().equals(id))
                .collect(toList());

        List<CompletableFuture<Void>> closeFutures = new ArrayList<>();
        for (Entry<ConnectorKey<UUID>, NettySender> entry : entriesToRemove) {
            closeFutures.add(entry.getValue().closeAsync());

            channels.remove(entry.getKey());
        }

        return allOf(closeFutures.toArray(CompletableFuture[]::new));
    }

    private void disposeRecoveryDescriptorsOfLeftNode(UUID id) {
        Exception exceptionToFailSendFutures = new RecipientLeftException("Recipient left [id=" + id + "]");

        for (RecoveryDescriptor descriptor : descriptorProvider.getRecoveryDescriptorsByLaunchId(id)) {
            blockAndDisposeDescriptor(descriptor, exceptionToFailSendFutures);
        }
    }

    private CompletableFuture<Void> blockAndDisposeDescriptor(RecoveryDescriptor descriptor, Exception exceptionToFailSendFutures) {
        while (!descriptor.tryBlockForever(exceptionToFailSendFutures)) {
            if (descriptor.isBlockedForever()) {
                // Already blocked concurrently, nothing to do here, the one who blocked it will handle the disposal (or already did).
                return nullCompletedFuture();
            }

            DescriptorAcquiry acquiry = descriptor.holder();
            if (acquiry == null) {
                // The descriptor was acquired when we tried to block it, but now it was released. Let's try blocking it again.
                continue;
            }

            // Could not block, someone did not release the descriptor yet (but will do soon). Close their channel (just in case)
            // and try again.
            Channel channel = acquiry.channel();
            assert channel != null;

            return NettyUtils.toCompletableFuture(channel.close())
                    .handleAsync(
                            (res, e) -> blockAndDisposeDescriptor(descriptor, exceptionToFailSendFutures),
                            connectionMaintenanceExecutor
                    )
                    .thenCompose(identity());
        }

        // Now we hold the acquiry, so noone else can touch the unacknowledged queue and there is happens-before between all
        // previous operations with this queue and our current actions.
        descriptor.dispose(exceptionToFailSendFutures);

        return nullCompletedFuture();
    }

    /**
     * Sets the local node. Only after this this manager becomes able to accept incoming connections.
     */
    public void setLocalNode(InternalClusterNode localNode) {
        localNodeFuture.complete(localNode);
    }
}
