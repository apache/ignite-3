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

import static java.util.function.Function.identity;
import static org.apache.ignite.network.ChannelType.getChannel;

import io.netty.bootstrap.Bootstrap;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite.internal.future.OrderingFuture;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.configuration.NetworkView;
import org.apache.ignite.internal.network.handshake.ChannelAlreadyExistsException;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.recovery.RecoveryClientHandshakeManager;
import org.apache.ignite.internal.network.recovery.RecoveryClientHandshakeManagerFactory;
import org.apache.ignite.internal.network.recovery.RecoveryDescriptorProvider;
import org.apache.ignite.internal.network.recovery.RecoveryServerHandshakeManager;
import org.apache.ignite.internal.network.recovery.StaleIdDetector;
import org.apache.ignite.internal.network.serialization.SerializationService;
import org.apache.ignite.network.ChannelType;
import org.apache.ignite.network.NettyBootstrapFactory;
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

    /** Channels map from consistentId to {@link NettySender}. */
    private final Map<ConnectorKey<String>, NettySender> channels = new ConcurrentHashMap<>();

    /** Clients. */
    private final Map<ConnectorKey<InetSocketAddress>, NettyClient> clients = new ConcurrentHashMap<>();

    /** Serialization service. */
    private final SerializationService serializationService;

    /** Message listeners. */
    private final List<Consumer<InNetworkObject>> listeners = new CopyOnWriteArrayList<>();

    /** Node consistent id. */
    private final String consistentId;

    /** Node launch id. As opposed to {@link #consistentId}, this identifier changes between restarts. */
    private final UUID launchId;

    /** Used to detect that a peer uses a stale ID. */
    private final StaleIdDetector staleIdDetector;

    /** Factory producing {@link RecoveryClientHandshakeManager} instances. */
    private final @Nullable RecoveryClientHandshakeManagerFactory clientHandshakeManagerFactory;

    /** Start flag. */
    private final AtomicBoolean started = new AtomicBoolean(false);

    private final AtomicBoolean stopping = new AtomicBoolean(false);

    /** Stop flag. */
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    /** Recovery descriptor provider. */
    private final RecoveryDescriptorProvider descriptorProvider = new DefaultRecoveryDescriptorProvider();

    /** Network Configuration. */
    private final NetworkView networkConfiguration;

    /**
     * Constructor.
     *
     * @param networkConfiguration          Network configuration.
     * @param serializationService          Serialization service.
     * @param launchId                      Launch id of this node.
     * @param consistentId                  Consistent id of this node.
     * @param bootstrapFactory              Bootstrap factory.
     * @param staleIdDetector               Detects stale member IDs.
     */
    public ConnectionManager(
            NetworkView networkConfiguration,
            SerializationService serializationService,
            UUID launchId,
            String consistentId,
            NettyBootstrapFactory bootstrapFactory,
            StaleIdDetector staleIdDetector
    ) {
        this(
                networkConfiguration,
                serializationService,
                launchId,
                consistentId,
                bootstrapFactory,
                staleIdDetector,
                null
        );
    }

    /**
     * Constructor.
     *
     * @param networkConfiguration          Network configuration.
     * @param serializationService          Serialization service.
     * @param launchId                      Launch id of this node.
     * @param consistentId                  Consistent id of this node.
     * @param bootstrapFactory              Bootstrap factory.
     * @param staleIdDetector               Detects stale member IDs.
     * @param clientHandshakeManagerFactory Factory for {@link RecoveryClientHandshakeManager} instances.
     */
    public ConnectionManager(
            NetworkView networkConfiguration,
            SerializationService serializationService,
            UUID launchId,
            String consistentId,
            NettyBootstrapFactory bootstrapFactory,
            StaleIdDetector staleIdDetector,
            @Nullable RecoveryClientHandshakeManagerFactory clientHandshakeManagerFactory
    ) {
        this.serializationService = serializationService;
        this.launchId = launchId;
        this.consistentId = consistentId;
        this.staleIdDetector = staleIdDetector;
        this.clientHandshakeManagerFactory = clientHandshakeManagerFactory;
        this.networkConfiguration = networkConfiguration;

        this.server = new NettyServer(
                networkConfiguration,
                this::createServerHandshakeManager,
                this::onMessage,
                serializationService,
                bootstrapFactory
        );

        this.clientBootstrap = bootstrapFactory.createClientBootstrap();
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
            throw new IgniteInternalException("Failed to start the connection manager: " + cause.getMessage(), cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInternalException("Interrupted while starting the connection manager", e);
        }
    }

    /**
     * Returns server local address.
     *
     * @return Server local address.
     */
    public InetSocketAddress localAddress() {
        return (InetSocketAddress) server.address();
    }

    /**
     * Gets a {@link NettySender}, that sends data from this node to another node with the specified address.
     *
     * @param consistentId Another node's consistent id.
     * @param address      Another node's address.
     * @return Sender.
     */
    public OrderingFuture<NettySender> channel(@Nullable String consistentId, ChannelType type, InetSocketAddress address) {
        return getChannelWithRetry(consistentId, type, address, 0);
    }

    private OrderingFuture<NettySender> getChannelWithRetry(
            @Nullable String consistentId,
            ChannelType type,
            InetSocketAddress address,
            int attempt
    ) {
        if (attempt > MAX_RETRIES_TO_OPEN_CHANNEL) {
            return OrderingFuture.failedFuture(new IllegalStateException("Too many attempts to open channel to " + consistentId));
        }

        return doGetChannel(consistentId, type, address)
                .handle((res, ex) -> {
                    if (ex instanceof ChannelAlreadyExistsException) {
                        return getChannelWithRetry(((ChannelAlreadyExistsException) ex).consistentId(), type, address, attempt + 1);
                    }
                    if (ex != null && ex.getCause() instanceof ChannelAlreadyExistsException) {
                        return getChannelWithRetry(
                                ((ChannelAlreadyExistsException) ex.getCause()).consistentId(),
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
                        return getChannelWithRetry(res.consistentId(), type, address, attempt + 1);
                    }
                })
                .thenCompose(identity());
    }

    private OrderingFuture<NettySender> doGetChannel(
            @Nullable String consistentId,
            ChannelType type,
            InetSocketAddress address
    ) {
        // Problem is we can't look up a channel by consistent id because consistent id is not known yet.
        if (consistentId != null) {
            // If consistent id is known, try looking up a channel by consistent id. There can be an outbound connection
            // or an inbound connection associated with that consistent id.
            NettySender channel = channels.compute(
                    new ConnectorKey<>(consistentId, type),
                    (key, sender) -> (sender == null || !sender.isOpen()) ? null : sender
            );

            if (channel != null) {
                return OrderingFuture.completedFuture(channel);
            }
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
     * Callback that is called upon new client connected to the server.
     *
     * @param channel Channel from client to this {@link #server}.
     */
    @Override
    public void handshakeFinished(NettySender channel) {
        ConnectorKey<String> key = new ConnectorKey<>(channel.consistentId(), getChannel(channel.channelId()));
        NettySender oldChannel = channels.put(key, channel);

        // Old channel can still be in the map, but it must be closed already by the tie breaker in the
        // handshake manager.
        assert oldChannel == null || !oldChannel.isOpen() : "Incorrect channel creation flow";
    }

    /**
     * Create new client from this node to specified address.
     *
     * @param address Target address.
     * @return New netty client or {@code null} if we are stopping.
     */
    @Nullable
    private NettyClient connect(InetSocketAddress address, ChannelType channelType) {
        if (stopping.get()) {
            return null;
        }

        var client = new NettyClient(
                address,
                serializationService,
                createClientHandshakeManager(channelType.id()),
                this::onMessage,
                this.networkConfiguration.ssl()
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

        Stream<CompletableFuture<Void>> stream = Stream.concat(Stream.concat(
                    clients.values().stream().map(NettyClient::stop),
                    Stream.of(server.stop())
                ),
                channels.values().stream().map(NettySender::closeAsync)
        );

        CompletableFuture<Void> stopFut = CompletableFuture.allOf(stream.toArray(CompletableFuture<?>[]::new));

        try {
            stopFut.join();
        } catch (Exception e) {
            LOG.warn("Failed to stop connection manager [reason={}]", e.getMessage());
        }
    }

    /**
     * Returns {@code true} if the connection manager is stopped or is being stopped, {@code false} otherwise.
     *
     * @return {@code true} if the connection manager is stopped or is being stopped, {@code false} otherwise.
     */
    public boolean isStopped() {
        return stopped.get();
    }

    private HandshakeManager createClientHandshakeManager(short connectionId) {
        if (clientHandshakeManagerFactory == null) {
            return new RecoveryClientHandshakeManager(
                    launchId,
                    consistentId,
                    connectionId,
                    descriptorProvider,
                    staleIdDetector,
                    this,
                    stopping
            );
        }

        return clientHandshakeManagerFactory.create(
                launchId,
                consistentId,
                connectionId,
                descriptorProvider
        );
    }

    private HandshakeManager createServerHandshakeManager() {
        return new RecoveryServerHandshakeManager(
                launchId,
                consistentId,
                FACTORY,
                descriptorProvider,
                staleIdDetector,
                this,
                stopping
        );
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
     * Returns this node's consistent id.
     *
     * @return This node's consistent id.
     */
    public String consistentId() {
        return consistentId;
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
    public Map<ConnectorKey<String>, NettySender> channels() {
        return Map.copyOf(channels);
    }

    /**
     * Marks this connection manager as being stopped. In this state, it does not make any new connections, does not accept any connections
     * and does not consider handshake rejections as critical events.
     */
    public void initiateStopping() {
        stopping.set(true);
    }
}
