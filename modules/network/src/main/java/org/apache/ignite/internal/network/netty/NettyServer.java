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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Network.BIND_ERR;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import java.net.SocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.network.configuration.NetworkView;
import org.apache.ignite.internal.network.configuration.SslConfigurationSchema;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.serialization.PerSessionSerializationService;
import org.apache.ignite.internal.network.serialization.SerializationService;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Netty server channel wrapper.
 */
public class NettyServer {
    /** A lock for start and stop operations. */
    private final Object startStopLock = new Object();

    /** Bootstrap factory. */
    private final NettyBootstrapFactory bootstrapFactory;

    /** Server socket configuration. */
    private final NetworkView configuration;

    /** Serialization service. */
    private final SerializationService serializationService;

    /** Incoming message listener. */
    private final Consumer<InNetworkObject> messageListener;

    /** Handshake manager. */
    private final Supplier<HandshakeManager> handshakeManager;

    /** Server start future. */
    private CompletableFuture<Void> serverStartFuture;

    /** Server socket channel. */
    @Nullable
    private volatile ServerChannel channel;

    /** Server close future. */
    @Nullable
    private CompletableFuture<Void> serverCloseFuture;

    /** Flag indicating if {@link #stop()} has been called. */
    private boolean stopped;

    /** {@code null} if SSL is not {@link SslConfigurationSchema#enabled}. */
    private final @Nullable SslContext sslContext;

    /**
     * Constructor.
     *
     * @param configuration Server configuration.
     * @param handshakeManager Handshake manager supplier.
     * @param messageListener Message listener.
     * @param serializationService Serialization service.
     * @param bootstrapFactory Netty bootstrap factory.
     * @param sslContext Server SSL context, {@code null} if SSL is not {@link SslConfigurationSchema#enabled}.
     */
    public NettyServer(
            NetworkView configuration,
            Supplier<HandshakeManager> handshakeManager,
            Consumer<InNetworkObject> messageListener,
            SerializationService serializationService,
            NettyBootstrapFactory bootstrapFactory,
            @Nullable SslContext sslContext
    ) {
        this.configuration = configuration;
        this.handshakeManager = handshakeManager;
        this.messageListener = messageListener;
        this.serializationService = serializationService;
        this.bootstrapFactory = bootstrapFactory;
        this.sslContext = sslContext;
    }

    /**
     * Starts the server.
     *
     * @return Future that resolves when the server is successfully started.
     */
    public CompletableFuture<Void> start() {
        synchronized (startStopLock) {
            if (stopped) {
                throw new IgniteInternalException(INTERNAL_ERR, "Attempted to start an already stopped server");
            }

            if (serverStartFuture != null) {
                throw new IgniteInternalException(INTERNAL_ERR, "Attempted to start an already started server");
            }

            ServerBootstrap bootstrap = bootstrapFactory.createServerBootstrap();

            bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            var sessionSerializationService = new PerSessionSerializationService(serializationService);

                            // Get handshake manager for the new channel.
                            HandshakeManager manager = handshakeManager.get();

                            if (sslContext != null) {
                                PipelineUtils.setup(ch.pipeline(), sessionSerializationService, manager, messageListener, sslContext);
                            } else {
                                PipelineUtils.setup(ch.pipeline(), sessionSerializationService, manager, messageListener);
                            }
                        }
                    });

            int port = configuration.port();
            String[] addresses = configuration.listenAddresses();

            var bindFuture = new CompletableFuture<Channel>();

            ChannelFuture channelFuture;
            if (addresses.length == 0) {
                channelFuture = bootstrap.bind(port);
            } else {
                if (addresses.length > 1) {
                    // TODO: IGNITE-22369 - support more than one listen address.
                    throw new IgniteException(INTERNAL_ERR, "Only one listen address is allowed for now, but got " + List.of(addresses));
                }

                channelFuture = bootstrap.bind(addresses[0], port);
            }

            channelFuture.addListener((ChannelFuture future) -> {
                if (future.isSuccess()) {
                    bindFuture.complete(future.channel());
                } else if (future.isCancelled()) {
                    bindFuture.cancel(true);
                } else {
                    String address = addresses.length == 0 ? "" : addresses[0];
                    String errorMessage = "Cannot start server at address=" + address + ", port=" + port;
                    bindFuture.completeExceptionally(new IgniteException(BIND_ERR, errorMessage, future.cause()));
                }
            });

            serverStartFuture = bindFuture
                    .handle((channel, err) -> {
                        synchronized (startStopLock) {
                            if (channel != null) {
                                serverCloseFuture = NettyUtils.toCompletableFuture(channel.closeFuture());
                            }

                            this.channel = (ServerChannel) channel;

                            if (err != null || stopped) {
                                Throwable stopErr = err != null ? err : new CancellationException("Server was stopped");

                                return CompletableFuture.<Void>failedFuture(stopErr);
                            } else {
                                return CompletableFutures.<Void>nullCompletedFuture();
                            }
                        }
                    })
                    .thenCompose(Function.identity());

            return serverStartFuture;
        }
    }

    /**
     * Returns address to which the server is bound (might be an 'any local'/wildcard address if bound to all interfaces).
     *
     * @return Gets the local address of the server.
     */
    public SocketAddress address() {
        return Objects.requireNonNull(channel, "Not started yet").localAddress();
    }

    /**
     * Stops the server.
     *
     * @return Future that is resolved when the server's channel has closed or an already completed future for a subsequent call.
     */
    public CompletableFuture<Void> stop() {
        synchronized (startStopLock) {
            if (stopped) {
                return nullCompletedFuture();
            }

            stopped = true;

            if (serverStartFuture == null) {
                return nullCompletedFuture();
            }

            return serverStartFuture.handle((unused, throwable) -> {
                synchronized (startStopLock) {
                    ServerChannel localChannel = channel;
                    if (localChannel != null) {
                        localChannel.close();
                    }

                    return serverCloseFuture == null ? CompletableFutures.<Void>nullCompletedFuture() : serverCloseFuture;
                }
            }).thenCompose(Function.identity());
        }
    }

    /**
     * Returns {@code true} if the server is running, {@code false} otherwise.
     *
     * @return {@code true} if the server is running, {@code false} otherwise.
     */
    @TestOnly
    public boolean isRunning() {
        var channel0 = channel;

        return channel0 != null && channel0.isOpen();
    }
}
