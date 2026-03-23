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

import static java.util.Objects.requireNonNullElse;
import static org.apache.ignite.internal.network.netty.NettyUtils.toCompletableFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Network.BIND_ERR;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
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

    /** Server socket address. */
    private final InetSocketAddress bindAddress;

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
    private volatile ServerChannel serverChannel;

    /** Server close future. */
    @Nullable
    private CompletableFuture<Void> serverChannelCloseFuture;

    /** Flag indicating if {@link #stop()} has been called. */
    private boolean stopped;

    /** {@code null} if SSL is not {@link SslConfigurationSchema#enabled}. */
    private final @Nullable SslContext sslContext;

    /** Guarded by {@link #startStopLock}. */
    private final Set<SocketChannel> acceptedChannels = new HashSet<>();

    /**
     * Constructor.
     *
     * @param bindAddress Server socket address.
     * @param handshakeManager Handshake manager supplier.
     * @param messageListener Message listener.
     * @param serializationService Serialization service.
     * @param bootstrapFactory Netty bootstrap factory.
     * @param sslContext Server SSL context, {@code null} if SSL is not {@link SslConfigurationSchema#enabled}.
     */
    public NettyServer(
            InetSocketAddress bindAddress,
            Supplier<HandshakeManager> handshakeManager,
            Consumer<InNetworkObject> messageListener,
            SerializationService serializationService,
            NettyBootstrapFactory bootstrapFactory,
            @Nullable SslContext sslContext
    ) {
        this.bindAddress = bindAddress;
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
                    registerAcceptedChannelOrCloseIfStopped(ch);

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

            var bindFuture = new CompletableFuture<Channel>();

            bootstrap.bind(bindAddress).addListener((ChannelFuture future) -> {
                if (future.isSuccess()) {
                    bindFuture.complete(future.channel());
                } else if (future.isCancelled()) {
                    bindFuture.cancel(true);
                } else {
                    String errorMessage = String.format(
                            "Cannot start server at address=%s, port=%d",
                            bindAddress.getHostString(), bindAddress.getPort()
                    );

                    bindFuture.completeExceptionally(new IgniteException(BIND_ERR, errorMessage, future.cause()));
                }
            });

            serverStartFuture = bindFuture
                    .handle((channel, err) -> {
                        synchronized (startStopLock) {
                            if (channel != null) {
                                serverChannelCloseFuture = toCompletableFuture(channel.closeFuture());
                            }

                            this.serverChannel = (ServerChannel) channel;

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

    private void registerAcceptedChannelOrCloseIfStopped(SocketChannel ch) {
        synchronized (startStopLock) {
            if (stopped) {
                ch.close();
            } else {
                acceptedChannels.add(ch);

                ch.closeFuture().addListener((ChannelFutureListener) future -> {
                    synchronized (startStopLock) {
                        acceptedChannels.remove(ch);
                    }
                });
            }
        }
    }

    /**
     * Returns address to which the server is bound (might be an 'any local'/wildcard address if bound to all interfaces).
     *
     * @return Gets the local address of the server.
     */
    public SocketAddress address() {
        return Objects.requireNonNull(serverChannel, "Not started yet").localAddress();
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

            return serverStartFuture
                    .handle((unused, throwable) -> {
                        synchronized (startStopLock) {
                            ServerChannel localServerChannel = serverChannel;
                            if (localServerChannel != null) {
                                localServerChannel.close();
                            }

                            return requireNonNullElse(serverChannelCloseFuture, CompletableFutures.<Void>nullCompletedFuture());
                        }
                    })
                    .thenCompose(Function.identity())
                    .thenCompose(unused -> {
                        synchronized (startStopLock) {
                            List<CompletableFuture<Void>> closeFutures = new ArrayList<>();

                            for (Channel acceptedChannel : new HashSet<>(acceptedChannels)) {
                                closeFutures.add(toCompletableFuture(acceptedChannel.close()));
                            }

                            return CompletableFutures.allOf(closeFutures);
                        }
                    });
        }
    }

    /**
     * Returns {@code true} if the server is running, {@code false} otherwise.
     *
     * @return {@code true} if the server is running, {@code false} otherwise.
     */
    @TestOnly
    public boolean isRunning() {
        ServerChannel channel0 = serverChannel;

        return channel0 != null && channel0.isOpen();
    }

    @TestOnly
    boolean hasAcceptedChannels() {
        synchronized (startStopLock) {
            return !acceptedChannels.isEmpty();
        }
    }
}
