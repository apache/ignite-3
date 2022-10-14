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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.internal.future.OrderingFuture;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.serialization.PerSessionSerializationService;
import org.apache.ignite.internal.network.serialization.SerializationService;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * Netty client channel wrapper.
 */
public class NettyClient {
    /** A lock for start and stop operations. */
    private final Object startStopLock = new Object();

    /** Serialization service. */
    private final SerializationService serializationService;

    /** Destination address. */
    private final SocketAddress address;

    /** Future that resolves when the client finished the handshake. */
    @Nullable
    private volatile OrderingFuture<NettySender> senderFuture = null;

    /** Future that resolves when the client channel is opened. */
    private final CompletableFuture<Void> channelFuture = new CompletableFuture<>();

    /** Client channel. */
    @Nullable
    private volatile Channel channel = null;

    /** Message listener. */
    private final Consumer<InNetworkObject> messageListener;

    /** Handshake manager. */
    private final HandshakeManager handshakeManager;

    /** Flag indicating if {@link #stop()} has been called. */
    private boolean stopped = false;

    /**
     * Constructor.
     *
     * @param address               Destination address.
     * @param serializationService  Serialization service.
     * @param manager               Client handshake manager.
     * @param messageListener       Message listener.
     */
    public NettyClient(
            SocketAddress address,
            SerializationService serializationService,
            HandshakeManager manager,
            Consumer<InNetworkObject> messageListener
    ) {
        this.address = address;
        this.serializationService = serializationService;
        this.handshakeManager = manager;
        this.messageListener = messageListener;
    }

    /**
     * Start client.
     *
     * @param bootstrapTemplate Template client bootstrap.
     * @return Future that resolves when client channel is opened.
     */
    public OrderingFuture<NettySender> start(Bootstrap bootstrapTemplate) {
        synchronized (startStopLock) {
            if (stopped) {
                throw new IgniteInternalException("Attempted to start an already stopped NettyClient");
            }

            if (senderFuture != null) {
                throw new IgniteInternalException("Attempted to start an already started NettyClient");
            }

            Bootstrap bootstrap = bootstrapTemplate.clone();

            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                /** {@inheritDoc} */
                @Override
                public void initChannel(SocketChannel ch) {
                    var sessionSerializationService = new PerSessionSerializationService(serializationService);

                    PipelineUtils.setup(ch.pipeline(), sessionSerializationService, handshakeManager, messageListener);
                }
            });

            CompletableFuture<NettySender> senderCompletableFuture = NettyUtils.toChannelCompletableFuture(bootstrap.connect(address))
                    .handle((channel, throwable) -> {
                        synchronized (startStopLock) {
                            this.channel = channel;

                            if (throwable != null) {
                                channelFuture.completeExceptionally(throwable);
                            } else {
                                channelFuture.complete(null);
                            }

                            if (stopped) {
                                return CompletableFuture.<NettySender>failedFuture(new CancellationException("Client was stopped"));
                            } else if (throwable != null) {
                                return CompletableFuture.<NettySender>failedFuture(throwable);
                            } else {
                                return handshakeManager.handshakeFuture();
                            }
                        }
                    })
                    .thenCompose(Function.identity());
            senderFuture = OrderingFuture.adapt(senderCompletableFuture);

            return senderFuture;
        }
    }

    /**
     * Returns client start future.
     *
     * @return Client start future.
     */
    public OrderingFuture<NettySender> sender() {
        Objects.requireNonNull(senderFuture, "NettyClient is not connected yet");

        return senderFuture;
    }

    /**
     * Stops the client.
     *
     * @return Future that is resolved when the client's channel has closed or an already completed future for a subsequent call.
     */
    public CompletableFuture<Void> stop() {
        synchronized (startStopLock) {
            if (stopped) {
                return CompletableFuture.completedFuture(null);
            }

            stopped = true;

            if (senderFuture == null) {
                return CompletableFuture.completedFuture(null);
            }

            return channelFuture
                    .handle((sender, throwable) ->
                            channel == null
                                    ? CompletableFuture.<Void>completedFuture(null) : NettyUtils.toCompletableFuture(channel.close()))
                    .thenCompose(Function.identity());
        }
    }

    /**
     * Returns {@code true} if the client has failed to connect to the remote server, {@code false} otherwise.
     *
     * @return {@code true} if the client has failed to connect to the remote server, {@code false} otherwise.
     */
    public boolean failedToConnect() {
        OrderingFuture<NettySender> currentFuture = senderFuture;

        return currentFuture != null && currentFuture.isCompletedExceptionally();
    }

    /**
     * Returns {@code true} if the client has lost the connection or has been stopped, {@code false} otherwise.
     *
     * @return {@code true} if the client has lost the connection or has been stopped, {@code false} otherwise.
     */
    public boolean isDisconnected() {
        Channel currentChannel = channel;

        return (currentChannel != null && !currentChannel.isOpen()) || stopped;
    }
}
