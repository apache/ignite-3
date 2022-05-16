/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.function.Consumer;
import org.apache.ignite.internal.network.handshake.HandshakeException;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.serialization.PerSessionSerializationService;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.NetworkMessage;

/**
 * Netty handler of the handshake operation.
 */
public class HandshakeHandler extends ChannelInboundHandlerAdapter {
    /** Handler name. */
    public static final String NAME = "handshake-handler";

    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(HandshakeHandler.class);

    /** Handshake manager. */
    private final HandshakeManager manager;

    private final Consumer<InNetworkObject> messageListener;

    private final PerSessionSerializationService serializationService;

    /**
     * Constructor.
     *
     * @param manager Handshake manager.
     * @param messageListener Message listener.
     * @param serializationService Serialization service.
     */
    public HandshakeHandler(HandshakeManager manager, Consumer<InNetworkObject> messageListener,
            PerSessionSerializationService serializationService) {
        this.manager = manager;
        this.messageListener = messageListener;
        this.serializationService = serializationService;
    }

    /** {@inheritDoc} */
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        manager.onInit(ctx);
    }

    /** {@inheritDoc} */
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        manager.onConnectionOpen();

        manager.handshakeFuture().whenComplete((unused, throwable) -> {
            if (throwable != null) {
                LOG.debug("Error when performing handshake", throwable);

                ctx.close();
            }
        });

        ctx.fireChannelActive();
    }

    /** {@inheritDoc} */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        manager.onMessage((NetworkMessage) msg);
    }

    /** {@inheritDoc} */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // If this method is called that means channel has been closed before handshake has finished or handshake
        // has failed.
        manager.handshakeFuture().completeExceptionally(
                new HandshakeException("Channel has been closed before handshake has finished or handshake has failed")
        );

        ctx.fireChannelInactive();
    }

    /** {@inheritDoc} */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        manager.handshakeFuture().completeExceptionally(cause);
    }

    /**
     * Creates a {@link MessageHandler} for the current pipeline.
     *
     * @param remoteConsistentId Remote node's consistent id.
     * @return Message handler.
     */
    public MessageHandler createMessageHandler(String remoteConsistentId) {
        return new MessageHandler(messageListener, remoteConsistentId, serializationService);
    }
}
