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

package org.apache.ignite.network.internal.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.internal.handshake.HandshakeAction;
import org.apache.ignite.network.internal.handshake.HandshakeException;
import org.apache.ignite.network.internal.handshake.HandshakeManager;

/**
 * Netty handler of the handshake operation.
 */
public class HandshakeHandler extends ChannelInboundHandlerAdapter {
    /** Handshake manager. */
    private final HandshakeManager manager;

    /**
     * Constructor.
     *
     * @param manager Handshake manager.
     */
    public HandshakeHandler(HandshakeManager manager) {
        this.manager = manager;
    }

    /** {@inheritDoc} */
    @Override public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        HandshakeAction handshakeAction = manager.init(ctx.channel());

        handleHandshakeAction(handshakeAction, ctx);
    }

    /** {@inheritDoc} */
    @Override public void channelActive(ChannelHandlerContext ctx) throws Exception {
        HandshakeAction handshakeAction = manager.onConnectionOpen(ctx.channel());

        handleHandshakeAction(handshakeAction, ctx);

        ctx.fireChannelActive();
    }

    /** {@inheritDoc} */
    @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        HandshakeAction handshakeAction = manager.onMessage(ctx.channel(), (NetworkMessage) msg);

        handleHandshakeAction(handshakeAction, ctx);
        // No need to forward the message to the next handler as this message only matters for a handshake.
    }

    /** {@inheritDoc} */
    @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // If this method is called that means channel has been closed before handshake has finished or handshake
        // has failed.
        manager.handshakeFuture().completeExceptionally(
            new HandshakeException("Channel has been closed before handshake has finished or handshake has failed")
        );

        ctx.fireChannelInactive();
    }

    /**
     * Handle {@link HandshakeAction}.
     *
     * @param action Handshake action.
     * @param ctx Netty channel context.
     */
    private void handleHandshakeAction(HandshakeAction action, ChannelHandlerContext ctx) {
        switch (action) {
            case REMOVE_HANDLER:
                ctx.pipeline().remove(this);
                break;

            case FAIL:
                ctx.channel().close();
                break;

            case NOOP:
                break;
        }
    }
}
