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

package org.apache.ignite.internal.client.io.netty;

import static org.apache.ignite.internal.client.io.netty.NettyClientConnection.ATTR_CONN;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Netty client message handler.
 */
@SuppressWarnings("resource")
public class NettyClientMessageHandler extends ChannelInboundHandlerAdapter {
    /** {@inheritDoc} */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        connection(ctx).onMessage((ByteBuf) msg);
    }

    /** {@inheritDoc} */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        System.out.println(">>> INACTIVE: ");

        connection(ctx).onDisconnected(null);
    }

    /** {@inheritDoc} */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        System.out.println(">>> ERR: " + cause.getMessage());
        ctx.channel().close();
        connection(ctx).onDisconnected(cause); // TODO: Suppress channelInactive call?
    }

    private static NettyClientConnection connection(ChannelHandlerContext ctx) {
        return ctx.channel().attr(ATTR_CONN).get();
    }
}
