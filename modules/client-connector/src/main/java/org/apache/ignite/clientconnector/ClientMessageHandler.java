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

package org.apache.ignite.clientconnector;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;
import org.msgpack.core.MessagePack;

/**
 * https://netty.io/wiki/user-guide-for-4.x.html
 */
public class ClientMessageHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf inBuffer = (ByteBuf) msg;

        // TODO: Pooled unpacker.
        // TODO: Ensure no buffer copy.
        var unpacker = MessagePack.newDefaultUnpacker(inBuffer.nioBuffer());

        // TODO: Read full messages before processing.
        System.out.println("RCV: ");

        for (int i = 0; i < inBuffer.readableBytes(); i++) {
            System.out.print(inBuffer.getByte(i) + " ");
        }

        System.out.println();

        ctx.write(Unpooled.copiedBuffer("todo", CharsetUtil.UTF_8));
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // TODO: Logging
        cause.printStackTrace();
        ctx.close();
    }
}
