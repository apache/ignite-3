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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.AttributeKey;
import java.net.InetSocketAddress;
import org.apache.ignite.internal.client.ClientMetricSource;
import org.apache.ignite.internal.client.io.ClientConnection;
import org.apache.ignite.internal.client.io.ClientConnectionStateHandler;
import org.apache.ignite.internal.client.io.ClientMessageHandler;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Netty client connection.
 */
public class NettyClientConnection implements ClientConnection {
    /** Connection attribute. */
    static final AttributeKey<NettyClientConnection> ATTR_CONN = AttributeKey.newInstance("CONN");

    /** Target address. */
    private final InetSocketAddress addr;

    /** Channel. */
    private final Channel channel;

    /** Message handler. */
    private final ClientMessageHandler msgHnd;

    /** State handler. */
    private final ClientConnectionStateHandler stateHnd;

    /** Metrics. */
    private final ClientMetricSource metrics;

    /**
     * Constructor.
     *
     * @param addr Target address.
     * @param channel Channel.
     * @param msgHnd Message handler.
     * @param stateHnd State handler.
     * @param metrics Metrics.
     */
    NettyClientConnection(
            InetSocketAddress addr,
            Channel channel,
            ClientMessageHandler msgHnd,
            ClientConnectionStateHandler stateHnd,
            ClientMetricSource metrics) {
        this.addr = addr;
        this.channel = channel;
        this.msgHnd = msgHnd;
        this.stateHnd = stateHnd;
        this.metrics = metrics;

        //noinspection ThisEscapedInObjectConstruction
        channel.attr(ATTR_CONN).set(this);
    }

    /** {@inheritDoc} */
    @Override
    public ChannelFuture send(ByteBuf msg) throws IgniteException {
        int bytes = msg.readableBytes();

        // writeAndFlush releases pooled buffer.
        ChannelFuture fut = channel.writeAndFlush(msg);

        metrics.bytesSentAdd(bytes);

        return fut;
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuf getBuffer() {
        return channel.alloc().buffer();
    }

    /** {@inheritDoc} */
    @Override
    public InetSocketAddress remoteAddress() {
        return addr;
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        channel.close();
    }

    /**
     * Handles incoming message.
     *
     * @param buf Message.
     */
    void onMessage(ByteBuf buf) {
        metrics.bytesReceivedAdd(buf.readableBytes());

        msgHnd.onMessage(buf);
    }

    /**
     * Handles disconnect.
     *
     * @param e Exception that caused the disconnect.
     */
    void onDisconnected(@Nullable Throwable e) {
        stateHnd.onDisconnected(e);
    }
}
