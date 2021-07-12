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

package org.apache.ignite.client.internal.io.netty;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import org.apache.ignite.client.internal.io.ClientConnection;
import org.apache.ignite.client.internal.io.ClientConnectionStateHandler;
import org.apache.ignite.client.internal.io.ClientMessageHandler;
import org.apache.ignite.lang.IgniteException;

import java.io.IOException;
import java.nio.ByteBuffer;

public class NettyClientConnection implements ClientConnection {
    public static final AttributeKey<NettyClientConnection> ATTR_CONN = AttributeKey.newInstance("CONN");

    private final Channel channel;
    private final ClientMessageHandler msgHnd;
    private final ClientConnectionStateHandler stateHnd;

    public NettyClientConnection(Channel channel, ClientMessageHandler msgHnd, ClientConnectionStateHandler stateHnd) {
        this.channel = channel;
        this.msgHnd = msgHnd;
        this.stateHnd = stateHnd;

        channel.attr(ATTR_CONN).set(this);
    }

    @Override public void send(ByteBuffer msg) throws IgniteException {
        channel.writeAndFlush(msg);
    }

    @Override public void close() {
        channel.close();
    }

    public void onMessage(ByteBuffer buf) throws IOException {
        msgHnd.onMessage(buf);
    }
}
