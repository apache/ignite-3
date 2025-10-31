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

import io.netty.channel.Channel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.stream.ChunkedInput;
import java.nio.channels.SocketChannel;
import org.apache.ignite.internal.network.serialization.MessageWriter;

public class NioSocketChannelEx extends NioSocketChannel {
    private MessageWriter messageWriter;
    private ChunkedInput<?> chunkedInput;

    public NioSocketChannelEx() {
    }

    public NioSocketChannelEx(Channel parent, SocketChannel socket) {
        super(parent, socket);
    }

    public ChunkedInput<?> getChunkedInput() {
        return chunkedInput;
    }

    public void setChunkedInput(ChunkedInput<?> chunkedInput) {
        this.chunkedInput = chunkedInput;
    }

    public MessageWriter getMessageWriter() {
        return messageWriter;
    }

    public void setMessageWriter(MessageWriter messageWriter) {
        this.messageWriter = messageWriter;
    }
}
