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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.stream.ChunkedInput;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.network.internal.direct.DirectMessageWriter;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.apache.ignite.network.message.MessageSerializer;
import org.apache.ignite.network.message.NetworkMessage;

/**
 * Wrapper for a Netty {@link Channel}, that uses {@link ChunkedInput} and {@link DirectMessageWriter} to send data.
 */
public class NettySender {
    /** Netty channel. */
    private final SocketChannel channel;

    /** Serialization registry. */
    private final MessageSerializationRegistry serializationRegistry;

    /**
     * Constructor.
     *
     * @param channel Netty channel.
     * @param registry Serialization registry.
     */
    public NettySender(SocketChannel channel, MessageSerializationRegistry registry) {
        this.channel = channel;
        serializationRegistry = registry;
    }

    /**
     * Send message.
     *
     * @param msg Network message.
     */
    public CompletableFuture<Void> send(NetworkMessage msg) {
        MessageSerializer<NetworkMessage> serializer = serializationRegistry.createSerializer(msg.directType());

        ChannelFuture future = channel.writeAndFlush(new NetworkMessageChunkedInput(msg, serializer));

        CompletableFuture<Void> fut = new CompletableFuture<>();

        future.addListener(sent -> {
           if (sent.isSuccess())
               fut.complete(null);
           else
               fut.completeExceptionally(sent.cause());
        });

        return fut;
    }

    /**
     * Close channel.
     */
    public void close() {
        this.channel.close().awaitUninterruptibly();
    }

    /**
     * @return Gets the remote address of the channel.
     */
    public InetSocketAddress remoteAddress() {
        return this.channel.remoteAddress();
    }

    /**
     * @return {@code true} if channel is open, {@code false} otherwise.
     */
    public boolean isOpen() {
        return this.channel.isOpen();
    }

    /**
     * Chunked input for network message.
     */
    private static class NetworkMessageChunkedInput implements ChunkedInput<ByteBuf> {
        /** Network message. */
        private final NetworkMessage msg;

        /** Message serializer. */
        private final MessageSerializer<NetworkMessage> serializer;

        /** Message writer. */
        private final DirectMessageWriter writer = new DirectMessageWriter(ConnectionManager.DIRECT_PROTOCOL_VERSION);

        /** Whether the message was fully written. */
        boolean finished = false;

        /**
         * Constructor.
         *
         * @param msg Network message.
         * @param serializer Serializer.
         */
        private NetworkMessageChunkedInput(NetworkMessage msg, MessageSerializer<NetworkMessage> serializer) {
            this.msg = msg;
            this.serializer = serializer;
        }

        /** {@inheritDoc} */
        @Override public boolean isEndOfInput() throws Exception {
            return finished;
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {

        }

        /** {@inheritDoc} */
        @Deprecated
        @Override public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
            return readChunk(ctx.alloc());
        }

        /** {@inheritDoc} */
        @Override public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
            ByteBuf buffer = allocator.buffer(4096);
            ByteBuffer byteBuffer = buffer.internalNioBuffer(0, 4096);
            int initialPosition = byteBuffer.position();
            writer.setBuffer(byteBuffer);
            finished = serializer.writeMessage(msg, writer);
            buffer.writerIndex(byteBuffer.position() - initialPosition);
            return buffer;
        }

        /** {@inheritDoc} */
        @Override public long length() {
            // Return negative values, because object's size is unknown.
            return -1;
        }

        /** {@inheritDoc} */
        @Override public long progress() {
            // Not really needed, as there won't be listeners for the write operation's progress.
            return 0;
        }
    }
}
