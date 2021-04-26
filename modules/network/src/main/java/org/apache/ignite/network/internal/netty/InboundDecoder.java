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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.internal.MessageReader;
import org.apache.ignite.network.internal.direct.DirectMessageReader;
import org.apache.ignite.network.message.MessageDeserializer;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.apache.ignite.network.message.NetworkMessage;

/**
 * Decodes {@link ByteBuf}s into {@link NetworkMessage}s.
 */
public class InboundDecoder extends ByteToMessageDecoder {
    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(InboundDecoder.class);

    /** Message reader channel attribute key. */
    private static final AttributeKey<MessageReader> READER_KEY = AttributeKey.valueOf("READER");

    /** Message deserializer channel attribute key. */
    private static final AttributeKey<MessageDeserializer<NetworkMessage>> DESERIALIZER_KEY = AttributeKey.valueOf("DESERIALIZER");

    /** Serialization registry. */
    private final MessageSerializationRegistry serializationRegistry;

    /**
     * Constructor.
     *
     * @param serializationRegistry Serialization registry.
     */
    public InboundDecoder(MessageSerializationRegistry serializationRegistry) {
        this.serializationRegistry = serializationRegistry;
    }

    /** {@inheritDoc} */
    @Override public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        ByteBuffer buffer = in.nioBuffer();

        Attribute<MessageReader> readerAttr = ctx.attr(READER_KEY);
        MessageReader reader = readerAttr.get();

        if (reader == null) {
            reader = new DirectMessageReader(serializationRegistry, ConnectionManager.DIRECT_PROTOCOL_VERSION);
            readerAttr.set(reader);
        }

        Attribute<MessageDeserializer<NetworkMessage>> messageAttr = ctx.attr(DESERIALIZER_KEY);

        while (buffer.hasRemaining()) {
            MessageDeserializer<NetworkMessage> msg = messageAttr.get();

            try {
                // Read message type.
                if (msg == null && buffer.remaining() >= NetworkMessage.DIRECT_TYPE_SIZE) {
                    byte b0 = buffer.get();
                    byte b1 = buffer.get();

                    msg = serializationRegistry.createDeserializer(makeMessageType(b0, b1));
                }

                boolean finished = false;

                // Read message if buffer has remaining data.
                if (msg != null && buffer.hasRemaining()) {
                    reader.setCurrentReadClass(msg.klass());
                    reader.setBuffer(buffer);

                    finished = msg.readMessage(reader);
                }

                // Set read position to Netty's ByteBuf.
                in.readerIndex(buffer.position());

                if (finished) {
                    reader.reset();
                    messageAttr.set(null);

                    out.add(msg.getMessage());
                }
                else
                    messageAttr.set(msg);
            }
            catch (Throwable e) {
                LOG.error(
                    String.format(
                        "Failed to read message [msg=%s, buf=%s, reader=%s]: %s",
                        msg, buffer, reader, e.getMessage()
                    ),
                    e
                );

                throw e;
            }
        }
    }

    /**
     * Concatenates the two parameter bytes to form a message type value.
     *
     * @param b0 The first byte.
     * @param b1 The second byte.
     */
    private static short makeMessageType(byte b0, byte b1) {
        return (short)((b1 & 0xFF) << 8 | b0 & 0xFF);
    }
}
