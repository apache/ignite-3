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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.message.ClassDescriptorListMessage;
import org.apache.ignite.internal.network.serialization.MessageDeserializer;
import org.apache.ignite.internal.network.serialization.MessageFormat;
import org.apache.ignite.internal.network.serialization.MessageReader;
import org.apache.ignite.internal.network.serialization.PerSessionSerializationService;

/**
 * Decodes {@link ByteBuf}s into {@link NetworkMessage}s.
 */
public class InboundDecoder extends ByteToMessageDecoder {
    /** Handler name. */
    public static final String NAME = "inbound-decoder";

    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(InboundDecoder.class);

    /** Message reader channel attribute key. */
    private static final AttributeKey<MessageReader> READER_KEY = AttributeKey.valueOf("READER");

    /** Message deserializer channel attribute key. */
    private static final AttributeKey<MessageDeserializer<NetworkMessage>> DESERIALIZER_KEY = AttributeKey.valueOf("DESERIALIZER");

    /** Message group type, for partially read message headers. */
    private static final AttributeKey<Short> GROUP_TYPE_KEY = AttributeKey.valueOf("GROUP_TYPE");

    private final MessageFormat messageFormat;

    /** Serialization service. */
    private final PerSessionSerializationService serializationService;

    /**
     * Constructor.
     *
     * @param serializationService Serialization service.
     */
    public InboundDecoder(MessageFormat messageFormat, PerSessionSerializationService serializationService) {
        this.messageFormat = messageFormat;
        this.serializationService = serializationService;
    }

    /** {@inheritDoc} */
    @Override
    public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        ByteBuffer buffer = in.nioBuffer();

        Attribute<MessageReader> readerAttr = ctx.channel().attr(READER_KEY);
        MessageReader reader = readerAttr.get();

        if (reader == null) {
            reader = messageFormat.reader(serializationService.serializationRegistry(), ConnectionManager.DIRECT_PROTOCOL_VERSION);
            readerAttr.set(reader);
        }

        Attribute<MessageDeserializer<NetworkMessage>> deserializerAttr = ctx.channel().attr(DESERIALIZER_KEY);

        Attribute<Short> groupTypeAttr = ctx.channel().attr(GROUP_TYPE_KEY);

        reader.setBuffer(buffer);

        while (buffer.hasRemaining()) {
            int initialNioBufferPosition = buffer.position();

            MessageDeserializer<NetworkMessage> deserializer = deserializerAttr.get();

            try {
                // Read message type.
                if (deserializer == null) {
                    Short groupType = groupTypeAttr.get();

                    if (groupType == null) {
                        groupType = reader.readHeaderShort();

                        if (!reader.isLastRead()) {
                            fixNettyBufferReaderIndex(in, buffer, initialNioBufferPosition);

                            break;
                        }
                    }

                    short messageType = reader.readHeaderShort();

                    if (!reader.isLastRead()) {
                        groupTypeAttr.set(groupType);

                        fixNettyBufferReaderIndex(in, buffer, initialNioBufferPosition);

                        break;
                    }

                    deserializer = serializationService.createMessageDeserializer(groupType, messageType);

                    groupTypeAttr.set(null);
                }

                boolean finished = false;

                // Read message if buffer has remaining data.
                if (deserializer != null && buffer.hasRemaining()) {
                    reader.setCurrentReadClass(deserializer.klass());

                    finished = deserializer.readMessage(reader);
                }

                int readBytes = fixNettyBufferReaderIndex(in, buffer, initialNioBufferPosition);

                if (finished) {
                    reader.reset();
                    deserializerAttr.set(null);

                    NetworkMessage message = deserializer.getMessage();

                    if (message instanceof ClassDescriptorListMessage) {
                        onClassDescriptorMessage((ClassDescriptorListMessage) message);
                    } else {
                        out.add(message);
                    }
                } else {
                    deserializerAttr.set(deserializer);
                }

                if (readBytes == 0) {
                    // Zero bytes were read from the buffer.
                    // This could've happened because for some types (like floats) we have to read N bytes at a time and there could be
                    // less than N bytes available.
                    break;
                }
            } catch (Throwable e) {
                LOG.debug("Failed to read message [deserializer={}, buf={}, reader={}, reason={}]",
                                e, deserializer, buffer, reader, e.getMessage()
                );

                throw e;
            }
        }
    }

    private static int fixNettyBufferReaderIndex(ByteBuf in, ByteBuffer buffer, int initialNioBufferPosition) {
        int readBytes = buffer.position() - initialNioBufferPosition;

        // Set read position to Netty's ByteBuf.
        in.readerIndex(in.readerIndex() + readBytes);

        return readBytes;
    }

    private void onClassDescriptorMessage(ClassDescriptorListMessage msg) {
        serializationService.mergeDescriptors(msg.messages());
    }
}
