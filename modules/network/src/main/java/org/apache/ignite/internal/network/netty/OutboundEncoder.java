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

import static org.apache.ignite.internal.util.ArrayUtils.EMPTY_BYTE_BUFFER;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.stream.ChunkedInput;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.OutNetworkObject;
import org.apache.ignite.internal.network.message.ClassDescriptorListMessage;
import org.apache.ignite.internal.network.message.ClassDescriptorMessage;
import org.apache.ignite.internal.network.serialization.MessageFormat;
import org.apache.ignite.internal.network.serialization.MessageSerializer;
import org.apache.ignite.internal.network.serialization.MessageWriter;
import org.apache.ignite.internal.network.serialization.PerSessionSerializationService;

/**
 * An encoder for the outbound messages that uses the provided {@link MessageFormat}.
 */
public class OutboundEncoder extends MessageToMessageEncoder<OutNetworkObject> {
    /** Handler name. */
    public static final String NAME = "outbound-encoder";

    private static final int IO_BUFFER_CAPACITY = 16 * 1024;

    private static final NetworkMessagesFactory MSG_FACTORY = new NetworkMessagesFactory();

    /** Message writer channel attribute key. */
    private static final AttributeKey<MessageWriter> WRITER_KEY = AttributeKey.valueOf("WRITER");

    private final MessageFormat messageFormat;

    /** Serialization registry. */
    private final PerSessionSerializationService serializationService;

    /**
     * Constructor.
     *
     * @param serializationService Serialization service.
     */
    public OutboundEncoder(MessageFormat messageFormat, PerSessionSerializationService serializationService) {
        this.messageFormat = messageFormat;
        this.serializationService = serializationService;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, OutNetworkObject msg, List<Object> out) throws Exception {
        Attribute<MessageWriter> writerAttr = ctx.channel().attr(WRITER_KEY);
        MessageWriter writer = writerAttr.get();

        if (writer == null) {
            writer = messageFormat.writer(serializationService.serializationRegistry(), ConnectionManager.DIRECT_PROTOCOL_VERSION);

            writerAttr.set(writer);
        }

        out.add(new NetworkMessageChunkedInput(msg, serializationService, writer));
    }

    /**
     * Chunked input for network message.
     */
    private static class NetworkMessageChunkedInput implements ChunkedInput<ByteBuf> {
        /** Network message. */
        private final NetworkMessage msg;

        /** Message serializer. */
        private final MessageSerializer<NetworkMessage> serializer;

        private final MessageSerializer<ClassDescriptorListMessage> descriptorSerializer;

        /** Message writer. */
        private final MessageWriter writer;

        private final ClassDescriptorListMessage descriptors;
        private final PerSessionSerializationService serializationService;

        /** Whether the message was fully written. */
        private boolean finished = false;
        private boolean descriptorsFinished = false;

        /**
         * Constructor.
         *
         * @param outObject            Out network object.
         * @param serializationService Serialization service.
         */
        private NetworkMessageChunkedInput(
                OutNetworkObject outObject,
                PerSessionSerializationService serializationService,
                MessageWriter writer
        ) {
            this.serializationService = serializationService;
            this.msg = outObject.networkMessage();

            List<ClassDescriptorMessage> outDescriptors = null;
            List<ClassDescriptorMessage> outObjectDescriptors = outObject.descriptors();
            //noinspection ForLoopReplaceableByForEach
            for (int i = 0, descriptorsSize = outObjectDescriptors.size(); i < descriptorsSize; i++) {
                ClassDescriptorMessage classDescriptorMessage = outObjectDescriptors.get(i);
                if (!serializationService.isDescriptorSent(classDescriptorMessage.descriptorId())) {
                    if (outDescriptors == null) {
                        outDescriptors = new ArrayList<>(outObject.descriptors().size());
                    }
                    outDescriptors.add(classDescriptorMessage);
                }
            }

            if (outDescriptors != null) {
                this.descriptors = MSG_FACTORY.classDescriptorListMessage().messages(outDescriptors).build();
                short groupType = this.descriptors.groupType();
                short messageType = this.descriptors.messageType();
                descriptorSerializer = serializationService.createMessageSerializer(groupType, messageType);
            } else {
                this.descriptors = null;
                descriptorSerializer = null;
                descriptorsFinished = true;
            }

            this.serializer = serializationService.createMessageSerializer(msg.groupType(), msg.messageType());
            this.writer = writer;
        }

        @Override
        public boolean isEndOfInput() {
            return finished;
        }

        @Override
        public void close() {
            // No-op.
        }

        @Deprecated
        @Override
        public ByteBuf readChunk(ChannelHandlerContext ctx) {
            return readChunk(ctx.alloc());
        }

        @Override
        public ByteBuf readChunk(ByteBufAllocator allocator) {
            ByteBuf buffer = allocator.ioBuffer(IO_BUFFER_CAPACITY);
            int capacity = buffer.capacity();

            ByteBuffer byteBuffer = buffer.internalNioBuffer(0, capacity);

            int initialPosition = byteBuffer.position();

            writer.setBuffer(byteBuffer);

            while (byteBuffer.hasRemaining()) {
                if (!descriptorsFinished) {
                    descriptorsFinished = descriptorSerializer.writeMessage(descriptors, writer);
                    if (descriptorsFinished) {
                        for (ClassDescriptorMessage classDescriptorMessage : descriptors.messages()) {
                            serializationService.addSentDescriptor(classDescriptorMessage.descriptorId());
                        }
                        writer.reset();
                    } else {
                        break;
                    }
                } else {
                    finished = serializer.writeMessage(msg, writer);

                    if (finished) {
                        writer.reset();
                    }

                    break;
                }
            }

            buffer.writerIndex(byteBuffer.position() - initialPosition);

            // Do not hold a reference, might help GC to do its job better.
            writer.setBuffer(EMPTY_BYTE_BUFFER);

            return buffer;
        }

        @Override
        public long length() {
            // Return negative values, because object's size is unknown.
            return -1;
        }

        @Override
        public long progress() {
            // Not really needed, as there won't be listeners for the write operation's progress.
            return 0;
        }
    }
}
