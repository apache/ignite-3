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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.stream.ChunkedInput;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.OutNetworkObject;
import org.apache.ignite.internal.network.direct.DirectMessageWriter;
import org.apache.ignite.internal.network.message.ClassDescriptorListMessage;
import org.apache.ignite.internal.network.message.ClassDescriptorMessage;
import org.apache.ignite.internal.network.serialization.MessageSerializer;
import org.apache.ignite.internal.network.serialization.MessageWriter;
import org.apache.ignite.internal.network.serialization.PerSessionSerializationService;
import org.jetbrains.annotations.Nullable;

/**
 * An encoder for the outbound messages that uses {@link DirectMessageWriter}.
 */
public class OutboundEncoder extends MessageToMessageEncoder<OutNetworkObject> {
    /** Handler name. */
    public static final String NAME = "outbound-encoder";

    private static final int IO_BUFFER_CAPACITY = 4 * 1024;

    /** Max number of messages in a single chunk. */
    private static final int MAX_MESSAGES_IN_CHUNK = 128;

    private static final NetworkMessagesFactory MSG_FACTORY = new NetworkMessagesFactory();

    /** Message writer channel attribute key. */
    private static final AttributeKey<MessageWriter> WRITER_KEY = AttributeKey.valueOf("WRITER");

    /** Last added chunk channel attribute key. */
    private static final AttributeKey<NetworkMessageChunkedInput> CHUNK_KEY = AttributeKey.valueOf("CHUNK");

    /** Serialization registry. */
    private final PerSessionSerializationService serializationService;

    /**
     * Constructor.
     *
     * @param serializationService Serialization service.
     */
    public OutboundEncoder(PerSessionSerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, OutNetworkObject msg, List<Object> out) throws Exception {
        Channel channel = ctx.channel();

        MessageWriter writer = getOrInitWriter(channel.attr(WRITER_KEY));

        appendMessage(msg, out, channel, writer);
    }

    /**
     * Lazy message writer creator.
     */
    private MessageWriter getOrInitWriter(Attribute<MessageWriter> writerAttr) {
        MessageWriter writer = writerAttr.get();

        if (writer == null) {
            writer = new DirectMessageWriter(serializationService.serializationRegistry(), ConnectionManager.DIRECT_PROTOCOL_VERSION);

            writerAttr.set(writer);
        }

        return writer;
    }

    /**
     * Adds new message to the latest chunk if that's possible. Appends it to output list if it's not possible.
     */
    private void appendMessage(OutNetworkObject msg, List<Object> out, Channel channel, MessageWriter writer) {
        Attribute<NetworkMessageChunkedInput> chunkAttr = channel.attr(CHUNK_KEY);
        NetworkMessageChunkedInput chunkedInput = chunkAttr.get();

        if (chunkedInput == null) {
            appendNewChunkedInput(msg, out, writer, chunkAttr);
        } else {
            if (chunkedInput.finished || chunkedInput.size >= MAX_MESSAGES_IN_CHUNK) {
                appendNewChunkedInput(msg, out, writer, chunkAttr);
            } else {
                chunkedInput.append(msg);
            }
        }
    }

    private void appendNewChunkedInput(
            OutNetworkObject msg,
            List<Object> out,
            MessageWriter writer,
            Attribute<NetworkMessageChunkedInput> chunkAttr
    ) {
        NetworkMessageChunkedInput chunkedInput = new NetworkMessageChunkedInput(msg, serializationService, writer);

        chunkAttr.set(chunkedInput);
        out.add(chunkedInput);
    }

    /**
     * Chunked input for network message.
     */
    private static class NetworkMessageChunkedInput implements ChunkedInput<ByteBuf> {
        OutNetworkObject[] messages;
        int size;
        boolean finished;

        /** Network message. */
        private @Nullable NetworkMessage msg;

        /** Class descriptors.*/
        private @Nullable ClassDescriptorListMessage descriptors;

        /** Message serializer. */
        private @Nullable MessageSerializer<NetworkMessage> serializer;

        /** Descriptors message serializer. */
        private @Nullable MessageSerializer<ClassDescriptorListMessage> descriptorSerializer;

        /** Serialization service, attached to the channel. */
        private final PerSessionSerializationService serializationService;

        /** Message writer. */
        private final MessageWriter writer;

        /** Whether the message was fully written. */
        private int currentMessageIndex = 0;

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
            this.writer = writer;

            messages = new OutNetworkObject[8];
            messages[0] = outObject;
            size = 1;

            prepareMessage();
        }

        private void prepareMessage() {
            OutNetworkObject outObject = messages[currentMessageIndex];
            messages[currentMessageIndex] = null;

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
            }

            this.serializer = serializationService.createMessageSerializer(msg.groupType(), msg.messageType());
        }

        private void cleanupMessage() {
            // Help GC by not holding any of those references anymore.
            this.msg = null;
            this.serializer = null;

            this.descriptors = null;
            this.descriptorSerializer = null;
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

            writeMessages();

            buffer.writerIndex(byteBuffer.position() - initialPosition);

            // Do not hold a reference, might help GC to do its job better.
            writer.setBuffer(EMPTY_BYTE_BUFFER);

            return buffer;
        }

        /**
         * Tries to write as many messages from {@link #state} as possible, until either buffer exhaustion or messages exhaustion.
         */
        private void writeMessages() {
            while (true) {
                if (descriptors != null) {
                    if (!descriptorSerializer.writeMessage(descriptors, writer)) {
                        return;
                    }

                    descriptors = null;

                    for (ClassDescriptorMessage classDescriptorMessage : descriptors.messages()) {
                        serializationService.addSentDescriptor(classDescriptorMessage.descriptorId());
                    }

                    writer.reset();
                }

                if (!serializer.writeMessage(msg, writer)) {
                    return;
                }

                writer.reset();

                cleanupMessage();
                currentMessageIndex++;

                if (currentMessageIndex < size) {
                    prepareMessage();

                    break;
                } else {
                    finished = true;

                    return;
                }
            }
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

        void append(OutNetworkObject msg) {
            assert size < MAX_MESSAGES_IN_CHUNK : "ChunkState size should be less than " + MAX_MESSAGES_IN_CHUNK + ", but was " + size;

            if (size >= messages.length) {
                messages = Arrays.copyOf(messages, Math.min(MAX_MESSAGES_IN_CHUNK, size + (size >> 1)));
            }

            messages[size] = msg;

            size++;
        }
    }
}
