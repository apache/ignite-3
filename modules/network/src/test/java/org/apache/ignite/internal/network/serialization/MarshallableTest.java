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

package org.apache.ignite.internal.network.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.internal.network.direct.DirectMessageWriter;
import org.apache.ignite.internal.network.netty.ConnectionManager;
import org.apache.ignite.internal.network.netty.InboundDecoder;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.TestMessageSerializationRegistryImpl;
import org.apache.ignite.network.TestMessagesFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.network.serialization.MessageSerializer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests marshallable serialization.
 */
public class MarshallableTest {
    /** {@link ByteBuf} allocator. */
    private final UnpooledByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;

    /** Registry. */
    private final MessageSerializationRegistry registry = new TestMessageSerializationRegistryImpl();

    /**
     * Tests that marshallable object can be serialized along with it's descriptor.
     */
    @Test
    public void testMarshallable() {
        TestMessagesFactory msgFactory = new TestMessagesFactory();

        ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);

        var channel = new EmbeddedChannel();

        Mockito.doReturn(channel).when(ctx).channel();

        ClassDescriptorFactoryContext descriptorContext = new ClassDescriptorFactoryContext();
        ClassDescriptorFactory factory = new ClassDescriptorFactory(descriptorContext);

        // Creates descriptor for SimpleSerializableObject
        ClassDescriptor descriptor = factory.create(SimpleSerializableObject.class);

        // Stub implementation of the serializer, uses standard JDK serializable serialization to actually marshall an object
        UserObjectSerializer userObjectSerializer = new UserObjectSerializer() {
            @Override
            public <T> T read(Map<Integer, ClassDescriptor> descriptor, byte[] array) {
                try (ByteArrayInputStream bais = new ByteArrayInputStream(array); ObjectInputStream ois = new ObjectInputStream(bais)) {
                    return (T) ois.readObject();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public <T> SerializationResult write(T object) {
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                    oos.writeObject(object);
                    oos.close();
                    return new SerializationResult(baos.toByteArray(), Collections.singletonList(descriptor.descriptorId()));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public ClassDescriptor getClassDescriptor(int typeDescriptorId) {
                return descriptorContext.getDescriptor(typeDescriptorId);
            }

            @Override
            public ClassDescriptor getClassDescriptor(String typeName) {
                assertEquals(descriptor.className(), typeName);

                return descriptor;
            }

            @Override
            public boolean isBuiltin(int typeDescriptorId) {
                return ClassDescriptorFactoryContext.isBuiltin(typeDescriptorId);
            }
        };

        var serializationService = new SerializationService(registry, userObjectSerializer);
        var perSessionSerializationService = new PerSessionSerializationService(serializationService);
        final var decoder = new InboundDecoder(perSessionSerializationService);

        // List that holds decoded object
        final var list = new ArrayList<>();

        var writer = new DirectMessageWriter(perSessionSerializationService, ConnectionManager.DIRECT_PROTOCOL_VERSION);

        // Test map that will be sent as a Marshallable object within the MessageWithMarshallable message
        Map<String, SimpleSerializableObject> testMap = Map.of("test", new SimpleSerializableObject(10));

        MessageWithMarshallable msg = msgFactory.messageWithMarshallable().marshallableMap(testMap).build();

        MessageSerializer<NetworkMessage> serializer = registry.createSerializer(msg.groupType(), msg.messageType());

        ByteBuffer nioBuffer = ByteBuffer.allocate(10_000);

        writer.setBuffer(nioBuffer);

        // Write a message to the ByteBuffer.
        boolean fullyWritten = serializer.writeMessage(msg, writer);

        assertTrue(fullyWritten);

        int size = nioBuffer.position();

        nioBuffer.flip();

        ByteBuf buffer = allocator.buffer();

        for (int i = 0; i < size; i++) {
            // Write bytes to a decoding buffer one by one
            buffer.writeByte(nioBuffer.get());

            decoder.decode(ctx, buffer, list);

            if (i < size - 1) {
                // Any time before the buffer is fully read, message object should not be decoded
                assertEquals(0, list.size());
            }
        }

        // Buffer is fully read, message object should be decoded
        assertEquals(1, list.size());

        MessageWithMarshallable received = (MessageWithMarshallable) list.get(0);

        assertEquals(testMap, received.marshallableMap());

        // Check that the descriptor of the SimpleSerializableObject was received
        Map<Integer, ClassDescriptor> mergedDescriptors = perSessionSerializationService.getDescriptorMapView();
        assertEquals(1, mergedDescriptors.size());

        ClassDescriptor mergedDescriptor = mergedDescriptors.values().stream().findFirst().get();

        assertEquals(descriptor.className(), mergedDescriptor.className());
    }
}
