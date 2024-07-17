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

package org.apache.ignite.internal.network.processor;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.ignite.internal.network.messages.InheritedMessage;
import org.apache.ignite.internal.network.messages.InheritedMessageSerializationFactory;
import org.apache.ignite.internal.network.messages.TestMessagesFactory;
import org.apache.ignite.internal.network.serialization.MessageDeserializer;
import org.apache.ignite.internal.network.serialization.MessageReader;
import org.apache.ignite.internal.network.serialization.MessageSerializer;
import org.apache.ignite.internal.network.serialization.MessageWriter;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Tests for support of network message inheritance hierarchies.
 *
 * @see InheritedMessage
 */
public class InheritedMessageTest extends BaseIgniteAbstractTest {
    private final TestMessagesFactory messageFactory = new TestMessagesFactory();

    private final InheritedMessageSerializationFactory serializationFactory =
            new InheritedMessageSerializationFactory(messageFactory);

    /**
     * Tests that the generated message implementation contains all fields from the superinterfaces and is serialized in the correct order.
     */
    @Test
    void testSerialization() {
        InheritedMessage msg = messageFactory.inheritedMessage()
                .intX(1).intY(2).intZ(3)
                .build();

        MessageSerializer<InheritedMessage> serializer = serializationFactory.createSerializer();

        var mockWriter = mock(MessageWriter.class);

        when(mockWriter.isHeaderWritten()).thenReturn(true);
        when(mockWriter.writeInt(anyString(), anyInt())).thenReturn(true);

        serializer.writeMessage(msg, mockWriter);

        InOrder inOrder = inOrder(mockWriter);

        inOrder.verify(mockWriter).writeInt(eq("intX"), eq(1));
        inOrder.verify(mockWriter).writeInt(eq("intY"), eq(2));
        inOrder.verify(mockWriter).writeInt(eq("intZ"), eq(3));
    }

    /**
     * Tests that the generated message implementation is deserialized in the correct order.
     */
    @Test
    void testDeserialization() {
        MessageDeserializer<InheritedMessage> deserializer = serializationFactory.createDeserializer();

        var mockReader = mock(MessageReader.class);

        when(mockReader.beforeMessageRead()).thenReturn(true);
        when(mockReader.isLastRead()).thenReturn(true);

        deserializer.readMessage(mockReader);

        InOrder inOrder = inOrder(mockReader);

        inOrder.verify(mockReader).readInt(eq("intX"));
        inOrder.verify(mockReader).readInt(eq("intY"));
        inOrder.verify(mockReader).readInt(eq("intZ"));
    }
}
