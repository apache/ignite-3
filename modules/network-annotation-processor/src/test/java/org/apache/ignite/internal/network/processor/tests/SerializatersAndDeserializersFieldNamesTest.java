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

package org.apache.ignite.internal.network.processor.tests;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.ignite.internal.network.serialization.MessageReader;
import org.apache.ignite.internal.network.serialization.MessageWriter;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SerializatersAndDeserializersFieldNamesTest extends BaseIgniteAbstractTest {
    private final TestMessagesFactory factory = new TestMessagesFactory();

    @Test
    void marshallableFieldIsWrittenWithCorrectName() {
        MessageWriter writer = mock(MessageWriter.class);

        when(writer.isHeaderWritten()).thenReturn(true);
        when(writer.state()).thenReturn(0);

        WithMarshallableSerializer.INSTANCE.writeMessage(messageWithMarshallable(), writer);

        verify(writer).writeByteArray(eq("objectsByteArray"), any());
    }

    @Test
    void marshallableFieldIsReadWithCorrectName() {
        MessageReader reader = mock(MessageReader.class);

        when(reader.beforeMessageRead()).thenReturn(true);
        when(reader.state()).thenReturn(0);

        WithMarshallableDeserializer deserializer = new WithMarshallableDeserializer(factory);
        deserializer.readMessage(reader);

        verify(reader).readByteArray(eq("objectsByteArray"));
    }

    private WithMarshallable messageWithMarshallable() {
        return factory.withMarshallable()
                .objects(new Object[]{"a", "b"})
                .build();
    }
}
