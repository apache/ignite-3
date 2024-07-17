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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.List;
import org.apache.ignite.internal.network.serialization.marshal.MarshalledObject;
import org.apache.ignite.internal.network.serialization.marshal.UserObjectMarshaller;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GenerationTest extends BaseIgniteAbstractTest {
    @Mock
    private UserObjectMarshaller marshaller;

    private final TestMessagesFactory factory = new TestMessagesFactory();

    @Test
    void listOfMessagesWithMarshallableIsReconstructed() throws Exception {
        byte[] abArrayMarker = new byte[0];

        when(marshaller.marshal(any(Object[].class))).thenReturn(new MarshalledObject(abArrayMarker, new IntOpenHashSet()));
        when(marshaller.unmarshal(eq(abArrayMarker), any())).thenReturn(new Object[]{"a", "b"});

        WithMarshallableImpl submessage = (WithMarshallableImpl) factory.withMarshallable()
                .objects(new Object[]{"a", "b"})
                .build();

        WithListOfMessagesWithMarshallable containerMessage = factory.withListOfMessagesWithMarshallable()
                .subMessages(List.of(submessage))
                .build();

        containerMessage.prepareMarshal(new IntOpenHashSet(), marshaller);

        assertThat(submessage.objectsByteArray(), is(notNullValue()));

        WithMarshallable reconstructedSubmessage = factory.withMarshallable()
                .objectsByteArray(submessage.objectsByteArray())
                .build();

        reconstructedSubmessage.unmarshal(marshaller, new Object());

        assertThat(reconstructedSubmessage.objects(), is(new Object[]{"a", "b"}));
    }
}
