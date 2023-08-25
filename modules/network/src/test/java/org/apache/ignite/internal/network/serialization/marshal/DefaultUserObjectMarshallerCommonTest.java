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

package org.apache.ignite.internal.network.serialization.marshal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.DescriptorRegistry;
import org.junit.jupiter.api.Test;

/**
 * Common tests for {@link DefaultUserObjectMarshaller}.
 */
class DefaultUserObjectMarshallerCommonTest {
    private final ClassDescriptorRegistry descriptorRegistry = new ClassDescriptorRegistry();
    private final ClassDescriptorFactory descriptorFactory = new ClassDescriptorFactory(descriptorRegistry);

    private final DefaultUserObjectMarshaller marshaller = new DefaultUserObjectMarshaller(descriptorRegistry, descriptorFactory);

    static DefaultUserObjectMarshaller staticMarshaller;
    static DescriptorRegistry staticRegistry;

    @Test
    void throwsOnExcessiveInputWhenUnmarshalling() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(null);

        byte[] tooManyBytes = Arrays.copyOf(marshalled.bytes(), marshalled.bytes().length + 1);

        UnmarshalException ex = assertThrows(UnmarshalException.class, () -> marshaller.unmarshal(tooManyBytes, descriptorRegistry));
        assertThat(ex.getMessage(), containsString("After reading a value, 1 excessive byte(s) still remain"));
    }

    @Test
    void previousInvocationsOfMarshallingInSameThreadDoNotHaveVisibleEffectsOnFollowingInvocations() throws Exception {
        List<Integer> list = List.of(1, 2, 3);

        MarshalledObject firstResult = marshaller.marshal(list);

        MarshalledObject secondResult = marshaller.marshal(list);

        assertThat(marshaller.unmarshal(secondResult.bytes(), descriptorRegistry), is(equalTo(list)));
        assertThat(secondResult.bytes(), is(equalTo(firstResult.bytes())));
    }

    @Test
    void nestedMarshallingUnmarshallingInsideWriteReadObjectDoNotInterfereWithOutsideMarshallingUnmarshalling() throws Exception {
        staticMarshaller = marshaller;
        staticRegistry = descriptorRegistry;

        WithNestedMarshalling unmarshalled = marshalAndUnmarshalNotNull(new WithNestedMarshalling(42));

        assertThat(unmarshalled.value, is(42));
    }

    private <T> T marshalAndUnmarshalNotNull(Object object) throws MarshalException, UnmarshalException {
        MarshalledObject marshalled = marshaller.marshal(object);
        T unmarshalled = marshaller.unmarshal(marshalled.bytes(), descriptorRegistry);

        assertThat(unmarshalled, is(notNullValue()));

        return unmarshalled;
    }

    private static class WithNestedMarshalling implements Serializable {
        private final int value;

        private WithNestedMarshalling(int value) {
            this.value = value;
        }

        private void writeObject(ObjectOutputStream stream) throws IOException {
            marshalSomethingElse();

            stream.defaultWriteObject();
        }

        private byte[] marshalSomethingElse() {
            try {
                return staticMarshaller.marshal(List.of(1, 2, 3)).bytes();
            } catch (MarshalException e) {
                throw new RuntimeException(e);
            }
        }

        private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
            byte[] marshalledBytes = marshalSomethingElse();
            try {
                staticMarshaller.unmarshal(marshalledBytes, staticRegistry);
            } catch (UnmarshalException e) {
                throw new RuntimeException(e);
            }

            stream.defaultReadObject();
        }
    }
}
