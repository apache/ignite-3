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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.DescriptorRegistry;

/**
 * Unmarshaller that, during each unmarshalling, only uses built-in descriptors and the descriptors that were actually
 * used when marshalling.
 */
class CleanSlateUnmarshaller {
    private final UserObjectMarshaller marshaller;
    private final DescriptorRegistry marshallingRegistry;

    CleanSlateUnmarshaller(UserObjectMarshaller marshaller, DescriptorRegistry marshallingRegistry) {
        this.marshaller = marshaller;
        this.marshallingRegistry = marshallingRegistry;
    }

    /**
     * Unmarshals using a fresh registry and only injecting the descriptors that were reported to be used when marshalling.
     *
     * @param marshalled Marshalling result.
     * @param <T> Expected type of the unmarshalled value.
     * @return Unmarshalled value.
     * @throws UnmarshalException If something goes wrong.
     */
    <T> T unmarshalNonNull(MarshalledObject marshalled) throws UnmarshalException {
        var unmarshallingRegistry = new ClassDescriptorRegistry();

        for (int usedDescriptorId : marshalled.usedDescriptorIds()) {
            ClassDescriptor usedDescriptor = marshallingRegistry.getRequiredDescriptor(usedDescriptorId);
            unmarshallingRegistry.injectDescriptor(usedDescriptor);
        }

        T unmarshalled = marshaller.unmarshal(marshalled.bytes(), unmarshallingRegistry);

        assertThat(unmarshalled, is(notNullValue()));

        return unmarshalled;
    }
}
