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

package org.apache.ignite.internal.network.serialization;

import org.apache.ignite.internal.network.NetworkMessage;

/**
 * Serialization service implementation.
 */
public class SerializationService {
    /** Message serialization registry. */
    private final MessageSerializationRegistry messageRegistry;

    /** Descriptor registry. */
    private final ClassDescriptorRegistry localDescriptorRegistry;

    /** Descriptor factory. */
    private final ClassDescriptorFactory descriptorFactory;

    /**
     * Constructor.
     *
     * @param messageRegistry Message registry.
     * @param userObjectSerializationContext User object serialization context.
     */
    public SerializationService(
            MessageSerializationRegistry messageRegistry,
            UserObjectSerializationContext userObjectSerializationContext
    ) {
        this.messageRegistry = messageRegistry;
        this.localDescriptorRegistry = userObjectSerializationContext.descriptorRegistry();
        this.descriptorFactory = userObjectSerializationContext.descriptorFactory();
    }

    /**
     * Returns underlying serialization registry.
     */
    public MessageSerializationRegistry serializationRegistry() {
        return messageRegistry;
    }

    /**
     * Creates a message serializer.
     *
     * @see MessageSerializationRegistry#createSerializer(short, short)
     */
    public <T extends NetworkMessage> MessageSerializer<T> createSerializer(short groupType, short messageType) {
        return messageRegistry.createSerializer(groupType, messageType);
    }

    /**
     * Creates a message deserializer.
     *
     * @see MessageSerializationRegistry#createDeserializer(short, short)
     */
    public <T extends NetworkMessage> MessageDeserializer<T> createDeserializer(short groupType, short messageType) {
        return messageRegistry.createDeserializer(groupType, messageType);
    }

    /**
     * Gets a class descriptor for a class.
     *
     * @param clazz the class
     * @return Class descriptor.
     */
    public ClassDescriptor getOrCreateLocalDescriptor(Class<?> clazz) {
        ClassDescriptor descriptor = localDescriptorRegistry.getDescriptor(clazz);
        if (descriptor != null) {
            return descriptor;
        } else {
            return descriptorFactory.create(clazz);
        }
    }

    public ClassDescriptor getLocalDescriptor(int descriptorId) {
        return localDescriptorRegistry.getDescriptor(descriptorId);
    }

    public ClassDescriptorRegistry getLocalDescriptorRegistry() {
        return localDescriptorRegistry;
    }
}
