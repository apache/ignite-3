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

package org.apache.ignite.network.serialization;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.network.NetworkConfigurationException;
import org.apache.ignite.network.NetworkMessage;

/**
 * Container that maps message types to {@link MessageSerializationFactory} instances.
 */
public class MessageSerializationRegistry {
    /** message type -> MessageSerializerProvider instance */
    private final Map<Integer, MessageSerializationFactory<?>> factories = new HashMap<>();

    /**
     * Registers message serialization factory by message type.
     *
     * @param moduleType Module type of a message.
     * @param messageType Message type.
     * @param factory Message's serialization factory.
     * @return This registry.
     * @throws NetworkConfigurationException If there is an already registered factory for the given type.
     */
    public MessageSerializationRegistry registerFactory(
        short moduleType, short messageType, MessageSerializationFactory<?> factory
    ) {
        Integer index = asInt(moduleType, messageType);

        if (factories.containsKey(index))
            throw new NetworkConfigurationException(String.format(
                "Message serialization factory for message type %d in module %d is already defined",
                messageType, moduleType
            ));

        factories.put(index, factory);

        return this;
    }

    /**
     * Gets a {@link MessageSerializationFactory} for the given message type.
     *
     * @param <T> Type of a message.
     * @param moduleType Module type of a message.
     * @param messageType Message type.
     * @return Message's serialization factory.
     */
    private <T extends NetworkMessage> MessageSerializationFactory<T> getFactory(
        short moduleType, short messageType
    ) {
        var provider = factories.get(asInt(moduleType, messageType));

        assert provider != null : "No serializer provider defined for type " + messageType;

        return (MessageSerializationFactory<T>) provider;
    }

    /**
     * Creates a {@link MessageSerializer} for the given message type.
     * <p>
     * {@link MessageSerializationRegistry} does not track the correspondence between the message type and its Java
     * representation, so the actual generic specialization of the returned provider relies on the caller of this
     * method.
     *
     * @param <T> Type of a message.
     * @param moduleType Module type of a message.
     * @param messageType Message type.
     * @return Message's serializer.
     */
    public <T extends NetworkMessage> MessageSerializer<T> createSerializer(short moduleType, short messageType) {
        MessageSerializationFactory<T> factory = getFactory(moduleType, messageType);
        return factory.createSerializer();
    }

    /**
     * Creates a {@link MessageDeserializer} for the given message type.
     * <p>
     * {@link MessageSerializationRegistry} does not track the correspondence between the message type and its Java
     * representation, so the actual generic specialization of the returned provider relies on the caller of this
     * method.
     *
     * @param <T> Type of a message.
     * @param moduleType Module type of a message.
     * @param messageType Message type.
     * @return Message's deserializer.
     */
    public <T extends NetworkMessage> MessageDeserializer<T> createDeserializer(short moduleType, short messageType) {
        MessageSerializationFactory<T> factory = getFactory(moduleType, messageType);
        return factory.createDeserializer();
    }

    /**
     * Concatenates two given {@code short}s into an {@code int}.
     *
     * @param higher Higher bytes.
     * @param lower Lower bytes.
     */
    private static int asInt(short higher, short lower) {
        return (higher << Short.SIZE) | lower;
    }
}
