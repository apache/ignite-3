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

package org.apache.ignite.network;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.network.serialization.MessageDeserializer;
import org.apache.ignite.network.serialization.MessageSerializationFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.network.serialization.MessageSerializer;

/**
 * Default implementation of a {@link MessageSerializationRegistry}.
 */
public class MessageSerializationRegistryImpl implements MessageSerializationRegistry {
    /** message type -> MessageSerializerProvider instance */
    private final Map<Integer, MessageSerializationFactory<?>> factories = new HashMap<>();

    /**
     * Default constructor.
     */
    public MessageSerializationRegistryImpl() {
        NetworkMessagesSerializationRegistryInitializer.registerFactories(this);
    }

    /** {@inheritDoc} */
    @Override
    public MessageSerializationRegistry registerFactory(
        short groupType, short messageType, MessageSerializationFactory<?> factory
    ) {
        Integer index = asInt(groupType, messageType);

        if (factories.containsKey(index))
            throw new NetworkConfigurationException(String.format(
                "Message serialization factory for message type %d in module %d is already defined",
                messageType, groupType
            ));

        factories.put(index, factory);

        return this;
    }

    /**
     * Gets a {@link MessageSerializationFactory} for the given message type.
     *
     * @param <T> Type of a message.
     * @param groupType Message group type.
     * @param messageType Message type.
     * @return Message's serialization factory.
     */
    private <T extends NetworkMessage> MessageSerializationFactory<T> getFactory(
        short groupType, short messageType
    ) {
        var provider = factories.get(asInt(groupType, messageType));

        assert provider != null : "No serializer provider defined for type " + messageType;

        return (MessageSerializationFactory<T>) provider;
    }

    /** {@inheritDoc} */
    @Override
    public <T extends NetworkMessage> MessageSerializer<T> createSerializer(short groupType, short messageType) {
        MessageSerializationFactory<T> factory = getFactory(groupType, messageType);
        return factory.createSerializer();
    }

    /** {@inheritDoc} */
    @Override
    public <T extends NetworkMessage> MessageDeserializer<T> createDeserializer(short groupType, short messageType) {
        MessageSerializationFactory<T> factory = getFactory(groupType, messageType);
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
