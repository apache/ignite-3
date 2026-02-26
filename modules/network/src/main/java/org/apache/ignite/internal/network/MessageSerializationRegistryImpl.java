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

package org.apache.ignite.internal.network;

import io.micronaut.core.annotation.Creator;
import jakarta.inject.Singleton;
import java.util.ServiceLoader;
import org.apache.ignite.internal.network.serialization.MessageDeserializer;
import org.apache.ignite.internal.network.serialization.MessageSerializationFactory;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistryInitializer;
import org.apache.ignite.internal.network.serialization.MessageSerializer;

/**
 * Default implementation of a {@link MessageSerializationRegistry}.
 */
@Singleton
public class MessageSerializationRegistryImpl implements MessageSerializationRegistry {
    /** group type → message type → MessageSerializerProvider instance. */
    private final MessageSerializationFactory<?>[][] factories =
            new MessageSerializationFactory<?>[Short.MAX_VALUE + 1][];

    /**
     * Creates and initializes an instance of {@link MessageSerializationRegistry}.
     *
     * This method constructs a new instance of {@link MessageSerializationRegistryImpl} and populates it
     * by loading all {@link MessageSerializationRegistryInitializer} implementations available via the
     * Java ServiceLoader mechanism. Each initializer contributes its message serialization factories
     * to the registry.
     *
     * @return A fully initialized instance of {@link MessageSerializationRegistry} containing all registered
     *      message serialization factories.
     */
    @Creator
    public static MessageSerializationRegistry registry() {
        var registry = new MessageSerializationRegistryImpl();

        ServiceLoader<MessageSerializationRegistryInitializer> loader =
                ServiceLoader.load(MessageSerializationRegistryInitializer.class, null);

        for (MessageSerializationRegistryInitializer initializer : loader) {
            initializer.registerFactories(registry);
        }

        return registry;
    }

    @Override
    public MessageSerializationRegistry registerFactory(
            short groupType, short messageType, MessageSerializationFactory<?> factory
    ) {
        assert groupType >= 0 : "group type must not be negative";
        assert messageType >= 0 : "message type must not be negative";

        MessageSerializationFactory<?>[] groupFactories = factories[groupType];

        if (groupFactories == null) {
            groupFactories = new MessageSerializationFactory<?>[Short.MAX_VALUE + 1];
            factories[groupType] = groupFactories;
        } else if (groupFactories[messageType] != null) {
            throw new NetworkConfigurationException(String.format(
                    "Message serialization factory for message type %d in module %d is already defined",
                    messageType, groupType
            ));
        }

        groupFactories[messageType] = factory;

        return this;
    }

    /**
     * Gets a {@link MessageSerializationFactory} for the given message type.
     *
     * @param <T>         Type of a message.
     * @param groupType   Group type of a message.
     * @param messageType Message type.
     * @return Message's serialization factory.
     * @throws NetworkConfigurationException if no serializers have been registered for the given group type and message type.
     */
    private <T extends NetworkMessage> MessageSerializationFactory<T> getFactory(short groupType, short messageType) {
        if (groupType < 0) {
            throw new NetworkConfigurationException("Group type must not be negative, groupType=" + groupType);
        }
        if (messageType < 0) {
            throw new NetworkConfigurationException("Message type must not be negative, messageType=" + messageType);
        }

        MessageSerializationFactory<?>[] groupFactories = factories[groupType];

        MessageSerializationFactory<?> provider = groupFactories == null ? null : groupFactories[messageType];

        if (provider == null) {
            throw new NetworkConfigurationException(String.format(
                    "No serializer provider defined for group type %d and message type %d", groupType, messageType
            ));
        }

        return (MessageSerializationFactory<T>) provider;
    }

    @Override
    public <T extends NetworkMessage> MessageSerializer<T> createSerializer(short groupType, short messageType) {
        MessageSerializationFactory<T> factory = getFactory(groupType, messageType);
        return factory.createSerializer();
    }

    @Override
    public <T extends NetworkMessage> MessageDeserializer<T> createDeserializer(short groupType, short messageType) {
        MessageSerializationFactory<T> factory = getFactory(groupType, messageType);
        return factory.createDeserializer();
    }
}
