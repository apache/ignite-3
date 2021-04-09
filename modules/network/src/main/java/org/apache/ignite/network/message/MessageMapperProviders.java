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

package org.apache.ignite.network.message;

import org.apache.ignite.network.NetworkConfigurationException;

/**
 * Container that maps message types to {@link MessageSerializerProvider} instances.
 */
public final class MessageMapperProviders {
    /** Message mappers, messageMapperProviders[message type] -> message mapper provider for message with message type. */
    private final MessageSerializerProvider<?>[] messageMapperProviders = new MessageSerializerProvider<?>[Short.MAX_VALUE << 1];

    /**
     * Registers message mapper by message type.
     *
     * @param type Message type.
     * @param mapperProvider Message mapper provider.
     */
    public MessageMapperProviders registerProvider(
        short type, MessageSerializerProvider<?> mapperProvider
    ) throws NetworkConfigurationException {
        if (this.messageMapperProviders[type] != null)
            throw new NetworkConfigurationException("Message mapper for type " + type + " is already defined");

        this.messageMapperProviders[type] = mapperProvider;

        return this;
    }

    /**
     * Returns a {@link MessageSerializerProvider} for the given message type.
     * <p>
     * This class does not track the correspondence between a message type and its Java representation, so the
     * actual generic specialization of the returned provider relies on the caller of this method.
     */
    public <T extends NetworkMessage> MessageSerializerProvider<T> getProvider(short type) {
        var provider = messageMapperProviders[type];

        assert provider != null : "No mapper provider defined for type " + type;

        return (MessageSerializerProvider<T>) provider;
    }
}
