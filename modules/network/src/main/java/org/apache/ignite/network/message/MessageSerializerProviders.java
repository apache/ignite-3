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
 * Container that maps message types to {@link MessageSerializationFactory} instances.
 */
public final class MessageSerializerProviders {
    /** message type -> MessageSerializerProvider instance */
    private final MessageSerializationFactory<?>[] providers = new MessageSerializationFactory<?>[Short.MAX_VALUE << 1];

    /**
     * Registers message mapper by message type.
     *
     * @param type Message type.
     * @param provider Message serializer provider.
     */
    public MessageSerializerProviders registerProvider(
        short type, MessageSerializationFactory<?> provider
    ) throws NetworkConfigurationException {
        if (this.providers[type] != null)
            throw new NetworkConfigurationException("Message mapper for type " + type + " is already defined");

        this.providers[type] = provider;

        return this;
    }

    /**
     * Returns a {@link MessageSerializationFactory} for the given message type.
     * <p>
     * This class does not track the correspondence between a message type and its Java representation, so the
     * actual generic specialization of the returned provider relies on the caller of this method.
     */
    public <T extends NetworkMessage> MessageSerializationFactory<T> getProvider(short type) {
        var provider = providers[type];

        assert provider != null : "No serializer provider defined for type " + type;

        return (MessageSerializationFactory<T>) provider;
    }
}
