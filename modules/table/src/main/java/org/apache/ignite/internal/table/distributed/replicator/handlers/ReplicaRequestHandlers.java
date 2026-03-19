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

package org.apache.ignite.internal.table.distributed.replicator.handlers;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.jetbrains.annotations.Nullable;

/**
 * Registry of replica request handlers keyed by message group and message type.
 *
 * <p>Contains two separate registries: one for {@link ReplicaRequestHandler} and one for {@link ReadOnlyReplicaRequestHandler}.
 */
public class ReplicaRequestHandlers {
    private final Map<MessageId, ReplicaRequestHandler<?>> handlers;

    private final Map<MessageId, ReadOnlyReplicaRequestHandler<?>> roHandlers;

    private ReplicaRequestHandlers(
            Map<MessageId, ReplicaRequestHandler<?>> handlers,
            Map<MessageId, ReadOnlyReplicaRequestHandler<?>> roHandlers
    ) {
        this.handlers = handlers;
        this.roHandlers = roHandlers;
    }

    /**
     * Returns a handler for the given message group and type, or {@code null} if no handler is registered.
     *
     * @param messageGroup Message group identifier.
     * @param messageType Message type identifier.
     * @return Handler, or {@code null} if not found.
     */
    public @Nullable ReplicaRequestHandler<ReplicaRequest> handler(short messageGroup, short messageType) {
        return (ReplicaRequestHandler<ReplicaRequest>) handlers.get(new MessageId(messageGroup, messageType));
    }

    /**
     * Returns a read-only handler for the given message group and type, or {@code null} if no handler is registered.
     *
     * @param messageGroup Message group identifier.
     * @param messageType Message type identifier.
     * @return Handler, or {@code null} if not found.
     */
    public @Nullable ReadOnlyReplicaRequestHandler<ReplicaRequest> roHandler(
            short messageGroup,
            short messageType
    ) {
        return (ReadOnlyReplicaRequestHandler<ReplicaRequest>) roHandlers.get(new MessageId(messageGroup, messageType));
    }

    private static final class MessageId {
        final short messageGroup;
        final short messageType;

        MessageId(short messageGroup, short messageType) {
            this.messageGroup = messageGroup;
            this.messageType = messageType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MessageId messageId = (MessageId) o;
            return messageGroup == messageId.messageGroup && messageType == messageId.messageType;
        }

        @Override
        public int hashCode() {
            int result = 1;

            result = 31 * result + messageGroup;
            result = 31 * result + messageType;

            return result;
        }
    }

    /**
     * Builder for {@link ReplicaRequestHandlers}.
     */
    public static class Builder {
        private final Map<MessageId, ReplicaRequestHandler<?>> handlers = new HashMap<>();

        private final Map<MessageId, ReadOnlyReplicaRequestHandler<?>> roHandlers = new HashMap<>();

        /**
         * Registers a handler for the given message group and message type.
         *
         * @param messageGroup Message group identifier.
         * @param messageType Message type identifier.
         * @param handler Handler.
         * @throws IllegalArgumentException If a handler for the given message id is already registered.
         */
        public void addHandler(
                short messageGroup,
                short messageType,
                ReplicaRequestHandler<?> handler
        ) {
            MessageId id = new MessageId(messageGroup, messageType);

            if (handlers.containsKey(id)) {
                throw new IllegalArgumentException(
                        "Handler already exists [messageGroup=" + messageGroup + ", messageType=" + messageType + "].");
            }

            handlers.put(id, handler);
        }

        /**
         * Registers a read-only handler for the given message group and message type.
         *
         * @param messageGroup Message group identifier.
         * @param messageType Message type identifier.
         * @param handler Handler.
         * @throws IllegalArgumentException If a handler for the given message id is already registered.
         */
        public void addRoHandler(
                short messageGroup,
                short messageType,
                ReadOnlyReplicaRequestHandler<?> handler
        ) {
            MessageId id = new MessageId(messageGroup, messageType);

            if (roHandlers.containsKey(id)) {
                throw new IllegalArgumentException(
                        "RO handler already exists [messageGroup=" + messageGroup + ", messageType=" + messageType + "].");
            }

            roHandlers.put(id, handler);
        }

        /**
         * Builds the registry.
         *
         * @return Immutable {@link ReplicaRequestHandlers} instance.
         */
        public ReplicaRequestHandlers build() {
            return new ReplicaRequestHandlers(Map.copyOf(handlers), Map.copyOf(roHandlers));
        }
    }
}
