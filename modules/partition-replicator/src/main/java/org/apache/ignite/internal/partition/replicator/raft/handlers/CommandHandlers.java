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

package org.apache.ignite.internal.partition.replicator.raft.handlers;

import java.util.HashMap;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

/**
 * This class represents a collection of command handlers.
 *
 * @see AbstractCommandHandler
 */
public class CommandHandlers {
    private final Map<MessageId, AbstractCommandHandler<?>> handlers;

    /**
     * Creates a new instance of the command handlers collection.
     *
     * @param handlers Command handlers.
     */
    private CommandHandlers(Map<MessageId, AbstractCommandHandler<?>> handlers) {
        this.handlers = handlers;
    }

    /**
     * Returns a command handler for the specified message group and message type.
     *
     * @param messageGroup Message group identifier.
     * @param messageType Message type identifier.
     * @return Command handler.
     */
    public @Nullable AbstractCommandHandler<?> handler(short messageGroup, short messageType) {
        return handlers.get(new MessageId(messageGroup, messageType));
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
     * Creates a new instance of the command handlers collection.
     */
    public static class Builder {
        private final Map<MessageId, AbstractCommandHandler<?>> handlers = new HashMap<>();

        /**
         * Adds a command handler to the collection.
         *
         * @param messageGroup Message group identifier.
         * @param messageType Message type identifier.
         * @param handler Command handler.
         * @throws IllegalArgumentException If a handler for the specified message group and message type already exists.
         */
        public Builder addHandler(
                short messageGroup,
                short messageType,
                AbstractCommandHandler<?> handler
        ) throws IllegalArgumentException {
            MessageId id = new MessageId(messageGroup, messageType);

            if (handlers.containsKey(id)) {
                throw new IllegalArgumentException("Handler already exists [messageGroup=" + messageGroup
                        + ", messageType=" + messageType + "].");
            }

            handlers.put(id, handler);

            return this;
        }

        /**
         * Builds a new instance of the command handlers collection.
         *
         * @return Command handlers collection.
         */
        public CommandHandlers build() {
            return new CommandHandlers(handlers);
        }
    }
}
