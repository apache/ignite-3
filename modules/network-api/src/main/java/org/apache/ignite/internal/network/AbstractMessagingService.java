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

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.internal.network.annotations.MessageGroup;
import org.apache.ignite.internal.thread.ExecutorChooser;

/**
 * Base class for {@link MessagingService} implementations.
 */
public abstract class AbstractMessagingService implements MessagingService {
    /** Special value meaning that messages are to be processed in the node-wide inbound pool. */
    protected static final ExecutorChooser<NetworkMessage> IN_INBOUND_POOL = new InInboundPool();

    /** A handler and its registration context. */
    protected static class HandlerContext {
        private final NetworkMessageHandler handler;
        private final ExecutorChooser<NetworkMessage> executorChooser;

        private HandlerContext(NetworkMessageHandler handler, ExecutorChooser<NetworkMessage> executorChooser) {
            this.handler = handler;
            this.executorChooser = executorChooser;
        }

        public NetworkMessageHandler handler() {
            return handler;
        }

        public ExecutorChooser<NetworkMessage> executorChooser() {
            return executorChooser;
        }
    }

    /**
     * Class holding a pair of a message group class and corresponding handlers.
     */
    private static class Handlers {
        /** Message group. */
        final Class<?> messageGroup;

        /** Handlers, registered for the corresponding message group. */
        final List<HandlerContext> handlerContexts;

        /**
         * Constructor.
         *
         * @param messageGroup Message group.
         * @param handlerContexts     Message handlers.
         */
        Handlers(Class<?> messageGroup, List<HandlerContext> handlerContexts) {
            this.messageGroup = messageGroup;
            this.handlerContexts = handlerContexts;
        }
    }

    /** Mapping from group type (array index) to a list of registered message handlers. */
    private final AtomicReferenceArray<Handlers> handlersByGroupType = new AtomicReferenceArray<>(Short.MAX_VALUE + 1);

    @Override
    public void addMessageHandler(Class<?> messageGroup, NetworkMessageHandler handler) {
        doAddMessageHandler(messageGroup, IN_INBOUND_POOL, handler);
    }

    @Override
    public void addMessageHandler(Class<?> messageGroup, ExecutorChooser<NetworkMessage> executorChooser, NetworkMessageHandler handler) {
        doAddMessageHandler(messageGroup, executorChooser, handler);
    }

    private void doAddMessageHandler(
            Class<?> messageGroup,
            ExecutorChooser<NetworkMessage> executorChooser,
            NetworkMessageHandler handler
    ) {
        // Only track handling time if the handling is going to happen in an inbound thread. If there is a
        // custom executor chooser, then the chosen executor is probably ready for long processing if it happens.
        NetworkMessageHandler handlerToAdd = wantsInboundPool(executorChooser)
                ? new TrackableNetworkMessageHandler(handler) : handler;
        HandlerContext newHandlerContext = new HandlerContext(handlerToAdd, executorChooser);

        handlersByGroupType.getAndUpdate(getMessageGroupType(messageGroup), oldHandlers -> {
            if (oldHandlers == null) {
                return new Handlers(messageGroup, List.of(newHandlerContext));
            }

            if (oldHandlers.messageGroup != messageGroup) {
                throw new IllegalArgumentException(String.format(
                        "Handlers are already registered for a message group with the same group ID "
                                + "but different class. Group ID: %d, given message group: %s, existing message group: %s",
                        getMessageGroupType(messageGroup), messageGroup, oldHandlers.messageGroup
                ));
            }

            var handlerContexts = new ArrayList<HandlerContext>(oldHandlers.handlerContexts.size() + 1);

            handlerContexts.addAll(oldHandlers.handlerContexts);
            handlerContexts.add(newHandlerContext);

            return new Handlers(messageGroup, handlerContexts);
        });
    }

    protected static boolean wantsInboundPool(ExecutorChooser<NetworkMessage> executorChooser) {
        return executorChooser == IN_INBOUND_POOL;
    }

    /**
     * Extracts the message group ID from a class annotated with {@link MessageGroup}.
     *
     * @param messageGroup Message group.
     * @return Message group ID.
     */
    private static short getMessageGroupType(Class<?> messageGroup) {
        MessageGroup annotation = messageGroup.getAnnotation(MessageGroup.class);

        assert annotation != null : "No MessageGroup annotation present on " + messageGroup;

        short groupType = annotation.groupType();

        assert groupType >= 0 : "Group type must not be negative";

        return groupType;
    }

    /**
     * Returns registered handlers for the given group ID.
     *
     * @param groupType Message group ID.
     * @return Registered message handlers.
     */
    protected final Collection<NetworkMessageHandler> getMessageHandlers(short groupType) {
        assert groupType >= 0 : "Group type must not be negative";

        Handlers result = handlersByGroupType.get(groupType);

        return result == null ? List.of() : result.handlerContexts.stream().map(HandlerContext::handler).collect(toList());
    }

    /**
     * Returns registered handlers for the given group ID.
     *
     * @param groupType Message group ID.
     * @return Registered message handlers.
     */
    protected final List<HandlerContext> getHandlerContexts(short groupType) {
        assert groupType >= 0 : "Group type must not be negative";

        Handlers result = handlersByGroupType.get(groupType);

        return result == null ? List.of() : result.handlerContexts;
    }

    /**
     * A 'chooser' that exists only to signal that an incoming message has to be handled in the node-wide inbound thread pool.
     */
    private static class InInboundPool implements ExecutorChooser<NetworkMessage> {
        @Override
        public Executor choose(NetworkMessage argument) {
            throw new UnsupportedOperationException("This should never be called");
        }
    }
}
