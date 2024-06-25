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

import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.ignite.internal.network.serialization.MessageSerializer;

/**
 * Message for exchanging information in a cluster.
 */
public interface NetworkMessage extends Cloneable {
    /** Group type for the {@code null} message. */
    short NULL_GROUP_TYPE = -1;

    /**
     * Returns a serializer instance for the specific type of message, defined by the implementation.
     */
    MessageSerializer<NetworkMessage> serializer();

    /**
     * Message type. Must be <b>distinct</b> among all messages in a <i>message group</i>. Only positive values are allowed.
     *
     * <p>Message types are not required to be universally unique among multiple groups.
     *
     * @return message type.
     */
    short messageType();

    /**
     * Message group type. Must be the <b>same</b> for all messages in a <i>message group</i>. Only positive values are allowed.
     *
     * <p>Message group types are required to be universally unique among all groups.
     *
     * @return group type.
     */
    short groupType();

    default void prepareMarshal(IntSet ids, Object marshaller) throws Exception {
        // No-op.
    }

    default void unmarshal(Object marshaller, Object descriptors) throws Exception {
        // No-op.
    }

    /**
     * Returns {@code true} if this message needs an acknowledgement from the remote node, {@code false} otherwise.
     *
     * @return {@code true} if this message needs an acknowledgement from the remote node, {@code false} otherwise.
     */
    default boolean needAck() {
        return true;
    }

    /**
     * Public clone version that is implemented in generated *Impl class.
     */
    NetworkMessage clone();
}
