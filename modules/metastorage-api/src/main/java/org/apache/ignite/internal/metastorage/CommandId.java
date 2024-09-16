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

package org.apache.ignite.internal.metastorage;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.internal.metastorage.dsl.MetaStorageMessageGroup;
import org.apache.ignite.internal.metastorage.dsl.MetaStorageMessagesFactory;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;

/**
 * Command id. It consists of node id and a counter that is generated on the node and is unique for that node, so the whole command id
 * would be unique cluster-wide.
 */
@Transferable(MetaStorageMessageGroup.COMMAND_ID)
public interface CommandId extends NetworkMessage, Serializable {
    MetaStorageMessagesFactory MSG_FACTORY = new MetaStorageMessagesFactory();

    UUID nodeId();

    long counter();

    /**
     * Returns string representation of a CommandId to use as a human readable meta storage key.
     *
     * @return String representation of a CommandId to use as a human readable meta storage key.
     */
    default String toMgKeyAsString() {
        return nodeId() + "_cnt_" + counter();
    }

    /**
     * Meta storage key as string to CommandId converter. See {@link CommandId#toMgKeyAsString()} for more details.
     *
     * @param mgKeyString String representation of a CommandId.
     * @return CommandId instance.
     */
    static CommandId fromString(String mgKeyString) {
        String[] parts = mgKeyString.split("_cnt_");

        return MSG_FACTORY.commandId()
                .nodeId(UUID.fromString(parts[0]))
                .counter(Long.parseLong(parts[1]))
                .build();
    }
}
