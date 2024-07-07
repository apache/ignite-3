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

package org.apache.ignite.internal.replicator.message;

import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;

/** A class with auxiliary constants and methods for the {@link ReplicaMessageGroup}. */
public class ReplicaMessageUtils {
    /**
     * Converts to a network message.
     *
     * @param messagesFactory Messages factory.
     * @param tablePartitionId Table replication group ID for a given partition.
     * @return New instance of network message.
     */
    public static TablePartitionIdMessage toTablePartitionIdMessage(
            ReplicaMessagesFactory messagesFactory,
            TablePartitionId tablePartitionId
    ) {
        return messagesFactory.tablePartitionIdMessage()
                .tableId(tablePartitionId.tableId())
                .partitionId(tablePartitionId.partitionId())
                .build();
    }

    /**
     * Converts to a network message.
     *
     * @param messagesFactory Messages factory.
     * @param zonePartitionId Zone replication group ID for a given partition.
     * @return New instance of network message.
     */
    public static ZonePartitionIdMessage toZonePartitionIdMessage(
            ReplicaMessagesFactory messagesFactory,
            ZonePartitionId zonePartitionId
    ) {
        return messagesFactory.zonePartitionIdMessage()
                .zoneId(zonePartitionId.zoneId())
                .tableId(zonePartitionId.tableId())
                .partitionId(zonePartitionId.partitionId())
                .build();
    }
}
