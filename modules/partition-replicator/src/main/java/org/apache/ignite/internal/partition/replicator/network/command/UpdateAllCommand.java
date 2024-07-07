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

package org.apache.ignite.internal.partition.replicator.network.command;

import static org.apache.ignite.internal.hlc.HybridTimestamp.nullableHybridTimestamp;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.network.TimedBinaryRow;
import org.apache.ignite.internal.replicator.message.TablePartitionIdMessage;
import org.apache.ignite.internal.util.CollectionUtils;
import org.jetbrains.annotations.Nullable;

/**
 * State machine command for updating a batch of entries.
 */
@Transferable(PartitionReplicationMessageGroup.Commands.UPDATE_ALL)
public interface UpdateAllCommand extends PartitionCommand {
    TablePartitionIdMessage tablePartitionId();

    Map<UUID, TimedBinaryRowMessage> messageRowsToUpdate();

    String txCoordinatorId();

    /** Lease start time, hybrid timestamp as long, see {@link HybridTimestamp#longValue()}. Should be non-null for the full transactions.*/
    @Nullable Long leaseStartTime();

    /**
     * Returns the timestamps of the last committed entries for each row.
     */
    default Map<UUID, TimedBinaryRow> rowsToUpdate() {
        Map<UUID, TimedBinaryRow> map = new HashMap<>();

        Map<UUID, TimedBinaryRowMessage> timedRowMap = messageRowsToUpdate();

        if (!CollectionUtils.nullOrEmpty(timedRowMap)) {
            timedRowMap.forEach(
                    (uuid, trMsg) -> map.put(uuid, new TimedBinaryRow(trMsg.binaryRow(), nullableHybridTimestamp(trMsg.timestamp()))));
        }

        return map;
    }
}
