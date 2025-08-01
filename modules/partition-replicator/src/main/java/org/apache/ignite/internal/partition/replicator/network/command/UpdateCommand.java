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

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.annotations.PropertyName;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup.Commands;
import org.apache.ignite.internal.replicator.message.ReplicationGroupIdMessage;
import org.apache.ignite.internal.schema.BinaryRow;
import org.jetbrains.annotations.Nullable;

/**
 * State machine command to update a row specified by a row ID.
 *
 * <p>This command is replaced with {@link UpdateCommandV2} and only exists in the source code for backward compatibility.</p>
 */
@Transferable(Commands.UPDATE_V1)
public interface UpdateCommand extends PartitionCommand {
    @PropertyName("tablePartitionId")
    ReplicationGroupIdMessage commitPartitionId();

    UUID rowUuid();

    @Nullable TimedBinaryRowMessage messageRowToUpdate();

    UUID txCoordinatorId();

    /** Lease start time, hybrid timestamp as long, see {@link HybridTimestamp#longValue()}. Should be non-null for the full transactions.*/
    @Nullable Long leaseStartTime();

    /** Returns the row to update or {@code null} if the row should be removed. */
    default @Nullable BinaryRow rowToUpdate() {
        TimedBinaryRowMessage tsRoMsg = messageRowToUpdate();

        return tsRoMsg == null ? null : tsRoMsg.binaryRow();
    }

    /** Returns the timestamp of the last committed entry. */
    default @Nullable HybridTimestamp lastCommitTimestamp() {
        TimedBinaryRowMessage tsRoMsg = messageRowToUpdate();

        return tsRoMsg == null ? null : tsRoMsg.timestamp();
    }
}
