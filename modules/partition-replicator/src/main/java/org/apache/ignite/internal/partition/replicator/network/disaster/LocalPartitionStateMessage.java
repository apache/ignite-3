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

package org.apache.ignite.internal.partition.replicator.network.disaster;

import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup.DisasterRecoveryMessages;
import org.apache.ignite.internal.replicator.message.TablePartitionIdMessage;
import org.apache.ignite.internal.replicator.message.ZonePartitionIdMessage;
import org.jetbrains.annotations.Nullable;

/**
 * Local partition state message, has partition ID, state and last committed log index.
 */
@Transferable(DisasterRecoveryMessages.LOCAL_PARTITION_STATE)
public interface LocalPartitionStateMessage extends NetworkMessage {
    /** Table Partition ID. */
    @Nullable
    @Deprecated(forRemoval = true)
    TablePartitionIdMessage partitionId();

    /** Zone Partition ID. */
    @Nullable
    ZonePartitionIdMessage zonePartitionId();

    /** Calculated state of the partition. */
    LocalPartitionStateEnum state();

    /** Index of the last received log entry for this partition. */
    long logIndex();

    /** Estimated number of rows for this partition. */
    long estimatedRows();

    /** Returns whether the node holding this partition is a learner in the Raft group. */
    boolean isLearner();
}
