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

package org.apache.ignite.internal.tx.message;

import java.util.Set;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.replicator.message.ReplicationGroupIdMessage;
import org.apache.ignite.internal.tx.impl.EnlistedPartitionGroup;

/**
 * Message for {@link EnlistedPartitionGroup}.
 */
@Transferable(TxMessageGroup.ENLISTED_PARTITION_GROUP_MESSAGE)
public interface EnlistedPartitionGroupMessage extends NetworkMessage {
    /**
     * Replication group ID of the partition.
     */
    ReplicationGroupIdMessage groupId();

    /**
     * IDs of tables for which the partition is enlisted.
     */
    Set<Integer> tableIds();

    /**
     * Converts this message to the corresponding {@link EnlistedPartitionGroup}.
     */
    default EnlistedPartitionGroup asPartitionInfo() {
        return new EnlistedPartitionGroup(groupId().asReplicationGroupId(), tableIds());
    }
}
