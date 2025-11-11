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

import org.apache.ignite.internal.network.annotations.MessageGroup;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommand;

/**
 * Message group for the replication process.
 */
@MessageGroup(groupType = ReplicaMessageGroup.GROUP_TYPE, groupName = "ReplicaMessages")
public interface ReplicaMessageGroup {
    /** Table message group type. */
    short GROUP_TYPE = 8;

    /** Message type for {@link ErrorReplicaResponse}. */
    short ERROR_REPLICA_RESPONSE = 1;

    /** Message type for {@link ReplicaResponse}. */
    short REPLICA_RESPONSE = 2;

    /** Message type for {@link ErrorTimestampAwareReplicaResponse}. */
    short ERROR_TIMESTAMP_AWARE_REPLICA_RESPONSE = 3;

    /** Message type for {@link TimestampAwareReplicaResponse}. */
    short TIMESTAMP_AWARE_REPLICA_RESPONSE = 4;

    /** Message type for {@link ReplicaSafeTimeSyncRequest}. */
    short SAFE_TIME_SYNC_REQUEST = 5;

    /** Message type for {@link AwaitReplicaRequest}. */
    short AWAIT_REPLICA_REQUEST = 6;

    /** Message type for {@link AwaitReplicaResponse}. */
    short AWAIT_REPLICA_RESPONSE = 7;

    /** Message type for {@link SafeTimeSyncCommand}. */
    short SAFE_TIME_SYNC_COMMAND = 40;

    /** Message type for {@link PrimaryReplicaChangeCommand}. */
    short PRIMARY_REPLICA_CHANGE_COMMAND = 41;

    /** Message type for {@link TablePartitionIdMessage}. */
    short TABLE_PARTITION_ID_MESSAGE = 42;

    /** Message type for {@link ZonePartitionIdMessage}. */
    short ZONE_PARTITION_ID_MESSAGE = 43;
}
