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

import org.apache.ignite.internal.replicator.command.HybridTimestampMessage;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommand;
import org.apache.ignite.network.annotations.MessageGroup;

/**
 * Message group for the replication process.
 */
@MessageGroup(groupType = 8, groupName = "ReplicaMessages")
public interface ReplicaMessageGroup {
    /** Message type for {@link ErrorReplicaResponse}. */
    short ERROR_REPLICA_RESPONSE = 1;

    /** Message type for {@link ReplicaResponse}. */
    short REPLICA_RESPONSE = 2;

    /** Message type for {@link TimestampAware}. */
    short TIMESTAMP_AWARE = 3;

    /** Message type for {@link ErrorTimestampAwareReplicaResponse}. */
    short ERROR_TIMESTAMP_AWARE_REPLICA_RESPONSE = 4;

    /** Message type for {@link TimestampAwareReplicaResponse}. */
    short TIMESTAMP_AWARE_REPLICA_RESPONSE = 5;

    /** Message type for {@link ReplicaSafeTimeSyncRequest}. */
    short SAFE_TIME_SYNC_REQUEST = 6;

    /** Message type for {@link AwaitReplicaRequest}. */
    short AWAIT_REPLICA_REQUEST = 7;

    /** Message type for {@link AwaitReplicaResponse}. */
    short AWAIT_REPLICA_RESPONSE = 8;

    /** Message type for {@link SafeTimeSyncCommand}. */
    short SAFE_TIME_SYNC_COMMAND = 40;

    /** Message type for {@link HybridTimestampMessage}. */
    short HYBRID_TIMESTAMP = 60;
}
