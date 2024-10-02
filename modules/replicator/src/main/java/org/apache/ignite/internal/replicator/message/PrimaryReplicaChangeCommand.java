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

import static org.apache.ignite.internal.replicator.message.ReplicaMessageGroup.PRIMARY_REPLICA_CHANGE_COMMAND;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.raft.WriteCommand;

/**
 * Command to write the primary replica change to the replication group.
 */
@Transferable(PRIMARY_REPLICA_CHANGE_COMMAND)
public interface PrimaryReplicaChangeCommand extends WriteCommand {
    /** Lease start time, hybrid timestamp as long, see {@link HybridTimestamp#longValue()}. */
    long leaseStartTime();

    /** Primary replica node id. */
    UUID primaryReplicaNodeId();

    /** Primary replica node name. */
    String primaryReplicaNodeName();
}
