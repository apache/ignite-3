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

package org.apache.ignite.internal.partition.replicator.network.replication;

import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaRequest;

/**
 * Request for performing {@link org.apache.ignite.internal.raft.service.RaftGroupService#changePeersAndLearnersAsync} on a primary replica.
 */
@Transferable(PartitionReplicationMessageGroup.CHANGE_PEERS_AND_LEARNERS_ASYNC_REPLICA_REQUEST)
public interface ChangePeersAndLearnersAsyncReplicaRequest extends PrimaryReplicaRequest {
    /** New peers configuration to rebalance. */
    byte[] pendingAssignments();

    /** Sequence token of the change.
     *
     * <p>Used to version updates to prevent reordering and applying stale ones.
     **/
    long sequenceToken();
}
