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

package org.apache.ignite.internal.table.distributed.disaster;

import static org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum.HEALTHY;
import static org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum.INITIALIZING;
import static org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum.INSTALLING_SNAPSHOT;

import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.core.State;

class LocalPartitionStateEnumWithLogIndex {
    final LocalPartitionStateEnum state;

    final long logIndex;

    private LocalPartitionStateEnumWithLogIndex(LocalPartitionStateEnum state, long logIndex) {
        this.state = state;
        this.logIndex = logIndex;
    }

    static LocalPartitionStateEnumWithLogIndex of(Node raftNode) {
        State nodeState = raftNode.getNodeState();

        LocalPartitionStateEnum localState = LocalPartitionStateEnum.convert(nodeState);
        long lastLogIndex = raftNode.lastLogIndex();

        if (localState == HEALTHY) {
            // Node without log didn't process anything yet, it's not really "healthy" before it accepts leader's configuration.
            if (lastLogIndex == 0) {
                localState = INITIALIZING;
            }

            if (raftNode.isInstallingSnapshot()) {
                localState = INSTALLING_SNAPSHOT;
            }
        }

        return new LocalPartitionStateEnumWithLogIndex(localState, lastLogIndex);
    }
}
