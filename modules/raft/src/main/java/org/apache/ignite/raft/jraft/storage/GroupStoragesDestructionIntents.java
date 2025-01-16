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

package org.apache.ignite.raft.jraft.storage;

import java.util.Map;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.replicator.ReplicationGroupId;

/** Persists and retrieves intent to complete storages destruction on node start. */
public interface GroupStoragesDestructionIntents {
    /** Add configurer for CMG or metastorage raft storages. */
    void addGroupOptionsConfigurer(ReplicationGroupId groupId, RaftGroupOptionsConfigurer groupOptionsConfigurer);

    /** Add configurer for partitions raft storages. */
    void addPartitionGroupOptionsConfigurer(RaftGroupOptionsConfigurer partitionRaftConfigurer);

    /** Save intent to destroy raft storages. */
    void saveDestroyStorageIntent(RaftNodeId nodeId, RaftGroupOptions groupOptions);

    /** Remove intent to destroy raft storages. */
    void removeDestroyStorageIntent(String nodeId);

    /** Returns group options needed to destroy raft storages, mapped by node id represented by String. */
    Map<String, RaftGroupOptions> readGroupOptionsByNodeIdForDestruction();
}
