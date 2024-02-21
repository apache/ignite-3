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

package org.apache.ignite.internal.table;

import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.network.ClusterNode;

/**
 * Internal facade that provides methods for table's raft operations.
 */
public interface TableRaftService extends ManuallyCloseable {

    /**
     * Returns cluster node that is the leader of the corresponding partition group or throws an exception if it cannot be found.
     *
     * @param partition partition number
     * @return leader node of the partition group corresponding to the partition
     */
    ClusterNode leaderAssignment(int partition);

    /**
     * Returns raft group client for corresponding partition.
     *
     * @param partition partition number
     * @return raft group client for corresponding partition
     * @throws IgniteInternalException if partition can't be found.
     */
    RaftGroupService partitionRaftGroupService(int partition);

    /**
     * Closes the service.
     */
    @Override
    void close();
}
