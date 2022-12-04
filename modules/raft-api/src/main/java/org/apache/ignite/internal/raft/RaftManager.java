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

package org.apache.ignite.internal.raft;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.lang.NodeStoppingException;

/**
 * Raft manager.
 */
public interface RaftManager extends IgniteComponent {
    /**
     * Creates a raft group service providing operations on a raft group. If {@code nodes} contains the current node, then raft group starts
     * on the current node.
     *
     * @param groupId Raft group id.
     * @param peerConsistentIds Consistent IDs of Raft peers.
     * @param lsnrSupplier Raft group listener supplier.
     * @return Future representing pending completion of the operation.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    CompletableFuture<RaftGroupService> prepareRaftGroup(
            ReplicationGroupId groupId,
            Collection<String> peerConsistentIds,
            Supplier<RaftGroupListener> lsnrSupplier
    ) throws NodeStoppingException;

    /**
     * Creates a raft group service providing operations on a raft group. If {@code nodeConsistentIds} or {@code learnerConsistentIds}
     * contains the current node, then raft group starts on the current node.
     *
     * @param groupId Raft group id.
     * @param peerConsistentIds Consistent IDs of Raft peers.
     * @param learnerConsistentIds Consistent IDs of Raft learner nodes.
     * @param lsnrSupplier Raft group listener supplier.
     * @param raftGrpEvtsLsnrSupplier Raft group events listener supplier.
     * @return Future representing pending completion of the operation.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    CompletableFuture<RaftGroupService> prepareRaftGroup(
            ReplicationGroupId groupId,
            Collection<String> peerConsistentIds,
            Collection<String> learnerConsistentIds,
            Supplier<RaftGroupListener> lsnrSupplier,
            Supplier<RaftGroupEventsListener> raftGrpEvtsLsnrSupplier
    ) throws NodeStoppingException;

    /**
     * Stops a raft group on the current node.
     *
     * @param groupId Raft group id.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    void stopRaftGroup(ReplicationGroupId groupId) throws NodeStoppingException;

    /**
     * Creates and starts a raft group service providing operations on a raft group.
     *
     * @param grpId RAFT group id.
     * @param peerConsistentIds Consistent IDs of Raft peers.
     * @param learnerConsistentIds Consistent IDs of Raft learners.
     * @return Future that will be completed with an instance of RAFT group service.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    CompletableFuture<RaftGroupService> startRaftGroupService(
            ReplicationGroupId grpId,
            Collection<String> peerConsistentIds,
            Collection<String> learnerConsistentIds
    ) throws NodeStoppingException;
}
