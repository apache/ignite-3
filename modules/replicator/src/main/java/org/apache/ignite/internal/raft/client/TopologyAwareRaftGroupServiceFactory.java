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

package org.apache.ignite.internal.raft.client;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;

/**
 * Factory for creation {@link TopologyAwareRaftGroupService}.
 */
public class TopologyAwareRaftGroupServiceFactory implements RaftServiceFactory<TopologyAwareRaftGroupService> {
    private final ClusterService clusterService;

    private final LogicalTopologyService logicalTopologyService;

    private final RaftMessagesFactory raftMessagesFactory;

    private final RaftGroupEventsClientListener eventsClientListener;

    /**
     * Constructor.
     *
     * @param clusterService Cluster service.
     * @param logicalTopologyService Logical topology service.
     * @param raftMessagesFactory Raft messages factory.
     * @param eventsClientListener Raft events client listener.
     */
    public TopologyAwareRaftGroupServiceFactory(
            ClusterService clusterService,
            LogicalTopologyService logicalTopologyService,
            RaftMessagesFactory raftMessagesFactory,
            RaftGroupEventsClientListener eventsClientListener
    ) {
        this.clusterService = clusterService;
        this.logicalTopologyService = logicalTopologyService;
        this.raftMessagesFactory = raftMessagesFactory;
        this.eventsClientListener = eventsClientListener;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<TopologyAwareRaftGroupService> startRaftGroupService(
            ReplicationGroupId groupId,
            PeersAndLearners peersAndLearners,
            RaftConfiguration raftConfiguration,
            ScheduledExecutorService raftClientExecutor,
            Marshaller commandsMarshaller
    ) {
        return TopologyAwareRaftGroupService.start(
                groupId,
                clusterService,
                raftMessagesFactory,
                raftConfiguration,
                peersAndLearners,
                true,
                raftClientExecutor,
                logicalTopologyService,
                eventsClientListener,
                true,
                commandsMarshaller
        );
    }
}
