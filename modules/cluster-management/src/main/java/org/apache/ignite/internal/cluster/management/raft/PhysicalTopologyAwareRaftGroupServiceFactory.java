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

package org.apache.ignite.internal.cluster.management.raft;

import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.ExceptionFactory;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.TimeAwareRaftServiceFactory;
import org.apache.ignite.internal.raft.client.PhysicalTopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;

/**
 * Factory for creating {@link PhysicalTopologyAwareRaftGroupService} instances for CMG.
 */
public class PhysicalTopologyAwareRaftGroupServiceFactory implements TimeAwareRaftServiceFactory {

    private final ClusterService clusterService;

    private final RaftGroupEventsClientListener eventsClientListener;

    private final FailureProcessor failureProcessor;

    /**
     * Constructor.
     *
     * @param clusterService Cluster service.
     * @param eventsClientListener Raft events client listener.
     * @param failureProcessor Failure processor.
     */
    public PhysicalTopologyAwareRaftGroupServiceFactory(
            ClusterService clusterService,
            RaftGroupEventsClientListener eventsClientListener,
            FailureProcessor failureProcessor
    ) {
        this.clusterService = clusterService;
        this.eventsClientListener = eventsClientListener;
        this.failureProcessor = failureProcessor;
    }

    @Override
    public PhysicalTopologyAwareRaftGroupService startRaftGroupService(
            ReplicationGroupId groupId,
            PeersAndLearners peersAndLearners,
            RaftConfiguration raftConfiguration,
            ScheduledExecutorService raftClientExecutor,
            Marshaller commandsMarshaller,
            ExceptionFactory stoppingExceptionFactory
    ) {
        return PhysicalTopologyAwareRaftGroupService.start(
                groupId,
                clusterService,
                raftConfiguration,
                peersAndLearners,
                raftClientExecutor,
                eventsClientListener,
                commandsMarshaller,
                stoppingExceptionFactory,
                failureProcessor
        );
    }
}
