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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Raft manager.
 *
 * <p>This class contains two groups of methods for starting Raft nodes: {@code #startRaftGroupNode} and
 * {@code startRaftGroupNodeAndWaitNodeReadyFuture} (and its overloads). When using {@code #startRaftGroupNode}, Raft log re-application
 * does not get performed and external synchronisation methods must be used to avoid observing a Raft node in inconsistent state. The other
 * group of methods synchronously waits for the Raft log to be re-applied, so no external synchronisation is required.
 *
 * <p>Usually Raft recovery is done synchronously, but sometimes there's an implicit dependency between Raft nodes, where the recovery of
 * one node triggers the recovery of the other and vice versa. In this case, {@code #startRaftGroupNode} group of methods should be used
 * to avoid deadlocks during Raft node startup.
 */
public interface RaftManager extends IgniteComponent {
    /**
     * Starts a Raft group and a Raft service on the current node, using the given service factory.
     *
     * <p>Does not wait for the Raft log to be applied.
     *
     * @param nodeId Raft node ID.
     * @param configuration Peers and Learners of the Raft group.
     * @param lsnr Raft group listener.
     * @param eventsLsnr Raft group events listener.
     * @param factory Service factory.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    <T extends RaftGroupService> CompletableFuture<T> startRaftGroupNode(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupListener lsnr,
            RaftGroupEventsListener eventsLsnr,
            RaftServiceFactory<T> factory,
            RaftGroupOptionsConfigurer groupOptionsConfigurer
    ) throws NodeStoppingException;

    /**
     * Starts a Raft group and a Raft service on the current node.
     *
     * <p>Synchronously waits for the Raft log to be applied.
     *
     * @param nodeId Raft node ID.
     * @param configuration Peers and Learners of the Raft group.
     * @param lsnr Raft group listener.
     * @param eventsLsnr Raft group events listener.
     * @param groupOptionsConfigurer Configures raft log and snapshot storages.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    // FIXME: IGNITE-19047 Meta storage and cmg raft log re-application in async manner
    CompletableFuture<RaftGroupService> startRaftGroupNodeAndWaitNodeReadyFuture(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupListener lsnr,
            RaftGroupEventsListener eventsLsnr,
            RaftGroupOptionsConfigurer groupOptionsConfigurer
    ) throws NodeStoppingException;

    /**
     * Starts a Raft group and a Raft service on the current node.
     *
     * <p>Synchronously waits for the Raft log to be applied.
     *
     * @param nodeId Raft node ID.
     * @param configuration Peers and Learners of the Raft group.
     * @param lsnr Raft group listener.
     * @param eventsLsnr Raft group events listener.
     * @param disruptorConfiguration Configuration own (not shared) striped disruptor for FSMCaller service of raft node.
     * @param groupOptionsConfigurer Configures raft log and snapshot storages.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    // FIXME: IGNITE-19047 Meta storage and cmg raft log re-application in async manner
    CompletableFuture<RaftGroupService> startRaftGroupNodeAndWaitNodeReadyFuture(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupListener lsnr,
            RaftGroupEventsListener eventsLsnr,
            RaftNodeDisruptorConfiguration disruptorConfiguration,
            RaftGroupOptionsConfigurer groupOptionsConfigurer
    ) throws NodeStoppingException;

    /**
     * Starts a Raft group and a Raft service on the current node, using the given service factory.
     *
     * <p>Synchronously waits for the Raft log to be applied.
     *
     * @param nodeId Raft node ID.
     * @param configuration Peers and Learners of the Raft group.
     * @param lsnr Raft group listener.
     * @param eventsLsnr Raft group events listener.
     * @param factory Service factory.
     * @param groupOptionsConfigurer Configures raft log and snapshot storages.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    // FIXME: IGNITE-19047 Meta storage and cmg raft log re-application in async manner
    <T extends RaftGroupService> CompletableFuture<T> startRaftGroupNodeAndWaitNodeReadyFuture(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupListener lsnr,
            RaftGroupEventsListener eventsLsnr,
            RaftNodeDisruptorConfiguration disruptorConfiguration,
            RaftServiceFactory<T> factory,
            RaftGroupOptionsConfigurer groupOptionsConfigurer
    ) throws NodeStoppingException;

    /**
     * Stops a given local Raft node.
     *
     * @param nodeId Raft node ID.
     * @return {@code true} if the node has been stopped, {@code false} otherwise.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    boolean stopRaftNode(RaftNodeId nodeId) throws NodeStoppingException;

    /**
     * Stops all local nodes running the given Raft group.
     *
     * <p>This method is different from {@link #stopRaftNode} as it stops all nodes that belong to the same Raft group. This can happen
     * when a Peer and a Learner are started on the same Ignite node.
     *
     * @param groupId Raft group ID.
     * @return {@code true} if at least one node has been stopped, {@code false} otherwise.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    boolean stopRaftNodes(ReplicationGroupId groupId) throws NodeStoppingException;

    /**
     * Creates a Raft group service providing operations on a Raft group.
     *
     * @param groupId Raft group ID.
     * @param configuration Peers and Learners of the Raft group.
     * @return Future that will be completed with an instance of a Raft group service.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    @TestOnly
    CompletableFuture<RaftGroupService> startRaftGroupService(
            ReplicationGroupId groupId,
            PeersAndLearners configuration
    ) throws NodeStoppingException;

    /**
     * Creates a Raft group service providing operations on a Raft group, using the given factory.
     *
     * @param groupId Raft group ID.
     * @param configuration Peers and Learners of the Raft group.
     * @param factory Factory that should be used to create raft service.
     * @param commandsMarshaller Marshaller that should be used to serialize commands. {@code null} if default marshaller should be used.
     * @return Future that will be completed with an instance of a Raft group service.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    <T extends RaftGroupService> CompletableFuture<T> startRaftGroupService(
            ReplicationGroupId groupId,
            PeersAndLearners configuration,
            RaftServiceFactory<T> factory,
            @Nullable Marshaller commandsMarshaller
    ) throws NodeStoppingException;

    /**
     * Get a state accessor for the given group type.
     *
     * @param groupType Group type class.
     * @param groupListenerType Group listener class.
     * @return State accessor.
     *
     * @param <G> Group class.
     * @param <L> Group listener class.
     */
    <G extends ReplicationGroupId, L> RaftGroupStateProvider<G, L> getGroupStateProvider(
            Class<G> groupType,
            Class<L> groupListenerType
    );
}
