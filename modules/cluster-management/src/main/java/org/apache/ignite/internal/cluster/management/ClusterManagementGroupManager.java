/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cluster.management;

import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static org.apache.ignite.network.util.ClusterServiceUtils.resolveNodes;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.cluster.management.messages.CancelInitMessage;
import org.apache.ignite.internal.cluster.management.messages.ClusterStateMessage;
import org.apache.ignite.internal.cluster.management.messages.CmgInitMessage;
import org.apache.ignite.internal.cluster.management.messages.CmgMessageGroup;
import org.apache.ignite.internal.cluster.management.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.rest.InitCommandHandler;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.rest.RestComponent;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;

/**
 * Ignite component responsible for cluster initialization and managing the Cluster Management Raft Group.
 */
public class ClusterManagementGroupManager implements IgniteComponent {
    private static final IgniteLogger log = IgniteLogger.forClass(ClusterManagementGroupManager.class);

    /** CMG Raft group name. */
    private static final String CMG_RAFT_GROUP_NAME = "cmg_raft_group";

    /** Init REST endpoint path. */
    private static final String REST_ENDPOINT = "/management/v1/cluster/init";

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    private final ClusterService clusterService;

    private final Loza raftManager;

    private final RestComponent restModule;

    /** Handles cluster initialization flow. */
    private final ClusterInitializer clusterInitializer;

    private final CmgMessagesFactory msgFactory = new CmgMessagesFactory();

    private final CompletableFuture<Collection<String>> metastorageNodes = new CompletableFuture<>();

    /** Constructor. */
    public ClusterManagementGroupManager(ClusterService clusterService, Loza raftManager, RestComponent restModule) {
        this.clusterService = clusterService;
        this.raftManager = raftManager;
        this.restModule = restModule;
        this.clusterInitializer = new ClusterInitializer(clusterService);

        MessagingService messagingService = clusterService.messagingService();

        messagingService.addMessageHandler(CmgMessageGroup.class, (msg, addr, correlationId) -> {
            if (!busyLock.enterBusy()) {
                if (correlationId != null) {
                    messagingService.respond(addr, errorResponse(msgFactory, new NodeStoppingException()), correlationId);
                }

                return;
            }

            try {
                if (msg instanceof CancelInitMessage) {
                    handleCancelInit((CancelInitMessage) msg);
                } else if (msg instanceof CmgInitMessage) {
                    assert correlationId != null;

                    handleInit((CmgInitMessage) msg, addr, correlationId);
                } else if (msg instanceof ClusterStateMessage) {
                    handleClusterState((ClusterStateMessage) msg);
                }
            } catch (Exception e) {
                log.error("CMG message handling failed", e);

                if (correlationId != null) {
                    messagingService.respond(addr, errorResponse(msgFactory, e), correlationId);
                }
            } finally {
                busyLock.leaveBusy();
            }
        });
    }

    /**
     * Initializes the cluster that this node is present in.
     *
     * @param metaStorageNodeNames names of nodes that will host the Meta Storage.
     * @param cmgNodeNames names of nodes that will host the Cluster Management Group.
     */
    public void initCluster(Collection<String> metaStorageNodeNames, Collection<String> cmgNodeNames) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            clusterInitializer.initCluster(metaStorageNodeNames, cmgNodeNames).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInternalException("Interrupted while initializing the cluster", e);
        } catch (ExecutionException e) {
            throw new IgniteInternalException("Unable to initialize the cluster", e.getCause());
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void handleInit(CmgInitMessage msg, NetworkAddress addr, long correlationId) throws NodeStoppingException {
        List<ClusterNode> nodes = resolveNodes(clusterService, msg.cmgNodes());

        raftManager.prepareRaftGroup(CMG_RAFT_GROUP_NAME, nodes, CmgRaftGroupListener::new)
                .whenComplete((service, e) -> {
                    MessagingService messagingService = clusterService.messagingService();

                    if (e == null) {
                        ClusterNode leader = getLeader(service);

                        ClusterNode thisNode = clusterService.topologyService().localMember();

                        messagingService.respond(addr, successResponse(msgFactory), correlationId);

                        if (leader.equals(thisNode)) {
                            broadcastClusterState(msg.metaStorageNodes());
                        }
                    } else {
                        messagingService.respond(addr, errorResponse(msgFactory, e), correlationId);
                    }
                });
    }

    private void handleCancelInit(CancelInitMessage msg) throws NodeStoppingException {
        log.info("CMG initialization cancelled, reason: " + msg.reason());

        raftManager.stopRaftGroup(CMG_RAFT_GROUP_NAME);

        // TODO: drop the Raft storage as well, https://issues.apache.org/jira/browse/IGNITE-16471
    }

    private void handleClusterState(ClusterStateMessage msg) {
        metastorageNodes.complete(msg.metastorageNodes());
    }

    private static NetworkMessage successResponse(CmgMessagesFactory msgFactory) {
        log.info("CMG started successfully");

        return msgFactory.initCompleteMessage().build();
    }

    private void broadcastClusterState(Collection<String> metaStorageNodes) {
        NetworkMessage clusterStateMsg = msgFactory.clusterStateMessage()
                .metastorageNodes(metaStorageNodes)
                .build();

        clusterService.topologyService()
                .allMembers()
                .forEach(node -> clusterService.messagingService().send(node, clusterStateMsg));
    }

    private static NetworkMessage errorResponse(CmgMessagesFactory msgFactory, Throwable e) {
        log.error("Exception when starting the CMG", e);

        return msgFactory.initErrorMessage()
                .cause(e.getMessage())
                .build();
    }

    private ClusterNode getLeader(RaftGroupService raftService) {
        Peer leader = raftService.leader();

        assert leader != null;

        ClusterNode leaderNode = clusterService.topologyService().getByAddress(leader.address());

        assert leaderNode != null;

        return leaderNode;
    }

    @Override
    public void start() {
        restModule.registerHandlers(routes ->
                routes.post(REST_ENDPOINT, APPLICATION_JSON.toString(), new InitCommandHandler(clusterInitializer))
        );
    }

    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        raftManager.stopRaftGroup(CMG_RAFT_GROUP_NAME);
    }

    public CompletableFuture<Collection<String>> metaStorageNodes() {
        return metastorageNodes;
    }
}
