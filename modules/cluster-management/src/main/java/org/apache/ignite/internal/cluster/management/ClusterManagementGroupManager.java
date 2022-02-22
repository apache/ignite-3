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

import static org.apache.ignite.internal.cluster.management.Utils.resolveNodes;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.cluster.management.messages.CancelInitMessage;
import org.apache.ignite.internal.cluster.management.messages.CmgInitMessage;
import org.apache.ignite.internal.cluster.management.messages.InitMessageGroup;
import org.apache.ignite.internal.cluster.management.messages.InitMessagesFactory;
import org.apache.ignite.internal.cluster.management.rest.InitCommandHandler;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.rest.RestModule;
import org.apache.ignite.internal.rest.routes.Route;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.raft.client.Peer;

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

    private final RestModule restModule;

    /** Handles cluster initialization flow. */
    private final ClusterInitializer clusterInitializer;

    /** Constructor. */
    public ClusterManagementGroupManager(ClusterService clusterService, Loza raftManager, RestModule restModule) {
        this.clusterService = clusterService;
        this.raftManager = raftManager;
        this.restModule = restModule;
        this.clusterInitializer = new ClusterInitializer(clusterService);

        MessagingService messagingService = clusterService.messagingService();

        var msgFactory = new InitMessagesFactory();

        messagingService.addMessageHandler(InitMessageGroup.class, (msg, addr, correlationId) -> {
            if (!busyLock.enterBusy()) {
                if (correlationId != null) {
                    messagingService.respond(addr, errorResponse(msgFactory, new NodeStoppingException()), correlationId);
                }

                return;
            }

            try {
                if (msg instanceof CancelInitMessage) {
                    log.info("CMG initialization cancelled, reason: " + ((CancelInitMessage) msg).reason());

                    raftManager.stopRaftGroup(CMG_RAFT_GROUP_NAME);

                    // TODO: drop the Raft storage as well, https://issues.apache.org/jira/browse/IGNITE-16471
                } else if (msg instanceof CmgInitMessage) {
                    assert correlationId != null;

                    String[] nodeIds = ((CmgInitMessage) msg).cmgNodes();

                    List<ClusterNode> nodes = resolveNodes(clusterService, Arrays.asList(nodeIds));

                    raftManager.prepareRaftGroup(CMG_RAFT_GROUP_NAME, nodes, CmgRaftGroupListener::new)
                            .whenComplete((service, e) -> {
                                if (e == null) {
                                    Peer leader = service.leader();

                                    assert leader != null;

                                    messagingService.respond(addr, leaderElectedResponse(msgFactory, leader), correlationId);
                                } else {
                                    messagingService.respond(addr, errorResponse(msgFactory, e), correlationId);
                                }
                            });
                }
            } catch (Exception e) {
                log.error("Exception when initializing the CMG", e);

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
    public Leaders initCluster(Collection<String> metaStorageNodeNames, Collection<String> cmgNodeNames) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            return clusterInitializer.initCluster(metaStorageNodeNames, cmgNodeNames).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInternalException("Interrupted while initializing the cluster", e);
        } catch (ExecutionException e) {
            throw new IgniteInternalException("Unable to initialize the cluster", e.getCause());
        } finally {
            busyLock.leaveBusy();
        }
    }

    private static NetworkMessage errorResponse(InitMessagesFactory msgFactory, Throwable e) {
        log.error("Exception when starting the CMG", e);

        return msgFactory.initErrorMessage()
                .cause(e.getMessage())
                .build();
    }

    private NetworkMessage leaderElectedResponse(InitMessagesFactory msgFactory, Peer leader) {
        ClusterNode leaderNode = clusterService.topologyService().getByAddress(leader.address());

        assert leaderNode != null;

        log.info("CMG leader elected: " + leaderNode.name());

        return msgFactory.initCompleteMessage()
                .leaderName(leaderNode.name())
                .build();
    }

    @Override
    public void start() {
        restModule.addRoute(new Route(
                REST_ENDPOINT,
                HttpMethod.POST,
                HttpHeaderValues.APPLICATION_JSON.toString(),
                new InitCommandHandler(clusterInitializer)
        ));
    }

    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        raftManager.stopRaftGroup(CMG_RAFT_GROUP_NAME);
    }
}
