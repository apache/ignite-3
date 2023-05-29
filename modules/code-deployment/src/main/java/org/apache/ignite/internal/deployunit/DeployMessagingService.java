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

package org.apache.ignite.internal.deployunit;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.deployunit.message.DeployUnitMessageTypes;
import org.apache.ignite.internal.deployunit.message.DeployUnitRequest;
import org.apache.ignite.internal.deployunit.message.DeployUnitRequestImpl;
import org.apache.ignite.internal.deployunit.message.DeployUnitResponse;
import org.apache.ignite.internal.deployunit.message.DeployUnitResponseImpl;
import org.apache.ignite.internal.deployunit.message.StopDeployRequest;
import org.apache.ignite.internal.deployunit.message.StopDeployRequestImpl;
import org.apache.ignite.internal.deployunit.message.StopDeployResponseImpl;
import org.apache.ignite.internal.deployunit.message.UndeployUnitRequest;
import org.apache.ignite.internal.deployunit.message.UndeployUnitRequestImpl;
import org.apache.ignite.internal.deployunit.message.UndeployUnitResponseImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.network.ChannelType;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;

/**
 * Messaging service for deploy actions.
 */
public class DeployMessagingService {
    private static final IgniteLogger LOG = Loggers.forClass(DeployMessagingService.class);

    private static final ChannelType DEPLOYMENT_CHANNEL = ChannelType.register((short) 1, "DeploymentUnits");

    /**
     * Cluster service.
     */
    private final ClusterService clusterService;

    /**
     * Cluster management group manager.
     */
    private final ClusterManagementGroupManager cmgManager;

    /**
     * File deployer service.
     */
    private final FileDeployerService deployerService;

    /**
     * Tracker of deploy actions.
     */
    private final DeployTracker tracker;

    /**
     * Constructor.
     *
     * @param clusterService Cluster service.
     * @param cmgManager CMG manager.
     * @param deployerService File deploying service.
     * @param tracker Deploy action tracker.
     */
    public DeployMessagingService(
            ClusterService clusterService,
            ClusterManagementGroupManager cmgManager,
            FileDeployerService deployerService,
            DeployTracker tracker
    ) {
        this.clusterService = clusterService;
        this.cmgManager = cmgManager;
        this.deployerService = deployerService;
        this.tracker = tracker;
    }

    /**
     * Subscribe to all deployment messages.
     */
    public void subscribe() {
        clusterService.messagingService().addMessageHandler(DeployUnitMessageTypes.class,
                (message, senderConsistentId, correlationId) -> {
                    if (message instanceof DeployUnitRequest) {
                        processDeployRequest((DeployUnitRequest) message, senderConsistentId, correlationId);
                    } else if (message instanceof UndeployUnitRequest) {
                        processUndeployRequest((UndeployUnitRequest) message, senderConsistentId, correlationId);
                    } else if (message instanceof StopDeployRequest) {
                        processStopDeployRequest((StopDeployRequest) message, senderConsistentId, correlationId);
                    }
                });
    }

    /**
     * Start deployment process to all nodes from CMG group.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @param unitContent Deployment unit file names and content.
     * @param nodeId Node consistent identifier.
     * @return Future with deployment result.
     */
    public CompletableFuture<Boolean> startDeployAsyncToNode(
            String id,
            Version version,
            Map<String, byte[]> unitContent,
            String nodeId
    ) {
        DeployUnitRequest request = DeployUnitRequestImpl.builder()
                .id(id)
                .version(version.render())
                .unitContent(unitContent)
                .build();

        return requestDeploy(clusterService.topologyService().getByConsistentId(nodeId), request);
    }

    /**
     * Stop all in-progress deployment processes for deployment unit with provided id and version.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @return Future with stop result.
     */
    public CompletableFuture<Void> stopInProgressDeploy(String id, Version version) {
        LOG.info("Stop in progress deploy for " + id + ":" + version);
        return CompletableFuture.allOf(cmgManager.logicalTopology()
                .thenApply(topology -> topology.nodes().stream().map(node ->
                                clusterService.messagingService()
                                        .invoke(node,
                                                DEPLOYMENT_CHANNEL,
                                                StopDeployRequestImpl
                                                        .builder()
                                                        .id(id)
                                                        .version(version.render())
                                                        .build(),
                                                Long.MAX_VALUE)
                        ).toArray(CompletableFuture[]::new)));
    }

    /**
     * Start undeploy process from provided node with provided id and version.
     *
     * @param node Cluster node.
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @return Future with undeploy result.
     */
    public CompletableFuture<Void> undeploy(ClusterNode node, String id, Version version) {
        return clusterService.messagingService()
                .invoke(node,
                        DEPLOYMENT_CHANNEL,
                        UndeployUnitRequestImpl.builder()
                                .id(id)
                                .version(version.render())
                                .build(),
                        Long.MAX_VALUE
                ).thenAccept(message ->
                        LOG.info("Undeploy unit " + id + ":" + version + " from node " + node + " finished"));
    }

    private CompletableFuture<Boolean> requestDeploy(ClusterNode clusterNode, DeployUnitRequest request) {
        return clusterService.messagingService()
                .invoke(clusterNode, DEPLOYMENT_CHANNEL, request, Long.MAX_VALUE)
                .thenCompose(message -> {
                    boolean success = ((DeployUnitResponse) message).success();
                    if (!success) {
                        LOG.error("Failed to deploy unit " + request.id() + ":" + request.version()
                                + " to node " + clusterNode);
                    }
                    return CompletableFuture.completedFuture(success);
                });
    }

    private void processStopDeployRequest(StopDeployRequest request, String senderConsistentId, long correlationId) {
        tracker.cancelIfDeploy(request.id(), Version.parseVersion(request.version()));
        clusterService.messagingService()
                .respond(senderConsistentId, StopDeployResponseImpl.builder().build(), correlationId);

    }

    private void processDeployRequest(DeployUnitRequest executeRequest, String senderConsistentId, long correlationId) {
        String id = executeRequest.id();
        String version = executeRequest.version();
        tracker.track(id, Version.parseVersion(version),
                deployerService.deploy(id, version, executeRequest.unitContent())
                        .thenCompose(success -> clusterService.messagingService().respond(
                                senderConsistentId,
                                DEPLOYMENT_CHANNEL,
                                DeployUnitResponseImpl.builder().success(success).build(),
                                correlationId)
                        )
        );
    }

    private void processUndeployRequest(UndeployUnitRequest executeRequest, String senderConsistentId, long correlationId) {
        LOG.info("Start to undeploy " + executeRequest.id() + " with version " + executeRequest.version() + " from "
                + clusterService.topologyService().localMember().name());
        deployerService.undeploy(executeRequest.id(), executeRequest.version())
                .thenRun(() -> clusterService.messagingService()
                        .respond(senderConsistentId, UndeployUnitResponseImpl.builder().build(), correlationId));
    }
}
