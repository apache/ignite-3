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

import static java.util.concurrent.CompletableFuture.allOf;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitReadException;
import org.apache.ignite.internal.deployunit.message.DeployUnitMessageTypes;
import org.apache.ignite.internal.deployunit.message.DeploymentUnitFactory;
import org.apache.ignite.internal.deployunit.message.DownloadUnitRequest;
import org.apache.ignite.internal.deployunit.message.DownloadUnitResponse;
import org.apache.ignite.internal.deployunit.message.StopDeployRequest;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;

/**
 * Messaging service for deploy actions.
 */
public class DeployMessagingService {
    private static final IgniteLogger LOG = Loggers.forClass(DeployMessagingService.class);

    /** Channel type for code deployment. */
    static final ChannelType DEPLOYMENT_CHANNEL = new ChannelType((short) 2, "DeploymentUnits");

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
    private final DownloadTracker tracker;

    private final DeploymentUnitFactory messageFactory = new DeploymentUnitFactory();

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
            DownloadTracker tracker
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
                (message, sender, correlationId) -> {
                    if (message instanceof DownloadUnitRequest) {
                        processDownloadRequest((DownloadUnitRequest) message, sender, correlationId);
                    } else if (message instanceof StopDeployRequest) {
                        processStopDeployRequest((StopDeployRequest) message, sender, correlationId);
                    }
                });
    }

    /**
     * Download deployment unit content from randomly selected node.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @param nodes Nodes where unit deployed.
     * @return Downloaded deployment unit content.
     */
    CompletableFuture<UnitContent> downloadUnitContent(String id, Version version, Collection<String> nodes) {
        DownloadUnitRequest request = messageFactory.downloadUnitRequest()
                .id(id)
                .version(version.render())
                .build();

        InternalClusterNode clusterNode = resolveClusterNode(nodes);

        return clusterService.messagingService()
                .invoke(clusterNode, DEPLOYMENT_CHANNEL, request, Long.MAX_VALUE)
                .thenApply(message -> ((DownloadUnitResponse) message).unitContent());
    }

    private InternalClusterNode resolveClusterNode(Collection<String> nodes) {
        return nodes.stream().map(node -> clusterService.topologyService().getByConsistentId(node))
                .filter(Objects::nonNull)
                .findAny()
                .orElseThrow(() -> {
                    LOG.error("No any available node for download unit from {}", nodes);
                    return new DeploymentUnitReadException("No any available node for download unit from " + nodes);
                });
    }

    /**
     * Stop all in-progress deployment processes for deployment unit with provided id and version.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @return Future with stop result.
     */
    CompletableFuture<Void> stopInProgressDeploy(String id, Version version) {
        LOG.info("Stop in progress deploy for " + id + ":" + version);

        return cmgManager.logicalTopology()
                .thenCompose(topology -> {
                    StopDeployRequest request = messageFactory.stopDeployRequest()
                            .id(id)
                            .version(version.render())
                            .build();

                    CompletableFuture<?>[] sendFutures = topology.nodes().stream()
                            .map(node -> clusterService.messagingService().invoke(node, DEPLOYMENT_CHANNEL, request, Long.MAX_VALUE))
                            .toArray(CompletableFuture[]::new);

                    return allOf(sendFutures);
                });
    }

    private void processStopDeployRequest(StopDeployRequest request, InternalClusterNode sender, long correlationId) {
        tracker.cancelIfDownloading(request.id(), Version.parseVersion(request.version()));
        clusterService.messagingService()
                .respond(sender, messageFactory.stopDeployResponse().build(), correlationId);
    }

    private void processDownloadRequest(DownloadUnitRequest request, InternalClusterNode sender, long correlationId) {
        deployerService.getUnitContent(request.id(), Version.parseVersion(request.version()))
                .thenApply(content -> clusterService.messagingService()
                        .respond(sender,
                                messageFactory.downloadUnitResponse().unitContent(content).build(),
                                correlationId)
                );
    }
}
