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

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.deployunit.message.DeployUnitMessageTypes;
import org.apache.ignite.internal.deployunit.message.DownloadUnitRequest;
import org.apache.ignite.internal.deployunit.message.DownloadUnitRequestImpl;
import org.apache.ignite.internal.deployunit.message.DownloadUnitResponse;
import org.apache.ignite.internal.deployunit.message.DownloadUnitResponseImpl;
import org.apache.ignite.internal.deployunit.message.StopDeployRequest;
import org.apache.ignite.internal.deployunit.message.StopDeployRequestImpl;
import org.apache.ignite.internal.deployunit.message.StopDeployResponseImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.network.ChannelType;
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
    private final DownloadTracker tracker;

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
                (message, senderConsistentId, correlationId) -> {
                    if (message instanceof DownloadUnitRequest) {
                        processDownloadRequest((DownloadUnitRequest) message, senderConsistentId, correlationId);
                    } else if (message instanceof StopDeployRequest) {
                        processStopDeployRequest((StopDeployRequest) message, senderConsistentId, correlationId);
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
    CompletableFuture<UnitContent> downloadUnitContent(String id, Version version, List<String> nodes) {
        String node = nodes.get(new Random().nextInt(nodes.size()));
        DownloadUnitRequest request = DownloadUnitRequestImpl.builder()
                .id(id)
                .version(version.render())
                .build();

        return clusterService.messagingService()
                .invoke(clusterService.topologyService().getByConsistentId(node), DEPLOYMENT_CHANNEL, request, Long.MAX_VALUE)
                .thenApply(message -> ((DownloadUnitResponse) message).unitContent());
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

    private void processStopDeployRequest(StopDeployRequest request, String senderConsistentId, long correlationId) {
        tracker.cancelIfDownloading(request.id(), Version.parseVersion(request.version()));
        clusterService.messagingService()
                .respond(senderConsistentId, StopDeployResponseImpl.builder().build(), correlationId);

    }

    private void processDownloadRequest(DownloadUnitRequest request, String senderConsistentId, long correlationId) {
        deployerService.getUnitContent(request.id(), Version.parseVersion(request.version()))
                .thenApply(content -> clusterService.messagingService()
                        .respond(senderConsistentId,
                                DownloadUnitResponseImpl.builder().unitContent(content).build(),
                                correlationId)
                );
    }
}
