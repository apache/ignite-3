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

import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.DEPLOYED;

import java.util.List;
import org.apache.ignite.internal.deployunit.metastore.DeploymentUnitStore;
import org.apache.ignite.internal.deployunit.metastore.NodeEventCallback;
import org.apache.ignite.internal.deployunit.metastore.status.UnitClusterStatus;
import org.apache.ignite.internal.deployunit.metastore.status.UnitNodeStatus;
import org.apache.ignite.network.ClusterService;

/**
 * Default implementation of {@link NodeEventCallback}.
 */
public class DefaultNodeCallback implements NodeEventCallback {
    private final DeploymentUnitStore deploymentUnitStore;

    private final DeployMessagingService messaging;

    private final FileDeployerService deployer;

    private final ClusterService clusterService;

    /**
     * Constructor.
     *
     * @param deploymentUnitStore Deployment units store.
     * @param messaging Deployment messaging service.
     * @param deployer Deployment unit file system service.
     * @param clusterService Cluster service.
     */
    public DefaultNodeCallback(
            DeploymentUnitStore deploymentUnitStore,
            DeployMessagingService messaging,
            FileDeployerService deployer,
            ClusterService clusterService
    ) {
        this.deploymentUnitStore = deploymentUnitStore;
        this.messaging = messaging;
        this.deployer = deployer;
        this.clusterService = clusterService;
    }

    @Override
    public void onUploading(UnitNodeStatus status, List<String> holders) {
        messaging.downloadUnitContent(status.id(), status.version(), holders)
                .thenCompose(content -> deployer.deploy(status.id(), status.version(), content))
                .thenApply(deployed -> {
                    if (deployed) {
                        return deploymentUnitStore.updateNodeStatus(
                                getLocalNodeId(),
                                status.id(),
                                status.version(),
                                DEPLOYED);
                    }
                    return deployed;
                });
    }

    @Override
    public void onDeploy(UnitNodeStatus status, List<String> holders) {
        deploymentUnitStore.getClusterStatus(status.id(), status.version())
                .thenApply(UnitClusterStatus::initialNodesToDeploy)
                .thenApply(holders::containsAll)
                .thenAccept(allRequiredDeployed -> {
                    if (allRequiredDeployed) {
                        deploymentUnitStore.updateClusterStatus(status.id(), status.version(), DEPLOYED);
                    }
                });
    }

    private String getLocalNodeId() {
        return clusterService.topologyService().localMember().name();
    }
}
