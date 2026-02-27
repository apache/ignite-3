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

import static org.apache.ignite.internal.deployunit.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.REMOVING;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.deployunit.metastore.DeploymentUnitStore;
import org.apache.ignite.internal.deployunit.metastore.NodeEventCallback;
import org.apache.ignite.internal.deployunit.metastore.status.UnitClusterStatus;
import org.apache.ignite.internal.deployunit.metastore.status.UnitNodeStatus;

/**
 * Default implementation of {@link NodeEventCallback}.
 */
public class DefaultNodeCallback extends NodeEventCallback {
    private final DeploymentUnitStore deploymentUnitStore;

    private final DeploymentUnitAcquiredWaiter undeployer;

    private final UnitDownloader downloader;

    private final ClusterManagementGroupManager cmgManager;

    /**
     * Constructor.
     *
     * @param deploymentUnitStore Deployment units store.
     * @param undeployer Deployment unit undeployer.
     * @param downloader Unit downloader.
     * @param cmgManager Cluster management group manager.
     */
    DefaultNodeCallback(
            DeploymentUnitStore deploymentUnitStore,
            DeploymentUnitAcquiredWaiter undeployer,
            UnitDownloader downloader,
            ClusterManagementGroupManager cmgManager
    ) {
        this.deploymentUnitStore = deploymentUnitStore;
        this.undeployer = undeployer;
        this.downloader = downloader;
        this.cmgManager = cmgManager;
    }

    @Override
    public void onUploading(String id, Version version, List<UnitNodeStatus> holders) {
        downloader.downloadUnit(id, version, getDeployedNodeIds(holders));
    }

    @Override
    public void onDeploy(String id, Version version, List<UnitNodeStatus> holders) {
        Set<String> nodeIds = getDeployedNodeIds(holders);
        deploymentUnitStore.getClusterStatus(id, version)
                .thenApply(UnitClusterStatus::initialNodesToDeploy)
                .thenApply(nodeIds::containsAll)
                .thenAccept(allRequiredDeployed -> {
                    if (allRequiredDeployed) {
                        deploymentUnitStore.updateClusterStatus(id, version, DEPLOYED);
                    }
                });
    }

    @Override
    public void onObsolete(String id, Version version, List<UnitNodeStatus> holders) {
        undeployer.submitToAcquireRelease(new DeploymentUnit(id, version));
    }

    @Override
    public void onRemoving(String id, Version version, List<UnitNodeStatus> holders) {
        cmgManager.logicalTopology()
                .thenAccept(snapshot -> {
                    boolean allRemoved = holders.stream()
                            .filter(nodeStatus -> snapshot.hasNode(nodeStatus.nodeId()))
                            .allMatch(nodeStatus -> nodeStatus.status() == REMOVING);

                    if (allRemoved) {
                        deploymentUnitStore.updateClusterStatus(id, version, REMOVING);
                    }
                });
    }

    /**
     * Returns a set of node IDs where unit is deployed.
     *
     * @param holders List of unit node statuses.
     * @return Set of node IDs where unit is deployed.
     */
    private static Set<String> getDeployedNodeIds(List<UnitNodeStatus> holders) {
        return holders.stream()
                .filter(status -> status.status() == DEPLOYED)
                .map(UnitNodeStatus::nodeId)
                .collect(Collectors.toSet());
    }
}
