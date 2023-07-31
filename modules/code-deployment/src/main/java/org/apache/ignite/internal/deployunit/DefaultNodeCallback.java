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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.deployunit.metastore.DeploymentUnitStore;
import org.apache.ignite.internal.deployunit.metastore.NodeEventCallback;
import org.apache.ignite.internal.deployunit.metastore.status.UnitClusterStatus;
import org.apache.ignite.internal.deployunit.metastore.status.UnitNodeStatus;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Default implementation of {@link NodeEventCallback}.
 */
public class DefaultNodeCallback extends NodeEventCallback {
    private static final IgniteLogger LOG = Loggers.forClass(DefaultNodeCallback.class);

    private final DeploymentUnitStore deploymentUnitStore;

    private final DeployMessagingService messaging;

    private final FileDeployerService deployer;

    private final DeploymentUnitAcquiredWaiter undeployer;

    private final DownloadTracker tracker;

    private final ClusterManagementGroupManager cmgManager;

    private final String nodeName;

    /**
     * Constructor.
     *
     * @param deploymentUnitStore Deployment units store.
     * @param messaging Deployment messaging service.
     * @param deployer Deployment unit file system service.
     * @param undeployer Deployment unit undeployer.
     * @param cmgManager Cluster management group manager.
     * @param nodeName Node consistent ID.
     */
    public DefaultNodeCallback(
            DeploymentUnitStore deploymentUnitStore,
            DeployMessagingService messaging,
            FileDeployerService deployer,
            DeploymentUnitAcquiredWaiter undeployer,
            DownloadTracker tracker,
            ClusterManagementGroupManager cmgManager,
            String nodeName
    ) {
        this.deploymentUnitStore = deploymentUnitStore;
        this.messaging = messaging;
        this.deployer = deployer;
        this.undeployer = undeployer;
        this.tracker = tracker;
        this.cmgManager = cmgManager;
        this.nodeName = nodeName;
    }

    @Override
    public void onUploading(String id, Version version, List<UnitNodeStatus> holders) {
        tracker.track(id, version,
                () -> messaging.downloadUnitContent(id, version, new ArrayList<>(getDeployedNodeIds(holders)))
                        .thenCompose(content -> {
                            org.apache.ignite.internal.deployunit.DeploymentUnit unit = UnitContent.toDeploymentUnit(content);
                            return deployer.deploy(id, version, unit)
                                    .whenComplete((deployed, err) -> {
                                        try {
                                            unit.close();
                                        } catch (Exception e) {
                                            LOG.error("Failed to close deployment unit", e);
                                        }
                                    });
                        })
                        .thenApply(deployed -> {
                            if (deployed) {
                                return deploymentUnitStore.updateNodeStatus(nodeName, id, version, DEPLOYED);
                            }
                            return deployed;
                        })
        );
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
                    Set<String> nodes = snapshot.nodes().stream().map(LogicalNode::name).collect(Collectors.toSet());
                    boolean allRemoved = holders.stream()
                            .filter(nodeStatus -> nodes.contains(nodeStatus.nodeId()))
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
