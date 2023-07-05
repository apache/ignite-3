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

package org.apache.ignite.internal.deployunit.metastore;

import static org.apache.ignite.internal.deployunit.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.OBSOLETE;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.REMOVING;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.UPLOADING;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.deployunit.DeploymentStatus;
import org.apache.ignite.internal.deployunit.FileDeployerService;
import org.apache.ignite.internal.deployunit.metastore.status.UnitClusterStatus;
import org.apache.ignite.internal.deployunit.metastore.status.UnitNodeStatus;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Deployment unit failover.
 */
public class DeploymentUnitFailover {
    private static final IgniteLogger LOG = Loggers.forClass(DeploymentUnitFailover.class);

    private final LogicalTopologyService logicalTopology;

    private final DeploymentUnitStore deploymentUnitStore;

    private final FileDeployerService deployer;

    private final ClusterManagementGroupManager cmgManager;

    private final String nodeName;

    /**
     * Constructor.
     *
     * @param logicalTopology Logical topology service.
     * @param deploymentUnitStore Deployment units store.
     * @param deployer Deployment unit file system service.
     * @param cmgManager
     * @param nodeName Node consistent ID.
     */
    public DeploymentUnitFailover(
            LogicalTopologyService logicalTopology,
            DeploymentUnitStore deploymentUnitStore,
            FileDeployerService deployer,
            ClusterManagementGroupManager cmgManager,
            String nodeName
    ) {
        this.logicalTopology = logicalTopology;
        this.deploymentUnitStore = deploymentUnitStore;
        this.deployer = deployer;
        this.cmgManager = cmgManager;
        this.nodeName = nodeName;
    }

    /**
     * Register {@link NodeEventCallback} as topology change callback.
     *
     * @param nodeEventCallback Node status callback.
     * @param clusterEventCallback Cluster status callback.
     */
    public void registerTopologyChangeCallback(NodeEventCallback nodeEventCallback, ClusterEventCallback clusterEventCallback) {
        logicalTopology.addEventListener(new LogicalTopologyEventListener() {
            @Override
            public void onNodeJoined(LogicalNode joinedNode, LogicalTopologySnapshot newTopology) {
                String consistentId = joinedNode.name();
                LOG.info("onNodeJoined {}", consistentId);
                if (Objects.equals(consistentId, nodeName)) {
                    deploymentUnitStore.getNodeStatuses(consistentId)
                            .thenAccept(nodeStatuses -> nodeStatuses.forEach(unitNodeStatus ->
                                    deploymentUnitStore.getClusterStatus(unitNodeStatus.id(), unitNodeStatus.version())
                                            .thenAccept(unitClusterStatus ->
                                                    processStatus(unitClusterStatus, unitNodeStatus, nodeEventCallback))));
                }
            }

            @Override
            public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
                cmgManager.isCmgLeader().thenAccept(isLeader -> {
                    String leftNodeName = leftNode.name();
                    LOG.info("onNodeLeft " + leftNodeName + (isLeader ? " on leader" : ""));
                    if (isLeader) {
                        // When a node from the majority is down, another node from the CMG should be selected for majority.
                        deploymentUnitStore.getNodeStatuses(leftNodeName)
                                .thenAccept(statuses -> {
                                    LOG.info("Left node {} statuses: {}", leftNodeName, statuses);
                                    statuses.stream()
                                            .filter(nodeStatus -> nodeStatus.status() == UPLOADING)
                                            .forEach(nodeStatus -> checkNodeStatus(nodeStatus, leftNodeName));
                                });
                    }
                });
            }
        });
    }

    private void checkNodeStatus(UnitNodeStatus nodeStatus, String leftNodeName) {
        deploymentUnitStore.getClusterStatus(nodeStatus.id(), nodeStatus.version())
                .thenAccept(clusterStatus -> {
                    LOG.info("clusterStatus {}", clusterStatus);
                    if (clusterStatus.isMajority() && clusterStatus.initialNodesToDeploy().contains(leftNodeName)) {
                        Set<String> initialNodesToDeploy = new HashSet<>(clusterStatus.initialNodesToDeploy());

                        initialNodesToDeploy.remove(leftNodeName);

                        // Find new candidate for majority
                        cmgManager.peers().thenAccept(peers -> {
                            LOG.info("peers {}", peers);
                            Optional<String> newNode = peers.stream()
                                    .filter(node -> !initialNodesToDeploy.contains(node) && !leftNodeName.equals(node))
                                    .findAny();
                            if (newNode.isPresent()) {
                                String newNodeName = newNode.get();
                                initialNodesToDeploy.add(newNodeName);

                                LOG.info("updateClusterStatus {}", initialNodesToDeploy);
                                deploymentUnitStore.updateClusterStatus(nodeStatus.id(), nodeStatus.version(), initialNodesToDeploy)
                                        .thenAccept(success -> {
                                            LOG.info("createNodeStatus for {}", newNodeName);
                                            deploymentUnitStore.createNodeStatus(
                                                    newNodeName,
                                                    nodeStatus.id(),
                                                    nodeStatus.version(),
                                                    nodeStatus.opId(),
                                                    UPLOADING);
                                                }
                                        );
                            }
                        });
                    }
                });
    }

    private void processStatus(UnitClusterStatus unitClusterStatus, UnitNodeStatus unitNodeStatus, NodeEventCallback nodeEventCallback) {
        String id = unitNodeStatus.id();
        Version version = unitNodeStatus.version();

        if (unitClusterStatus == null) {
            undeploy(id, version);
            return;
        }

        if (checkAbaProblem(unitClusterStatus, unitNodeStatus)) {
            return;
        }

        DeploymentStatus nodeStatus = unitNodeStatus.status();
        switch (unitClusterStatus.status()) {
            case UPLOADING: // fallthrough
            case DEPLOYED:
                if (nodeStatus == UPLOADING) {
                    deploymentUnitStore.getAllNodeStatuses(id, version)
                            .thenAccept(nodes -> nodeEventCallback.onUpdate(unitNodeStatus, nodes));
                }
                break;
            case OBSOLETE:
                if (nodeStatus == DEPLOYED || nodeStatus == OBSOLETE) {
                    deploymentUnitStore.updateNodeStatus(nodeName, id, version, REMOVING);
                }
                break;
            case REMOVING:
                deploymentUnitStore.getAllNodeStatuses(id, version)
                        .thenAccept(nodes -> {
                            UnitNodeStatus status = new UnitNodeStatus(id, version, REMOVING, unitClusterStatus.opId(), nodeName);
                            nodeEventCallback.onUpdate(status, nodes);
                        });
                break;
            default:
                break;
        }
    }

    private void undeploy(String id, Version version) {
        deployer.undeploy(id, version)
                .thenAccept(success -> {
                    if (success) {
                        deploymentUnitStore.removeNodeStatus(nodeName, id, version);
                    }
                });
    }

    private boolean checkAbaProblem(UnitClusterStatus clusterStatus, UnitNodeStatus nodeStatus) {
        String id = nodeStatus.id();
        Version version = nodeStatus.version();
        if (clusterStatus.opId() != nodeStatus.opId()) {
            if (nodeStatus.status() == DEPLOYED) {
                undeploy(id, version);
            } else {
                deploymentUnitStore.removeNodeStatus(nodeName, id, version);
            }
            return true;
        }
        return false;
    }
}
