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

import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.deployunit.DeploymentStatus;
import org.apache.ignite.internal.deployunit.FileDeployerService;
import org.apache.ignite.internal.deployunit.metastore.status.UnitClusterStatus;
import org.apache.ignite.internal.deployunit.metastore.status.UnitNodeStatus;

/**
 * Deployment unit failover.
 */
public class DeploymentUnitFailover {
    private final LogicalTopologyService logicalTopology;

    private final DeploymentUnitStore deploymentUnitStore;

    private final FileDeployerService deployer;

    private final String nodeName;

    /**
     * Constructor.
     *
     * @param logicalTopology Logical topology service.
     * @param deploymentUnitStore Deployment units store.
     * @param deployer Deployment unit file system service.
     * @param nodeName Node consistent ID.
     */
    public DeploymentUnitFailover(
            LogicalTopologyService logicalTopology,
            DeploymentUnitStore deploymentUnitStore,
            FileDeployerService deployer,
            String nodeName
    ) {
        this.logicalTopology = logicalTopology;
        this.deploymentUnitStore = deploymentUnitStore;
        this.deployer = deployer;
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
                if (Objects.equals(consistentId, nodeName)) {
                    deploymentUnitStore.getNodeStatuses(consistentId)
                            .thenAccept(nodeStatuses -> nodeStatuses.forEach(unitNodeStatus ->
                                    deploymentUnitStore.getClusterStatus(unitNodeStatus.id(), unitNodeStatus.version())
                                            .thenAccept(unitClusterStatus ->
                                                    processStatus(unitClusterStatus, unitNodeStatus, nodeEventCallback))));
                }
            }
        });
    }

    private void processStatus(UnitClusterStatus unitClusterStatus, UnitNodeStatus unitNodeStatus, NodeEventCallback nodeEventCallback) {
        String id = unitNodeStatus.id();
        Version version = unitNodeStatus.version();

        if (unitClusterStatus == null) {
            undeploy(id, version, unitNodeStatus.opId());
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

    private void undeploy(String id, Version version, UUID opId) {
        deployer.undeploy(id, version)
                .thenAccept(success -> {
                    if (success) {
                        deploymentUnitStore.removeNodeStatus(nodeName, id, version, opId);
                    }
                });
    }

    private boolean checkAbaProblem(UnitClusterStatus clusterStatus, UnitNodeStatus nodeStatus) {
        String id = nodeStatus.id();
        Version version = nodeStatus.version();
        UUID opId = nodeStatus.opId();
        if (!Objects.equals(clusterStatus.opId(), opId)) {
            if (nodeStatus.status() == DEPLOYED) {
                undeploy(id, version, opId);
            } else {
                deploymentUnitStore.removeNodeStatus(nodeName, id, version, opId);
            }
            return true;
        }
        return false;
    }
}
