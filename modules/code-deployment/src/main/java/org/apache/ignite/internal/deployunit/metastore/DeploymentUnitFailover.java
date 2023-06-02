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

import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.UPLOADING;

import java.util.Objects;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.network.ClusterService;

/**
 * Deployment unit failover.
 */
public class DeploymentUnitFailover {
    private final LogicalTopologyService logicalTopology;

    private final DeploymentUnitStore deploymentUnitStore;

    private final ClusterService clusterService;

    /**
     * Constructor.
     *
     * @param logicalTopology Logical topology service.
     * @param deploymentUnitStore Deployment units store.
     * @param clusterService Cluster service.
     */
    public DeploymentUnitFailover(
            LogicalTopologyService logicalTopology,
            DeploymentUnitStore deploymentUnitStore,
            ClusterService clusterService
    ) {
        this.logicalTopology = logicalTopology;
        this.deploymentUnitStore = deploymentUnitStore;
        this.clusterService = clusterService;
    }

    /**
     * Register {@link NodeEventCallback} as topology change callback.
     *
     * @param callback Node status callback.
     */
    public void registerTopologyChangeCallback(NodeEventCallback callback) {
        logicalTopology.addEventListener(new LogicalTopologyEventListener() {
            @Override
            public void onNodeJoined(LogicalNode joinedNode, LogicalTopologySnapshot newTopology) {
                String consistentId = joinedNode.name();
                if (Objects.equals(consistentId, getLocalNodeId())) {
                    deploymentUnitStore.getNodeStatuses(consistentId)
                            .thenAccept(nodeStatuses -> nodeStatuses.forEach(nodeStatus -> {
                                if (nodeStatus.status() == UPLOADING) {
                                    deploymentUnitStore.getAllNodes(nodeStatus.id(), nodeStatus.version())
                                            .thenAccept(nodes -> callback.onUploading(nodeStatus, nodes));
                                }
                            }));
                }
            }
        });
    }

    private String getLocalNodeId() {
        return clusterService.topologyService().localMember().name();
    }
}
