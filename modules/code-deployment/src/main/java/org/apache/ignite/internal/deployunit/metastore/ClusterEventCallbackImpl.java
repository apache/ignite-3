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

import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.REMOVING;

import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.deployunit.FileDeployerService;
import org.apache.ignite.internal.deployunit.metastore.status.UnitClusterStatus;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/** Listener of deployment unit cluster status changes. */
public class ClusterEventCallbackImpl implements ClusterEventCallback {
    private static final IgniteLogger LOG = Loggers.forClass(ClusterEventCallbackImpl.class);

    private final DeploymentUnitStore deploymentUnitStore;
    private final FileDeployerService deployerService;
    private final ClusterManagementGroupManager cmgManager;
    private final String nodeName;

    /**
     * Constructor.
     *
     * @param deploymentUnitStore Deployment units store.
     * @param deployerService Deployment unit file system service.
     * @param cmgManager Cluster management group manager.
     * @param nodeName Node consistent ID.
     */
    public ClusterEventCallbackImpl(
            DeploymentUnitStore deploymentUnitStore,
            FileDeployerService deployerService,
            ClusterManagementGroupManager cmgManager,
            String nodeName
    ) {
        this.deploymentUnitStore = deploymentUnitStore;
        this.deployerService = deployerService;
        this.cmgManager = cmgManager;
        this.nodeName = nodeName;
    }

    @Override
    public void onRemoving(UnitClusterStatus status) {
        String id = status.id();
        Version version = status.version();
        // Now the deployment unit can be removed from each target node and, after it, remove corresponding status records.
        deploymentUnitStore.getNodeStatus(nodeName, id, version).whenComplete((nodeStatus, err) -> {
            LOG.info("getNodeStatus {} err {}", nodeStatus, err);
            if (nodeStatus != null && nodeStatus.status() == REMOVING) {
                undeploy(id, version);
            }
        });
    }

    private void undeploy(String id, Version version) {
        deployerService.undeploy(id, version).whenComplete((success, err) -> {
            LOG.info("undeploy {} err {}", success, err);
            if (success) {
                deploymentUnitStore.removeNodeStatus(nodeName, id, version).whenComplete((successRemove, errRemove) -> {
                    LOG.info("removeNodeStatus {} err {}", successRemove, err);
                    if (successRemove) {
                        removeClusterStatus(id, version);
                    }
                });
            }
        });
    }

    private void removeClusterStatus(String id, Version version) {
        cmgManager.logicalTopology().whenComplete((logicalTopology, err) -> {
            LOG.info("logicalTopology {} err {}", logicalTopology, err);
            Set<String> logicalNodes = logicalTopology.nodes().stream()
                    .map(LogicalNode::name)
                    .collect(Collectors.toSet());
            deploymentUnitStore.getAllNodes(id, version).whenComplete((nodes, nodesErr) -> {
                LOG.info("getAllNodes {} err {}", nodesErr, nodesErr);
                boolean emptyTopology = nodes.stream()
                        .filter(logicalNodes::contains)
                        .findAny()
                        .isEmpty();
                if (emptyTopology) {
                    deploymentUnitStore.removeClusterStatus(id, version).whenComplete((success, removeErr) -> {
                        LOG.info("removeClusterStatus {} err {}", success, removeErr);
                    });
                }
            });
        });
    }
}
