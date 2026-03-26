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
import static org.apache.ignite.internal.deployunit.UnitContent.toDeploymentUnit;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.deployunit.metastore.DeploymentUnitStore;
import org.apache.ignite.internal.deployunit.metastore.status.UnitNodeStatus;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Unit downloader.
 */
class UnitDownloader {
    private static final IgniteLogger LOG = Loggers.forClass(UnitDownloader.class);

    private final DeploymentUnitStore deploymentUnitStore;

    private final String nodeName;

    private final FileDeployerService deployer;

    private final DownloadTracker tracker;

    private final DeployMessagingService messaging;

    UnitDownloader(
            DeploymentUnitStore deploymentUnitStore,
            String nodeName,
            FileDeployerService deployer,
            DownloadTracker tracker,
            DeployMessagingService messaging
    ) {
        this.deploymentUnitStore = deploymentUnitStore;
        this.nodeName = nodeName;
        this.deployer = deployer;
        this.tracker = tracker;
        this.messaging = messaging;
    }

    /**
     * Downloads specified unit from any node where this unit is deployed from the specified collection of nodes to the local node, deploys
     * it and sets the node status to {@link DeploymentStatus#DEPLOYED}.
     *
     * @param statuses Collection of all node statuses for this unit.
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     */
    CompletableFuture<Boolean> downloadUnit(Collection<UnitNodeStatus> statuses, String id, Version version) {
        List<String> deployedNodes = statuses.stream()
                .filter(status -> status.status() == DEPLOYED)
                .map(UnitNodeStatus::nodeId)
                .collect(Collectors.toList());

        return downloadUnit(id, version, deployedNodes);
    }

    /**
     * Downloads specified unit from any node from the specified collection of nodes to the local node, deploys it and sets the node status
     * to {@link DeploymentStatus#DEPLOYED}.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @param nodes Nodes where the unit is deployed.
     */
    CompletableFuture<Boolean> downloadUnit(String id, Version version, Collection<String> nodes) {
        // Get the status again inside the tracker. There could be a race when another thread just completed the download, updated the
        // status to DEPLOYED and stopped tracking. In this case the tracked job will be started, but now we have outdated status and
        // we call downloadUnitContent again. When it completes and unit is deployed again, updating node status returns false since it's
        // already DEPLOYED from another thread. Subsequently IgniteDeployment.onDemandDeploy returns false and the job fails.
        return tracker.track(id, version, () -> deploymentUnitStore.getNodeStatus(nodeName, id, version)
                .thenCompose(status -> {
                    if (status.status() == DEPLOYED) {
                        return trueCompletedFuture();
                    }
                    return downloadUnitContent(id, version, nodes);
                })
        );
    }

    private CompletableFuture<Boolean> downloadUnitContent(String id, Version version, Collection<String> nodes) {
        LOG.info("Downloading unit {}:{} from nodes {}", id, version, nodes);
        return messaging.downloadUnitContent(id, version, nodes)
                .thenCompose(content -> {
                    DeploymentUnit unit = toDeploymentUnit(content);
                    return deployer.deploy(id, version, unit)
                            .whenComplete((deployed, throwable) -> {
                                try {
                                    unit.close();
                                } catch (Exception e) {
                                    LOG.error("Error closing deployment unit", e);
                                }
                            });
                })
                .thenCompose(deployed -> {
                    if (deployed) {
                        return deploymentUnitStore.updateNodeStatus(nodeName, id, version, DEPLOYED);
                    }
                    return falseCompletedFuture();
                });
    }
}
