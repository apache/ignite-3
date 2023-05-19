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
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.OBSOLETE;
import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.REMOVING;
import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.UPLOADING;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.deployunit.UnitStatuses.UnitStatusesBuilder;
import org.apache.ignite.internal.deployunit.configuration.DeploymentConfiguration;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitAlreadyExistsException;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitReadException;
import org.apache.ignite.internal.deployunit.metastore.DeploymentUnitStore;
import org.apache.ignite.internal.deployunit.metastore.DeploymentUnitStoreImpl;
import org.apache.ignite.internal.deployunit.metastore.status.UnitClusterStatus;
import org.apache.ignite.internal.deployunit.metastore.status.UnitNodeStatus;
import org.apache.ignite.internal.deployunit.version.Version;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.network.ClusterService;

/**
 * Deployment manager implementation.
 */
public class DeploymentManagerImpl implements IgniteDeployment {

    private static final IgniteLogger LOG = Loggers.forClass(DeploymentManagerImpl.class);

    /**
     * Node working directory.
     */
    private final Path workDir;

    /**
     * Deployment configuration.
     */
    private final DeploymentConfiguration configuration;

    /**
     * Cluster management group manager.
     */
    private final ClusterManagementGroupManager cmgManager;

    /**
     * Cluster service.
     */
    private final ClusterService clusterService;

    /**
     * Deploy messaging service.
     */
    private final DeployMessagingService messaging;

    /**
     * File deployer.
     */
    private final FileDeployerService deployer;

    /**
     * Deployment units metastore service.
     */
    private final DeploymentUnitStore metastore;

    /**
     * Deploy tracker.
     */
    private final DeployTracker tracker;

    /**
     * Constructor.
     *
     * @param clusterService Cluster service.
     * @param metaStorage Meta storage.
     * @param workDir Node working directory.
     * @param configuration Deployment configuration.
     * @param cmgManager Cluster management group manager.
     */
    public DeploymentManagerImpl(ClusterService clusterService,
            MetaStorageManager metaStorage,
            Path workDir,
            DeploymentConfiguration configuration,
            ClusterManagementGroupManager cmgManager) {
        this.clusterService = clusterService;
        this.configuration = configuration;
        this.cmgManager = cmgManager;
        this.workDir = workDir;
        tracker = new DeployTracker();
        deployer = new FileDeployerService();
        messaging = new DeployMessagingService(clusterService, cmgManager, deployer, tracker);
        metastore = new DeploymentUnitStoreImpl(metaStorage,
                () -> this.clusterService.topologyService().localMember().name(),
                this::onUnitRegister);
    }

    private void onUnitRegister(UnitNodeStatus status, Set<String> deployedNodes) {
        if (status.status() == UPLOADING) {
            messaging.downloadUnitContent(status.id(), status.version(), new ArrayList<>(deployedNodes))
                    .thenCompose(content -> deployer.deploy(status.id(), status.version().render(), content))
                    .thenApply(deployed -> {
                        if (deployed) {
                            return metastore.updateNodeStatus(
                                    status.id(),
                                    status.version(),
                                    clusterService.topologyService().localMember().name(),
                                    DEPLOYED);
                        }
                        return deployed;
                    });
        } else if (status.status() == DEPLOYED) {
            metastore.getClusterStatus(status.id(), status.version())
                    .thenApply(UnitClusterStatus::initialNodesToDeploy)
                    .thenApply(deployedNodes::containsAll)
                    .thenAccept(allRequiredDeployed -> {
                        if (allRequiredDeployed) {
                            metastore.updateClusterStatus(status.id(), status.version(), DEPLOYED);
                        }
                    });
        }
    }

    @Override
    public CompletableFuture<Boolean> deployAsync(String id, Version version, boolean force, DeploymentUnit deploymentUnit) {
        checkId(id);
        Objects.requireNonNull(version);
        Objects.requireNonNull(deploymentUnit);

        return cmgManager.cmgNodes()
                .thenCompose(cmg -> metastore.createClusterStatus(id, version, cmg))
                .thenCompose(success -> {
                    if (success) {
                        return doDeploy(id, version, deploymentUnit);
                    } else {
                        if (force) {
                            return undeployAsync(id, version)
                                    .thenCompose(v -> deployAsync(id, version, deploymentUnit));
                        }
                        LOG.warn("Failed to deploy meta of unit " + id + ":" + version + " to metastore. "
                                + "Already exists.");
                        return failedFuture(
                                new DeploymentUnitAlreadyExistsException(id,
                                        "Unit " + id + ":" + version + " already exists"));
                    }

                });
    }

    private CompletableFuture<Boolean> doDeploy(String id, Version version, DeploymentUnit deploymentUnit) {
        UnitContent unitContent;
        try {
            unitContent = UnitContent.readContent(deploymentUnit);
        } catch (DeploymentUnitReadException e) {
            LOG.error("Error reading deployment unit content", e);
            return failedFuture(e);
        }
        return tracker.track(id, version, deployToLocalNode(id, version, unitContent)
                .thenApply(completed -> {
                    if (completed) {
                        cmgManager.cmgNodes().thenAccept(nodes ->
                                nodes.forEach(node -> metastore.createNodeStatus(id, version, node)));
                    }
                    return completed;
                })
        );
    }

    private CompletableFuture<Boolean> deployToLocalNode(String id, Version version, UnitContent unitContent) {
        return deployer.deploy(id, version.render(), unitContent)
                .thenCompose(deployed -> {
                    if (deployed) {
                        String nodeId = clusterService.topologyService().localMember().name();
                        return metastore.createNodeStatus(id, version, nodeId, DEPLOYED);
                    }
                    return completedFuture(false);
                });
    }

    @Override
    public CompletableFuture<Boolean> undeployAsync(String id, Version version) {
        checkId(id);
        Objects.requireNonNull(version);

        return messaging.stopInProgressDeploy(id, version)
                .thenCompose(v -> metastore.updateClusterStatus(id, version, OBSOLETE))
                .thenCompose(success -> {
                    if (success) {
                        //TODO: Check unit usages here. If unit used in compute task we cannot just remove it.
                        return metastore.updateClusterStatus(id, version, REMOVING);
                    }
                    return completedFuture(false);
                })
                .thenCompose(success -> {
                    if (success) {
                        return cmgManager.logicalTopology();
                    }
                    return failedFuture(new DeploymentUnitNotFoundException(
                            "Unit " + id + " with version " + version + " doesn't exist"));
                }).thenCompose(logicalTopologySnapshot -> allOf(
                        logicalTopologySnapshot.nodes().stream()
                                .map(node -> messaging.undeploy(node, id, version))
                                .toArray(CompletableFuture[]::new))
                ).thenCompose(unused -> metastore.remove(id, version));
    }

    @Override
    public CompletableFuture<List<UnitStatuses>> unitsAsync() {
        return metastore.getAllClusterStatuses()
                .thenApply(DeploymentManagerImpl::map);
    }

    @Override
    public CompletableFuture<List<Version>> versionsAsync(String id) {
        checkId(id);

        return metastore.getClusterStatuses(id)
                .thenApply(statuses -> statuses.stream().map(UnitClusterStatus::version)
                        .sorted()
                        .collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<UnitStatuses> statusAsync(String id) {
        checkId(id);
        return metastore.getClusterStatuses(id)
                .thenApply(statuses -> {
                    UnitStatusesBuilder builder = UnitStatuses.builder(id);
                    for (UnitClusterStatus status : statuses) {
                        builder.append(status.version(), status.status());
                    }

                    return builder.build();
                });
    }

    @Override
    public CompletableFuture<List<UnitStatuses>> findUnitByConsistentIdAsync(String consistentId) {
        Objects.requireNonNull(consistentId);

        return metastore.findAllByNodeConsistentId(consistentId)
                .thenApply(DeploymentManagerImpl::map);
    }

    @Override
    public CompletableFuture<Boolean> onDemandDeploy(String id, Version version) {
        return metastore.getAllNodes(id, version)
                .thenCompose(nodes -> {
                    if (nodes.isEmpty()) {
                        return completedFuture(false);
                    }
                    if (nodes.contains(clusterService.topologyService().localMember().name())) {
                        return completedFuture(true);
                    }
                    return messaging.downloadUnitContent(id, version, nodes)
                            .thenCompose(unitContent -> deployToLocalNode(id, version, unitContent));
                });
    }

    @Override
    public void start() {
        deployer.initUnitsFolder(workDir.resolve(configuration.deploymentLocation().value()));
        messaging.subscribe();
    }

    @Override
    public void stop() throws Exception {
        tracker.cancelAll();
    }

    private static void checkId(String id) {
        Objects.requireNonNull(id);

        if (id.isBlank()) {
            throw new IllegalArgumentException("Id is blank");
        }
    }

    private static List<UnitStatuses> map(List<UnitClusterStatus> statuses) {
        Map<String, UnitStatusesBuilder> map = new HashMap<>();
        for (UnitClusterStatus status : statuses) {
            map.computeIfAbsent(status.id(), UnitStatuses::builder)
                    .append(status.version(), status.status()).build();
        }
        return map.values().stream().map(UnitStatusesBuilder::build).collect(Collectors.toList());
    }
}
