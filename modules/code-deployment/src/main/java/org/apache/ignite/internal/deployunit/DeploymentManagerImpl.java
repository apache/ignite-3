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
import static org.apache.ignite.internal.deployunit.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.OBSOLETE;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.REMOVING;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.deployunit.UnitStatuses.UnitStatusesBuilder;
import org.apache.ignite.internal.deployunit.configuration.DeploymentConfiguration;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitAlreadyExistsException;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitReadException;
import org.apache.ignite.internal.deployunit.metastore.DeploymentUnitFailover;
import org.apache.ignite.internal.deployunit.metastore.DeploymentUnitStore;
import org.apache.ignite.internal.deployunit.metastore.NodeStatusWatchListener;
import org.apache.ignite.internal.deployunit.metastore.status.UnitClusterStatus;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.network.ClusterService;
import org.jetbrains.annotations.Nullable;

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
    private final DeploymentUnitStore deploymentUnitStore;

    /**
     * Deploy tracker.
     */
    private final DownloadTracker tracker;

    /**
     * Failover.
     */
    private final DeploymentUnitFailover failover;

    /**
     * Deployment unit accessor.
     */
    private final DeploymentUnitAccessor deploymentUnitAccessor;

    /**
     * Constructor.
     *
     * @param clusterService Cluster service.
     * @param deploymentUnitStore Deployment units metastore service.
     * @param workDir Node working directory.
     * @param configuration Deployment configuration.
     * @param cmgManager Cluster management group manager.
     */
    public DeploymentManagerImpl(
            ClusterService clusterService,
            DeploymentUnitStore deploymentUnitStore,
            LogicalTopologyService logicalTopology,
            Path workDir,
            DeploymentConfiguration configuration,
            ClusterManagementGroupManager cmgManager
    ) {
        this.clusterService = clusterService;
        this.deploymentUnitStore = deploymentUnitStore;
        this.configuration = configuration;
        this.cmgManager = cmgManager;
        this.workDir = workDir;
        tracker = new DownloadTracker();
        deployer = new FileDeployerService();
        messaging = new DeployMessagingService(clusterService, cmgManager, deployer, tracker);
        deploymentUnitAccessor = new DeploymentUnitAccessorImpl(deployer);
        failover = new DeploymentUnitFailover(logicalTopology, deploymentUnitStore, clusterService);
    }

    @Override
    public CompletableFuture<Boolean> deployAsync(
            String id,
            Version version,
            boolean force,
            CompletableFuture<DeploymentUnit> deploymentUnit,
            NodesToDeploy deployedNodes
    ) {
        checkId(id);
        Objects.requireNonNull(version);
        Objects.requireNonNull(deploymentUnit);

        return deployedNodes.extractNodes(cmgManager)
                .thenCompose(nodesToDeploy ->
                        doDeploy(id, version, force, deploymentUnit, nodesToDeploy)
                );
    }

    private CompletableFuture<Boolean> doDeploy(
            String id,
            Version version,
            boolean force,
            CompletableFuture<DeploymentUnit> unitFuture,
            Set<String> nodesToDeploy
    ) {
        return deploymentUnitStore.createClusterStatus(id, version, nodesToDeploy)
                .thenCompose(success -> unitFuture.thenCompose(deploymentUnit -> {
                    if (success) {
                        return doDeploy(id, version, deploymentUnit, nodesToDeploy);
                    } else {
                        if (force) {
                            return undeployAsync(id, version)
                                    .thenCompose(u -> doDeploy(id, version, false, unitFuture, nodesToDeploy));
                        }
                        LOG.warn("Failed to deploy meta of unit " + id + ":" + version + " to metastore. "
                                + "Already exists.");
                        return failedFuture(
                                new DeploymentUnitAlreadyExistsException(id,
                                        "Unit " + id + ":" + version + " already exists"));
                    }
                }));
    }

    private CompletableFuture<Boolean> doDeploy(
            String id,
            Version version,
            DeploymentUnit deploymentUnit,
            Set<String> nodesToDeploy
    ) {
        UnitContent unitContent;
        try {
            unitContent = UnitContent.readContent(deploymentUnit);
        } catch (DeploymentUnitReadException e) {
            LOG.error("Error reading deployment unit content", e);
            return failedFuture(e);
        }
        return deployToLocalNode(id, version, unitContent)
                .thenApply(completed -> {
                    if (completed) {
                        String localNodeId = getLocalNodeId();
                        nodesToDeploy.forEach(node -> {
                            if (!node.equals(localNodeId)) {
                                deploymentUnitStore.createNodeStatus(node, id, version);
                            }
                        });
                    }
                    return completed;
                });
    }

    private CompletableFuture<Boolean> deployToLocalNode(String id, Version version, UnitContent unitContent) {
        return deployer.deploy(id, version, unitContent)
                .thenCompose(deployed -> {
                    if (deployed) {
                        return deploymentUnitStore.createNodeStatus(getLocalNodeId(), id, version, DEPLOYED);
                    }
                    return completedFuture(false);
                });
    }

    @Override
    public CompletableFuture<Boolean> undeployAsync(String id, Version version) {
        checkId(id);
        Objects.requireNonNull(version);

        return messaging.stopInProgressDeploy(id, version)
                .thenCompose(v -> deploymentUnitStore.updateClusterStatus(id, version, OBSOLETE))
                .thenCompose(success -> {
                    if (success) {
                        //TODO: Check unit usages here. If unit used in compute task we cannot just remove it.
                        return deploymentUnitStore.updateClusterStatus(id, version, REMOVING);
                    }
                    return completedFuture(false);
                })
                .thenCompose(success -> {
                    if (success) {
                        return cmgManager.logicalTopology();
                    }
                    return failedFuture(new DeploymentUnitNotFoundException(id, version));
                }).thenCompose(logicalTopologySnapshot -> allOf(
                        logicalTopologySnapshot.nodes().stream()
                                .map(node -> messaging.undeploy(node, id, version))
                                .toArray(CompletableFuture[]::new))
                ).thenCompose(unused -> deploymentUnitStore.remove(id, version));
    }

    @Override
    public CompletableFuture<List<UnitStatuses>> clusterStatusesAsync() {
        return deploymentUnitStore.getClusterStatuses()
                .thenApply(DeploymentManagerImpl::fromUnitStatuses);
    }

    @Override
    public CompletableFuture<UnitStatuses> clusterStatusesAsync(String id) {
        checkId(id);

        return deploymentUnitStore.getClusterStatuses(id)
                .thenApply(statuses -> fromUnitStatuses(id, statuses));
    }

    @Override
    public CompletableFuture<DeploymentStatus> clusterStatusAsync(String id, Version version) {
        checkId(id);
        Objects.requireNonNull(version);

        return deploymentUnitStore.getClusterStatus(id, version)
                .thenApply(DeploymentManagerImpl::extractDeploymentStatus);
    }

    @Override
    public CompletableFuture<List<Version>> versionsAsync(String id) {
        checkId(id);

        return deploymentUnitStore.getClusterStatuses(id)
                .thenApply(statuses -> statuses.stream().map(UnitClusterStatus::version)
                        .sorted()
                        .collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<List<UnitStatuses>> nodeStatusesAsync() {
        return deploymentUnitStore.getNodeStatuses(getLocalNodeId())
                .thenApply(DeploymentManagerImpl::fromUnitStatuses);
    }

    @Override
    public CompletableFuture<UnitStatuses> nodeStatusesAsync(String id) {
        checkId(id);

        return deploymentUnitStore.getNodeStatuses(getLocalNodeId(), id)
                .thenApply(statuses -> fromUnitStatuses(id, statuses));
    }

    @Override
    public CompletableFuture<DeploymentStatus> nodeStatusAsync(String id, Version version) {
        checkId(id);
        Objects.requireNonNull(version);

        return deploymentUnitStore.getNodeStatus(getLocalNodeId(), id, version)
                .thenApply(DeploymentManagerImpl::extractDeploymentStatus);
    }

    @Override
    public CompletableFuture<Boolean> onDemandDeploy(String id, Version version) {
        return deploymentUnitStore.getAllNodes(id, version)
                .thenCompose(nodes -> {
                    if (nodes.isEmpty()) {
                        return completedFuture(false);
                    }
                    if (nodes.contains(getLocalNodeId())) {
                        return completedFuture(true);
                    }
                    return messaging.downloadUnitContent(id, version, nodes)
                            .thenCompose(unitContent -> deployToLocalNode(id, version, unitContent));
                });
    }

    @Override
    public CompletableFuture<Version> detectLatestDeployedVersion(String id) {
        return clusterStatusesAsync(id)
                .thenApply(statuses -> {
                    return statuses.versions()
                            .stream()
                            .filter(version -> statuses.status(version) == DEPLOYED)
                            .max(Version::compareTo)
                            .orElseThrow(() -> new DeploymentUnitNotFoundException(id));
                });
    }

    @Override
    public void start() {
        DefaultNodeCallback callback = new DefaultNodeCallback(deploymentUnitStore, messaging, deployer, clusterService, tracker);
        deployer.initUnitsFolder(workDir.resolve(configuration.deploymentLocation().value()));
        deploymentUnitStore.registerListener(new NodeStatusWatchListener(
                deploymentUnitStore,
                this::getLocalNodeId,
                callback));
        messaging.subscribe();
        failover.registerTopologyChangeCallback(callback);
    }

    @Override
    public void stop() throws Exception {
        tracker.cancelAll();
    }

    private String getLocalNodeId() {
        return clusterService.topologyService().localMember().name();
    }

    private static void checkId(String id) {
        Objects.requireNonNull(id);

        if (id.isBlank()) {
            throw new IllegalArgumentException("Id is blank");
        }
    }

    private static List<UnitStatuses> fromUnitStatuses(List<? extends UnitStatus> statuses) {
        Map<String, UnitStatusesBuilder> map = new HashMap<>();
        for (UnitStatus status : statuses) {
            map.computeIfAbsent(status.id(), UnitStatuses::builder)
                    .append(status.version(), status.status()).build();
        }
        return map.values().stream().map(UnitStatusesBuilder::build).collect(Collectors.toList());
    }

    @Nullable
    private static UnitStatuses fromUnitStatuses(String id, List<? extends UnitStatus> statuses) {
        if (statuses.isEmpty()) {
            return null;
        }

        UnitStatusesBuilder builder = UnitStatuses.builder(id);
        for (UnitStatus status : statuses) {
            builder.append(status.version(), status.status());
        }

        return builder.build();
    }

    @Nullable
    private static DeploymentStatus extractDeploymentStatus(UnitStatus status) {
        return status != null ? status.status() : null;
    }

    public DeploymentUnitAccessor deploymentUnitAccessor() {
        return deploymentUnitAccessor;
    }
}
