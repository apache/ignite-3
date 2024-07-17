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
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.OBSOLETE;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.REMOVING;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.UPLOADING;
import static org.apache.ignite.internal.deployunit.UnitContent.toDeploymentUnit;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.deployunit.UnitStatuses.UnitStatusesBuilder;
import org.apache.ignite.internal.deployunit.configuration.DeploymentConfiguration;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitAlreadyExistsException;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.apache.ignite.internal.deployunit.metastore.ClusterEventCallback;
import org.apache.ignite.internal.deployunit.metastore.ClusterEventCallbackImpl;
import org.apache.ignite.internal.deployunit.metastore.ClusterStatusWatchListener;
import org.apache.ignite.internal.deployunit.metastore.DeploymentUnitFailover;
import org.apache.ignite.internal.deployunit.metastore.DeploymentUnitStore;
import org.apache.ignite.internal.deployunit.metastore.NodeEventCallback;
import org.apache.ignite.internal.deployunit.metastore.NodeStatusWatchListener;
import org.apache.ignite.internal.deployunit.metastore.status.UnitClusterStatus;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterService;
import org.jetbrains.annotations.Nullable;

/**
 * Deployment manager implementation.
 */
public class DeploymentManagerImpl implements IgniteDeployment {
    private static final IgniteLogger LOG = Loggers.forClass(DeploymentManagerImpl.class);

    /**
     * Delay for undeployer.
     */
    private static final Duration UNDEPLOYER_DELAY = Duration.ofSeconds(5);

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

    private final DeploymentUnitAcquiredWaiter undeployer;

    private final String nodeName;

    private final NodeEventCallback nodeStatusCallback;

    private final NodeStatusWatchListener nodeStatusWatchListener;

    private final ClusterEventCallback clusterEventCallback;

    private final ClusterStatusWatchListener clusterStatusWatchListener;

    /**
     * Constructor.
     *
     * @param clusterService Cluster service.
     * @param deploymentUnitStore Deployment units metastore service.
     * @param workDir Node working directory.
     * @param configuration Deployment configuration.
     * @param cmgManager Cluster management group manager.
     * @param nodeName Node consistent ID.
     */
    public DeploymentManagerImpl(
            ClusterService clusterService,
            DeploymentUnitStore deploymentUnitStore,
            LogicalTopologyService logicalTopology,
            Path workDir,
            DeploymentConfiguration configuration,
            ClusterManagementGroupManager cmgManager,
            String nodeName
    ) {
        this.deploymentUnitStore = deploymentUnitStore;
        this.configuration = configuration;
        this.cmgManager = cmgManager;
        this.workDir = workDir;
        this.nodeName = nodeName;
        tracker = new DownloadTracker();
        deployer = new FileDeployerService(nodeName);
        deploymentUnitAccessor = new DeploymentUnitAccessorImpl(deployer);
        undeployer = new DeploymentUnitAcquiredWaiter(
                nodeName,
                deploymentUnitAccessor,
                unit -> deploymentUnitStore.updateNodeStatus(nodeName, unit.name(), unit.version(), REMOVING)
        );
        messaging = new DeployMessagingService(clusterService, cmgManager, deployer, tracker);

        nodeStatusCallback = new DefaultNodeCallback(
                deploymentUnitStore,
                messaging,
                deployer,
                undeployer,
                tracker,
                cmgManager,
                nodeName
        );
        nodeStatusWatchListener = new NodeStatusWatchListener(deploymentUnitStore, nodeName, nodeStatusCallback);

        clusterEventCallback = new ClusterEventCallbackImpl(deploymentUnitStore, deployer, cmgManager, nodeName);
        clusterStatusWatchListener = new ClusterStatusWatchListener(clusterEventCallback);

        failover = new DeploymentUnitFailover(logicalTopology, deploymentUnitStore, deployer, nodeName);
    }

    @Override
    public CompletableFuture<Boolean> deployAsync(
            String id,
            Version version,
            boolean force,
            DeploymentUnit deploymentUnit,
            NodesToDeploy deployedNodes
    ) {
        checkId(id);
        Objects.requireNonNull(version);
        Objects.requireNonNull(deploymentUnit);

        LOG.info("Deploying {}:{} on {}", id, version, deployedNodes);
        return deployedNodes.extractNodes(cmgManager)
                .thenCompose(nodesToDeploy ->
                        doDeploy(id, version, force, deploymentUnit, nodesToDeploy)
                );
    }

    private CompletableFuture<Boolean> doDeploy(
            String id,
            Version version,
            boolean force,
            DeploymentUnit deploymentUnit,
            Set<String> nodesToDeploy
    ) {
        return deploymentUnitStore.createClusterStatus(id, version, nodesToDeploy)
                .thenCompose(clusterStatus -> {
                    if (clusterStatus != null) {
                        return doDeploy(clusterStatus, deploymentUnit, nodesToDeploy);
                    } else {
                        if (force) {
                            return undeployAsync(id, version)
                                    .thenCompose(u -> doDeploy(id, version, false, deploymentUnit, nodesToDeploy));
                        }
                        LOG.warn("Failed to deploy meta of unit " + id + ":" + version + " to metastore. "
                                + "Already exists.");
                        return failedFuture(
                                new DeploymentUnitAlreadyExistsException(id,
                                        "Unit " + id + ":" + version + " already exists"));
                    }
                });
    }

    private CompletableFuture<Boolean> doDeploy(
            UnitClusterStatus clusterStatus,
            DeploymentUnit deploymentUnit,
            Set<String> nodesToDeploy
    ) {
        return deployToLocalNode(clusterStatus, deploymentUnit)
                .thenApply(completed -> {
                    if (completed) {
                        nodesToDeploy.forEach(node -> {
                            if (!node.equals(nodeName)) {
                                deploymentUnitStore.createNodeStatus(
                                        node,
                                        clusterStatus.id(),
                                        clusterStatus.version(),
                                        clusterStatus.opId(),
                                        UPLOADING
                                );
                            }
                        });
                    }
                    return completed;
                });
    }

    private CompletableFuture<Boolean> deployToLocalNode(UnitClusterStatus clusterStatus, DeploymentUnit deploymentUnit) {
        return deployer.deploy(clusterStatus.id(), clusterStatus.version(), deploymentUnit)
                .thenCompose(deployed -> {
                    if (deployed) {
                        return deploymentUnitStore.createNodeStatus(
                                nodeName,
                                clusterStatus.id(),
                                clusterStatus.version(),
                                clusterStatus.opId(),
                                DEPLOYED
                        );
                    }
                    return falseCompletedFuture();
                });
    }

    @Override
    public CompletableFuture<Boolean> undeployAsync(String id, Version version) {
        checkId(id);
        Objects.requireNonNull(version);

        LOG.info("Undeploying {}:{}", id, version);
        return messaging.stopInProgressDeploy(id, version)
                .thenCompose(v -> deploymentUnitStore.updateClusterStatus(id, version, OBSOLETE))
                .thenCompose(success -> {
                    if (success) {
                        return cmgManager.logicalTopology()
                                .thenCompose(logicalTopology -> {
                                    Set<String> logicalNodes = logicalTopology.nodes().stream()
                                            .map(LogicalNode::name)
                                            .collect(Collectors.toSet());
                                    // Set OBSOLETE status only to nodes which are present in the topology
                                    return deploymentUnitStore.getAllNodes(id, version)
                                            .thenCompose(nodes -> allOf(nodes.stream()
                                                    .filter(logicalNodes::contains)
                                                    .map(node -> deploymentUnitStore.updateNodeStatus(node, id, version, OBSOLETE))
                                                    .toArray(CompletableFuture[]::new)))
                                            .thenApply(v -> {
                                                // Now the nodes are handling the OBSOLETE node statuses and when all nodes are in the
                                                // REMOVING status, the cluster status will be changed to REMOVING
                                                return true;
                                            });
                                });
                    }
                    return failedFuture(new DeploymentUnitNotFoundException(id, version));
                });
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
        return deploymentUnitStore.getNodeStatuses(nodeName)
                .thenApply(DeploymentManagerImpl::fromUnitStatuses);
    }

    @Override
    public CompletableFuture<UnitStatuses> nodeStatusesAsync(String id) {
        checkId(id);

        return deploymentUnitStore.getNodeStatuses(nodeName, id)
                .thenApply(statuses -> fromUnitStatuses(id, statuses));
    }

    @Override
    public CompletableFuture<DeploymentStatus> nodeStatusAsync(String id, Version version) {
        checkId(id);
        Objects.requireNonNull(version);

        return deploymentUnitStore.getNodeStatus(nodeName, id, version)
                .thenApply(DeploymentManagerImpl::extractDeploymentStatus);
    }

    @Override
    public CompletableFuture<Boolean> onDemandDeploy(String id, Version version) {
        return deploymentUnitStore.getAllNodes(id, version)
                .thenCompose(nodes -> {
                    if (nodes.isEmpty()) {
                        return falseCompletedFuture();
                    }
                    if (nodes.contains(nodeName)) {
                        return trueCompletedFuture();
                    }
                    return messaging.downloadUnitContent(id, version, nodes)
                            .thenCompose(content -> {
                                return deploymentUnitStore.getClusterStatus(id, version)
                                        .thenCompose(status -> {
                                            DeploymentUnit unit = toDeploymentUnit(content);
                                            return deployToLocalNode(status, unit)
                                                    .whenComplete((deployed, throwable) -> {
                                                        try {
                                                            unit.close();
                                                        } catch (Exception e) {
                                                            LOG.error("Error closing deployment unit", e);
                                                        }
                                                    });
                                        });
                            });

                });
    }

    @Override
    public CompletableFuture<Version> detectLatestDeployedVersion(String id) {
        return clusterStatusesAsync(id)
                .thenApply(statuses -> {
                    if (statuses == null) {
                        throw new DeploymentUnitNotFoundException(id, Version.LATEST);
                    }

                    return statuses.versionStatuses().stream()
                            .filter(e -> e.getStatus() == DEPLOYED)
                            .reduce((first, second) -> second)
                            .orElseThrow(() -> new DeploymentUnitNotFoundException(id, Version.LATEST))
                            .getVersion();
                });
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        deployer.initUnitsFolder(workDir.resolve(configuration.deploymentLocation().value()));
        deploymentUnitStore.registerNodeStatusListener(nodeStatusWatchListener);
        deploymentUnitStore.registerClusterStatusListener(clusterStatusWatchListener);
        messaging.subscribe();
        failover.registerTopologyChangeCallback(nodeStatusCallback, clusterEventCallback);
        undeployer.start(UNDEPLOYER_DELAY.getSeconds(), TimeUnit.SECONDS);

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        deployer.stop();
        nodeStatusWatchListener.stop();
        tracker.cancelAll();
        deploymentUnitStore.unregisterNodeStatusListener(nodeStatusWatchListener);
        deploymentUnitStore.unregisterClusterStatusListener(clusterStatusWatchListener);
        undeployer.stop();

        return nullCompletedFuture();
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
