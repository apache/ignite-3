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
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
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
import org.apache.ignite.internal.deployunit.metastore.status.UnitNodeStatus;
import org.apache.ignite.internal.deployunit.structure.UnitFolder;
import org.apache.ignite.internal.deployunit.tempstorage.TempStorageProvider;
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

    private final UnitDownloader unitDownloader;

    /**
     * Constructor.
     *
     * @param clusterService Cluster service.
     * @param deploymentUnitStore Deployment units metastore service.
     * @param workDir Node working directory.
     * @param configuration Deployment configuration.
     * @param cmgManager Cluster management group manager.
     * @param nodeName Node consistent ID.
     * @param onPathRemoving Consumer to call when a deployment unit path is being removed.
     */
    public DeploymentManagerImpl(
            ClusterService clusterService,
            DeploymentUnitStore deploymentUnitStore,
            LogicalTopologyService logicalTopology,
            Path workDir,
            DeploymentConfiguration configuration,
            ClusterManagementGroupManager cmgManager,
            String nodeName,
            Consumer<Path> onPathRemoving
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
                unit -> {
                    onPathRemoving.accept(deployer.unitPath(unit.name(), unit.version(), false));
                    deploymentUnitStore.updateNodeStatus(nodeName, unit.name(), unit.version(), REMOVING);
                }
        );
        messaging = new DeployMessagingService(clusterService, cmgManager, deployer, tracker);
        unitDownloader = new UnitDownloader(deploymentUnitStore, nodeName, deployer, tracker, messaging);

        nodeStatusCallback = new DefaultNodeCallback(deploymentUnitStore, undeployer, unitDownloader, cmgManager);
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
                        return failedFuture(new DeploymentUnitAlreadyExistsException("Unit " + id + ":" + version + " already exists"));
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
                                    // Set OBSOLETE status only to nodes which are present in the topology
                                    return deploymentUnitStore.getAllNodes(id, version)
                                            .thenCompose(nodes -> allOf(nodes.stream()
                                                    .filter(logicalTopology::hasNode)
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
    public CompletableFuture<UnitFolder> nodeUnitFileStructure(String id, Version version) {
        checkId(id);
        Objects.requireNonNull(version);

        return deploymentUnitStore.getNodeStatus(nodeName, id, version).thenCompose(nodeStatus -> {
            if (nodeStatus == null) {
                return failedFuture(new DeploymentUnitNotFoundException(id, version));
            }

            return deployer.getUnitStructure(id, version);
        });
    }

    @Override
    public CompletableFuture<Boolean> onDemandDeploy(String id, Version version) {
        return deploymentUnitStore.getAllNodeStatuses(id, version)
                .thenCompose(statuses -> {
                    if (statuses.isEmpty()) {
                        return falseCompletedFuture();
                    }

                    Optional<UnitNodeStatus> nodeStatus = statuses.stream()
                            .filter(status -> status.nodeId().equals(nodeName))
                            .findFirst();

                    if (nodeStatus.isPresent()) {
                        switch (nodeStatus.get().status()) {
                            case UPLOADING:
                                // Wait for the upload
                                LOG.debug("Status is UPLOADING, downloading the unit");
                                return unitDownloader.downloadUnit(statuses, id, version);
                            case DEPLOYED:
                                // Unit is already deployed on the local node.
                                LOG.debug("Status is DEPLOYED");
                                return trueCompletedFuture();
                            default:
                                // Invalid status, cluster status should be deployed
                                LOG.debug("Invalid status {}", nodeStatus.get().status());
                                return falseCompletedFuture();
                        }
                    } else {
                        // Node was not in the initial deploy list, create status in the UPLOADING state and wait for the upload.
                        return deploymentUnitStore.getClusterStatus(id, version).thenCompose(clusterStatus ->
                                deploymentUnitStore.createNodeStatus(nodeName, id, version, clusterStatus.opId(), UPLOADING)
                                        .thenCompose(created -> {
                                            LOG.debug("Status {}, downloading the unit", created ? "created" : "not created");
                                            return unitDownloader.downloadUnit(statuses, id, version);
                                        })
                        );
                    }
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
        Path deploymentUnitFolder = workDir.resolve(configuration.location().value());
        Path deploymentUnitTmp = workDir.resolve(configuration.tempLocation().value());
        deployer.initUnitsFolder(deploymentUnitFolder, deploymentUnitTmp);
        deploymentUnitStore.registerNodeStatusListener(nodeStatusWatchListener);
        deploymentUnitStore.registerClusterStatusListener(clusterStatusWatchListener);
        messaging.subscribe();
        failover.registerTopologyChangeCallback(nodeStatusCallback, clusterEventCallback);
        undeployer.start(UNDEPLOYER_DELAY.getSeconds(), TimeUnit.SECONDS);
        return new StaticUnitDeployer(deploymentUnitStore, nodeName, deploymentUnitFolder).searchAndDeployStaticUnits();
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

    public TempStorageProvider tempStorageProvider() {
        return deployer.tempStorageProvider();
    }
}
