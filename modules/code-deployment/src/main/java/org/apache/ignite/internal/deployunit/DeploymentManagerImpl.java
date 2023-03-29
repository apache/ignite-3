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
import static org.apache.ignite.internal.deployunit.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.OBSOLETE;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.REMOVING;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.deployunit.configuration.DeploymentConfiguration;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitAlreadyExistsException;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitReadException;
import org.apache.ignite.internal.deployunit.key.UnitMetaSerializer;
import org.apache.ignite.internal.deployunit.metastore.EntrySubscriber;
import org.apache.ignite.internal.deployunit.metastore.SortedListAccumulator;
import org.apache.ignite.internal.deployunit.metastore.UnitStatusAccumulator;
import org.apache.ignite.internal.deployunit.metastore.UnitsAccumulator;
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
    private final DeployMetastoreService metastore;

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
        this.tracker = new DeployTracker();
        metastore = new DeployMetastoreService(metaStorage);
        deployer = new FileDeployerService();
        messaging = new DeployMessagingService(clusterService, cmgManager, deployer, tracker);
    }

    @Override
    public CompletableFuture<Boolean> deployAsync(String id, Version version, boolean force, DeploymentUnit deploymentUnit) {
        checkId(id);
        Objects.requireNonNull(version);
        Objects.requireNonNull(deploymentUnit);

        UnitMeta meta = new UnitMeta(id, version, deploymentUnit.name(), DeploymentStatus.UPLOADING, Collections.emptyList());

        byte[] unitContent;
        try {
            unitContent = deploymentUnit.content().readAllBytes();
        } catch (IOException e) {
            LOG.error("Error to read deployment unit content", e);
            return CompletableFuture.failedFuture(new DeploymentUnitReadException(e));
        }

        return metastore.putIfNotExist(id, version, meta)
                .thenCompose(success -> {
                    if (success) {
                        return tracker.track(id, version, deployer.deploy(id, version.render(), deploymentUnit.name(), unitContent)
                                .thenCompose(deployed -> {
                                    if (deployed) {
                                        return metastore.updateMeta(id, version,
                                                unitMeta -> unitMeta
                                                        .addConsistentId(clusterService.topologyService().localMember().name()));
                                    }
                                    return CompletableFuture.completedFuture(false);
                                })
                                .thenApply(completed -> {
                                    if (completed) {
                                        messaging.startDeployAsyncToCmg(id, version, deploymentUnit.name(), unitContent)
                                                .thenAccept(ids -> metastore.updateMeta(id, version, unitMeta -> {
                                                    for (String consistentId : ids) {
                                                        unitMeta.addConsistentId(consistentId);
                                                    }
                                                    unitMeta.updateStatus(DEPLOYED);
                                                }));
                                    }
                                    return completed;
                                }));
                    } else {
                        if (force) {
                            return undeployAsync(id, version)
                                    .thenCompose(v -> deployAsync(id, version, deploymentUnit));
                        }
                        LOG.error("Failed to deploy meta of unit " + id + ":" + version);
                        return CompletableFuture.failedFuture(
                                new DeploymentUnitAlreadyExistsException(id,
                                        "Unit " + id + ":" + version + " already exists"));
                    }

                });
    }

    @Override
    public CompletableFuture<Void> undeployAsync(String id, Version version) {
        checkId(id);
        Objects.requireNonNull(version);

        return messaging.stopInProgressDeploy(id, version)
                .thenCompose(v -> metastore.updateMeta(id, version, true, meta -> meta.updateStatus(OBSOLETE)))
                .thenCompose(success -> {
                    if (success) {
                        //TODO: Check unit usages here. If unit used in compute task we cannot just remove it.
                        return metastore.updateMeta(id, version, true, meta -> meta.updateStatus(REMOVING));
                    }
                    return CompletableFuture.completedFuture(false);
                })
                .thenCompose(success -> {
                    if (success) {
                        return cmgManager.logicalTopology();
                    }
                    return CompletableFuture.failedFuture(new DeploymentUnitNotFoundException(
                            "Unit " + id + " with version " + version + " doesn't exist"));
                }).thenCompose(logicalTopologySnapshot -> allOf(
                        logicalTopologySnapshot.nodes().stream()
                                .map(node -> messaging.undeploy(node, id, version))
                                .toArray(CompletableFuture[]::new))
                        .thenAccept(unused -> metastore.removeIfExist(id, version)));
    }

    @Override
    public CompletableFuture<List<UnitStatus>> unitsAsync() {
        CompletableFuture<List<UnitStatus>> result = new CompletableFuture<>();
        metastore.getAll()
                .subscribe(new EntrySubscriber<>(result, new UnitsAccumulator()));
        return result;
    }

    @Override
    public CompletableFuture<List<Version>> versionsAsync(String id) {
        checkId(id);
        CompletableFuture<List<Version>> result = new CompletableFuture<>();
        metastore.getAllWithId(id)
                .subscribe(
                        new EntrySubscriber<>(
                                result,
                                new SortedListAccumulator<>(e -> UnitMetaSerializer.deserialize(e.value()).version())
                        )
                );
        return result;
    }

    @Override
    public CompletableFuture<UnitStatus> statusAsync(String id) {
        checkId(id);
        CompletableFuture<UnitStatus> result = new CompletableFuture<>();
        metastore.getAllWithId(id)
                .subscribe(new EntrySubscriber<>(result, new UnitStatusAccumulator(id)));
        return result;
    }

    @Override
    public CompletableFuture<List<UnitStatus>> findUnitByConsistentIdAsync(String consistentId) {
        Objects.requireNonNull(consistentId);

        CompletableFuture<List<UnitStatus>> result = new CompletableFuture<>();
        metastore.getAll()
                .subscribe(
                        new EntrySubscriber<>(
                                result,
                                new UnitsAccumulator(meta -> meta.consistentIdLocation().contains(consistentId))
                        )
                );
        return result;
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
}
