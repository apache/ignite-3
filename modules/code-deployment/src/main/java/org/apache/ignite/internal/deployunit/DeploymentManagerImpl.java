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

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static org.apache.ignite.internal.deployunit.key.UnitKey.allUnits;
import static org.apache.ignite.internal.deployunit.key.UnitKey.key;
import static org.apache.ignite.internal.deployunit.key.UnitKey.withId;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.exists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.revision;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.deployment.IgniteDeployment;
import org.apache.ignite.deployment.UnitStatus;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.deployunit.configuration.DeploymentConfiguration;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitAlreadyExistsException;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitReadException;
import org.apache.ignite.internal.deployunit.key.UnitMetaSerializer;
import org.apache.ignite.internal.deployunit.message.DeployUnitMessageTypes;
import org.apache.ignite.internal.deployunit.message.DeployUnitRequest;
import org.apache.ignite.internal.deployunit.message.DeployUnitRequestBuilder;
import org.apache.ignite.internal.deployunit.message.DeployUnitRequestImpl;
import org.apache.ignite.internal.deployunit.message.DeployUnitResponse;
import org.apache.ignite.internal.deployunit.message.DeployUnitResponseBuilder;
import org.apache.ignite.internal.deployunit.message.DeployUnitResponseImpl;
import org.apache.ignite.internal.deployunit.message.UndeployUnitRequest;
import org.apache.ignite.internal.deployunit.message.UndeployUnitRequestImpl;
import org.apache.ignite.internal.deployunit.message.UndeployUnitResponse;
import org.apache.ignite.internal.deployunit.message.UndeployUnitResponseImpl;
import org.apache.ignite.internal.deployunit.metastore.EntrySubscriber;
import org.apache.ignite.internal.deployunit.metastore.SortedListAccumulator;
import org.apache.ignite.internal.deployunit.metastore.UnitStatusAccumulator;
import org.apache.ignite.internal.deployunit.metastore.UnitsAccumulator;
import org.apache.ignite.internal.future.InFlightFutures;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;

/**
 * Deployment manager implementation.
 */
public class DeploymentManagerImpl implements IgniteDeployment, IgniteComponent {

    private static final IgniteLogger LOG = Loggers.forClass(DeploymentManagerImpl.class);

    private static final String TMP_SUFFIX = ".tmp";

    /**
     * Meta storage.
     */
    private final MetaStorageManager metaStorage;

    /**
     * Deployment configuration.
     */
    private final DeploymentConfiguration configuration;

    /**
     * Cluster management group manager.
     */
    private final ClusterManagementGroupManager cmgManager;

    /**
     * In flight futures tracker.
     */
    private final InFlightFutures inFlightFutures = new InFlightFutures();

    /**
     * Cluster service.
     */
    private final ClusterService clusterService;

    /**
     * Folder for units.
     */
    private Path unitsFolder;

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
        this.metaStorage = metaStorage;
        this.configuration = configuration;
        this.cmgManager = cmgManager;
        unitsFolder = workDir;
    }

    @Override
    public CompletableFuture<Boolean> deployAsync(String id, Version version, DeploymentUnit deploymentUnit) {
        checkId(id);
        Objects.requireNonNull(version);
        Objects.requireNonNull(deploymentUnit);

        ByteArray key = key(id, version.render());
        UnitMeta meta = new UnitMeta(id, version, deploymentUnit.name(), Collections.emptyList());

        Operation put = put(key, UnitMetaSerializer.serialize(meta));

        DeployUnitRequestBuilder builder = DeployUnitRequestImpl.builder();

        try {
            builder.unitContent(deploymentUnit.content().readAllBytes());
        } catch (IOException e) {
            LOG.error("Error to read deployment unit content", e);
            return CompletableFuture.failedFuture(new DeploymentUnitReadException(e));
        }
        DeployUnitRequest request = builder
                .unitName(deploymentUnit.name())
                .id(id)
                .version(version.render())
                .build();

        return metaStorage.invoke(notExists(key), put, Operations.noop())
                .thenCompose(success -> {
                    if (success) {
                        return doDeploy(request);
                    }
                    LOG.error("Failed to deploy meta of unit " + id + ":" + version);
                    return CompletableFuture.failedFuture(
                            new DeploymentUnitAlreadyExistsException(id,
                                    "Unit " + id + ":" + version + " already exists"));
                })
                .thenApply(completed -> {
                    if (completed) {
                        startDeployAsyncToCmg(request);
                    }
                    return completed;
                });
    }

    private void startDeployAsyncToCmg(DeployUnitRequest request) {
        cmgManager.cmgNodes()
                .thenAccept(nodes -> {
                    for (String node : nodes) {
                        ClusterNode clusterNode = clusterService.topologyService().getByConsistentId(node);
                        if (clusterNode != null) {
                            inFlightFutures.registerFuture(requestDeploy(clusterNode, request));
                        }
                    }
                });
    }

    private CompletableFuture<Boolean> requestDeploy(ClusterNode clusterNode, DeployUnitRequest request) {
        return clusterService.messagingService()
                .invoke(clusterNode, request, Long.MAX_VALUE)
                .thenCompose(message -> {
                    Throwable error = ((DeployUnitResponse) message).error();
                    if (error != null) {
                        LOG.error("Failed to deploy unit " + request.id() + ":" + request.version()
                                + " to node " + clusterNode, error);
                        return CompletableFuture.failedFuture(error);
                    }
                    return CompletableFuture.completedFuture(true);
                });
    }

    @Override
    public CompletableFuture<Void> undeployAsync(String id, Version version) {
        checkId(id);
        Objects.requireNonNull(version);

        ByteArray key = key(id, version.render());

        return metaStorage.invoke(exists(key), Operations.remove(key), Operations.noop())
                .thenCompose(success -> {
                    if (success) {
                        return cmgManager.logicalTopology();
                    }
                    return CompletableFuture.failedFuture(new DeploymentUnitNotFoundException(
                            "Unit " + id + " with version " + version + " doesn't exist"));
                }).thenApply(logicalTopologySnapshot -> {
                    for (ClusterNode node : logicalTopologySnapshot.nodes()) {
                        clusterService.messagingService()
                                .invoke(node, UndeployUnitRequestImpl.builder()
                                                .id(id)
                                                .version(version.render())
                                                .build(),
                                        Long.MAX_VALUE)
                                .thenAccept(message -> {
                                    Throwable error = ((UndeployUnitResponse) message).error();
                                    if (error != null) {
                                        LOG.error("Failed to undeploy unit " + id + ":" + version
                                                + " from node " + node, error);
                                    }
                                });
                    }
                    return null;
                });
    }

    @Override
    public CompletableFuture<List<UnitStatus>> unitsAsync() {
        CompletableFuture<List<UnitStatus>> result = new CompletableFuture<>();
        metaStorage.prefix(allUnits())
                .subscribe(new EntrySubscriber<>(result, new UnitsAccumulator()));
        return result;
    }

    @Override
    public CompletableFuture<List<Version>> versionsAsync(String id) {
        checkId(id);
        CompletableFuture<List<Version>> result = new CompletableFuture<>();
        metaStorage.prefix(withId(id))
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
        metaStorage.prefix(withId(id))
                .subscribe(new EntrySubscriber<>(result, new UnitStatusAccumulator(id)));
        return result;
    }

    @Override
    public CompletableFuture<List<UnitStatus>> findUnitByConsistentIdAsync(String consistentId) {
        Objects.requireNonNull(consistentId);

        CompletableFuture<List<UnitStatus>> result = new CompletableFuture<>();
        metaStorage.prefix(allUnits())
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
        unitsFolder = unitsFolder.resolve(configuration.deploymentLocation().value());
        clusterService.messagingService().addMessageHandler(DeployUnitMessageTypes.class,
                (message, senderConsistentId, correlationId) -> {
                    if (message instanceof DeployUnitRequest) {
                        processDeployRequest((DeployUnitRequest) message, senderConsistentId, correlationId);
                    } else if (message instanceof UndeployUnitRequest) {
                        processUndeployRequest((UndeployUnitRequest) message, senderConsistentId, correlationId);
                    }
                });
    }

    private void processDeployRequest(DeployUnitRequest executeRequest, String senderConsistentId, long correlationId) {
        doDeploy(executeRequest).whenComplete((success, throwable) -> {
            DeployUnitResponseBuilder builder = DeployUnitResponseImpl.builder();
            if (throwable != null) {
                builder.error(throwable);
            }
            clusterService.messagingService().respond(senderConsistentId,
                    builder.build(), correlationId);
        });
    }

    private void processUndeployRequest(UndeployUnitRequest executeRequest, String senderConsistentId, long correlationId) {
        try {
            Path unitPath = unitsFolder
                    .resolve(executeRequest.id())
                    .resolve(executeRequest.version());

            IgniteUtils.deleteIfExistsThrowable(unitPath);
        } catch (IOException e) {
            LOG.error("Failed to undeploy unit " + executeRequest.id() + ":" + executeRequest.version(), e);
            clusterService.messagingService()
                    .respond(senderConsistentId, UndeployUnitResponseImpl.builder().error(e).build(), correlationId);
            return;
        }

        clusterService.messagingService()
                .respond(senderConsistentId, UndeployUnitResponseImpl.builder().build(), correlationId);
    }

    private CompletableFuture<Boolean> doDeploy(DeployUnitRequest executeRequest) {
        String id = executeRequest.id();
        String version = executeRequest.version();
        try {
            Path unitPath = unitsFolder
                    .resolve(executeRequest.id())
                    .resolve(executeRequest.version())
                    .resolve(executeRequest.unitName());

            Path unitPathTmp = unitPath.resolveSibling(unitPath.getFileName() + TMP_SUFFIX);

            Files.createDirectories(unitPathTmp.getParent());

            Files.write(unitPathTmp, executeRequest.unitContent(), CREATE, SYNC, TRUNCATE_EXISTING);
            Files.move(unitPathTmp, unitPath, ATOMIC_MOVE, REPLACE_EXISTING);
        } catch (IOException e) {
            LOG.error("Failed to deploy unit " + executeRequest.id() + ":" + executeRequest.version(), e);
            return CompletableFuture.failedFuture(e);
        }

        ByteArray key = key(id, version);
        return metaStorage.get(key)
                .thenCompose(e -> {
                    UnitMeta prev = UnitMetaSerializer.deserialize(e.value());

                    prev.addConsistentId(clusterService.topologyService().localMember().name());

                    return metaStorage.invoke(
                            revision(key).eq(e.revision()),
                            put(key, UnitMetaSerializer.serialize(prev)),
                            Operations.noop());
                });
    }

    @Override
    public void stop() throws Exception {
        inFlightFutures.cancelInFlightFutures();
    }

    private static void checkId(String id) {
        Objects.requireNonNull(id);

        if (id.isBlank()) {
            throw new IllegalArgumentException("Id is blank");
        }
    }
}
