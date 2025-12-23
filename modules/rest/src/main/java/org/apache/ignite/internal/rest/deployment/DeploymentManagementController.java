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

package org.apache.ignite.internal.rest.deployment;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.deployment.version.Version.parseVersion;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.multipart.CompletedFileUpload;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.deployunit.IgniteDeployment;
import org.apache.ignite.internal.deployunit.NodesToDeploy;
import org.apache.ignite.internal.deployunit.UnitStatuses;
import org.apache.ignite.internal.deployunit.structure.UnitFile;
import org.apache.ignite.internal.deployunit.structure.UnitFolder;
import org.apache.ignite.internal.deployunit.tempstorage.TempStorage;
import org.apache.ignite.internal.deployunit.tempstorage.TempStorageProvider;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.api.deployment.DeploymentCodeApi;
import org.apache.ignite.internal.rest.api.deployment.DeploymentStatus;
import org.apache.ignite.internal.rest.api.deployment.InitialDeployMode;
import org.apache.ignite.internal.rest.api.deployment.UnitEntry;
import org.apache.ignite.internal.rest.api.deployment.UnitStatus;
import org.apache.ignite.internal.rest.api.deployment.UnitVersionStatus;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * Implementation of {@link DeploymentCodeApi}.
 */
@SuppressWarnings("OptionalContainsCollection")
@Controller("/management/v1/deployment")
public class DeploymentManagementController implements DeploymentCodeApi, ResourceHolder {
    private static final IgniteLogger LOG = Loggers.forClass(DeploymentManagementController.class);

    private IgniteDeployment deployment;

    private TempStorageProvider tempStorageProvider;

    public DeploymentManagementController(IgniteDeployment deployment, TempStorageProvider tempStorageProvider) {
        this.deployment = deployment;
        this.tempStorageProvider = tempStorageProvider;
    }

    @Override
    public CompletableFuture<Boolean> deploy(
            String unitId,
            String unitVersion,
            Publisher<CompletedFileUpload> unitContent,
            Optional<InitialDeployMode> deployMode,
            Optional<List<String>> initialNodes
    ) {
        return doDeploy(unitId, unitVersion, unitContent, deployMode, initialNodes, false);
    }

    @Override
    public CompletableFuture<Boolean> deployZip(String unitId, String unitVersion, Publisher<CompletedFileUpload> unitContent,
            Optional<InitialDeployMode> deployMode, Optional<List<String>> initialNodes) {
        return doDeploy(unitId, unitVersion, unitContent, deployMode, initialNodes, true);
    }

    private CompletableFuture<Boolean> doDeploy(
            String unitId,
            String unitVersion,
            Publisher<CompletedFileUpload> unitContent,
            Optional<InitialDeployMode> deployMode,
            Optional<List<String>> initialNodes,
            boolean zip
    ) {
        Version version;
        TempStorage tempStorage;
        try {
            version = parseVersion(unitVersion);
            tempStorage = tempStorageProvider.tempStorage(unitId, version);
        } catch (Exception e) {
            // In case of any exception during initialization of temp storage we need to discard uploaded files. For some reason (probably
            // a bug in micronaut discarding the CompletedFileUpload can lead to buffer leaks. Let's close the underlying buffer directly
            // by getting the input stream and closing it.
            // In case of normal operation the Netty resource will be properly released by the CompletedFileUpload#getInputStream call in
            // the CompletedFileUploadSubscriber.
            return Flux.from(unitContent)
                    .doOnNext(completedFileUpload -> {
                        try {
                            completedFileUpload.getInputStream().close();
                        } catch (IOException ignored) {
                            // Ignore exceptions thrown from close
                        }
                    })
                    .collectList()
                    .toFuture()
                    .thenCompose(unused -> failedFuture(e));
        }

        CompletedFileUploadSubscriber subscriber = new CompletedFileUploadSubscriber(tempStorage, zip);
        unitContent.subscribe(subscriber);

        NodesToDeploy nodesToDeploy = initialNodes.map(NodesToDeploy::new)
                .orElseGet(() -> new NodesToDeploy(fromInitialDeployMode(deployMode)));

        return subscriber.result().thenCompose(deploymentUnit ->
                deployment.deployAsync(unitId, version, deploymentUnit, nodesToDeploy)
                        .whenComplete((unitStatus, throwable) -> {
                            tempStorage.close();
                            try {
                                deploymentUnit.close();
                            } catch (Exception e) {
                                LOG.error("Failed to close subscriber", e);
                            }
                        })
        );
    }

    @Override
    public CompletableFuture<Boolean> undeploy(String unitId, String unitVersion) {
        return deployment.undeployAsync(unitId, parseVersion(unitVersion));
    }

    @Override
    public CompletableFuture<Collection<UnitStatus>> clusterStatuses(Optional<List<DeploymentStatus>> statuses) {
        return deployment.clusterStatusesAsync()
                .thenApply(statusesList -> fromUnitStatuses(statusesList, statuses));
    }

    @Override
    public CompletableFuture<Collection<UnitStatus>> clusterStatuses(
            String unitId,
            Optional<String> version,
            Optional<List<DeploymentStatus>> statuses
    ) {
        return clusterStatuses(unitId, version)
                .thenApply(statusesList -> fromUnitStatuses(statusesList, statuses));
    }

    private CompletableFuture<List<UnitStatuses>> clusterStatuses(String unitId, Optional<String> version) {
        if (version.isPresent()) {
            Version parsedVersion = parseVersion(version.get());
            return deployment.clusterStatusAsync(unitId, parsedVersion)
                    .thenApply(deploymentStatus -> {
                        if (deploymentStatus != null) {
                            return List.of(UnitStatuses.builder(unitId).append(parsedVersion, deploymentStatus).build());
                        } else {
                            return List.of();
                        }
                    });
        } else {
            return deployment.clusterStatusesAsync(unitId)
                    .thenApply(unitStatuses -> unitStatuses != null ? List.of(unitStatuses) : List.of());
        }
    }

    @Override
    public CompletableFuture<Collection<UnitStatus>> nodeStatuses(Optional<List<DeploymentStatus>> statuses) {
        return deployment.nodeStatusesAsync()
                .thenApply(statusesList -> fromUnitStatuses(statusesList, statuses));
    }

    @Override
    public CompletableFuture<Collection<UnitStatus>> nodeStatuses(
            String unitId,
            Optional<String> version,
            Optional<List<DeploymentStatus>> statuses
    ) {
        return nodeStatuses(unitId, version)
                .thenApply(statusesList -> fromUnitStatuses(statusesList, statuses));
    }

    private CompletableFuture<List<UnitStatuses>> nodeStatuses(String unitId, Optional<String> version) {
        if (version.isPresent()) {
            Version parsedVersion = parseVersion(version.get());
            return deployment.nodeStatusAsync(unitId, parsedVersion)
                    .thenApply(deploymentStatus -> {
                        if (deploymentStatus != null) {
                            return List.of(UnitStatuses.builder(unitId).append(parsedVersion, deploymentStatus).build());
                        } else {
                            return List.of();
                        }
                    });
        } else {
            return deployment.nodeStatusesAsync(unitId)
                    .thenApply(unitStatuses -> unitStatuses != null ? List.of(unitStatuses) : List.of());
        }
    }

    private static List<UnitStatus> fromUnitStatuses(List<UnitStatuses> statusesList, Optional<List<DeploymentStatus>> deploymentStatuses) {
        return statusesList.stream()
                .map(unitStatuses -> fromUnitStatuses(unitStatuses, createStatusFilter(deploymentStatuses)))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Mapper method.
     *
     * @param statuses Unit statuses.
     * @param statusFilter Deployment status filter.
     * @return Unit statuses DTO.
     */
    private static @Nullable UnitStatus fromUnitStatuses(UnitStatuses statuses, Predicate<DeploymentStatus> statusFilter) {
        List<UnitVersionStatus> versionStatuses = new ArrayList<>();
        for (org.apache.ignite.internal.deployunit.UnitVersionStatus versionStatus : statuses.versionStatuses()) {
            DeploymentStatus deploymentStatus = fromDeploymentStatus(versionStatus.getStatus());
            if (statusFilter.test(deploymentStatus)) {
                versionStatuses.add(new UnitVersionStatus(versionStatus.getVersion().render(), deploymentStatus));
            }
        }

        if (versionStatuses.isEmpty()) {
            return null;
        }
        return new UnitStatus(statuses.id(), versionStatuses);
    }

    private static Predicate<DeploymentStatus> createStatusFilter(Optional<List<DeploymentStatus>> statuses) {
        if (statuses.isEmpty() || statuses.get().isEmpty()) {
            return status -> true;
        } else {
            EnumSet<DeploymentStatus> statusesSet = EnumSet.copyOf(statuses.get());
            return statusesSet::contains;
        }
    }

    private static org.apache.ignite.internal.deployunit.InitialDeployMode fromInitialDeployMode(Optional<InitialDeployMode> mode) {
        if (mode.isEmpty()) {
            return org.apache.ignite.internal.deployunit.InitialDeployMode.MAJORITY;
        }

        switch (mode.get()) {
            case ALL:
                return org.apache.ignite.internal.deployunit.InitialDeployMode.ALL;
            case MAJORITY:
            default:
                return org.apache.ignite.internal.deployunit.InitialDeployMode.MAJORITY;
        }
    }

    private static DeploymentStatus fromDeploymentStatus(org.apache.ignite.internal.deployunit.DeploymentStatus status) {
        return DeploymentStatus.valueOf(status.name());
    }

    @Override
    public CompletableFuture<UnitEntry.UnitFolder> unitStructure(String unitId, String unitVersion) {
        return deployment.nodeUnitFileStructure(unitId, parseVersion(unitVersion)).thenApply(DeploymentManagementController::toDto);
    }

    private static UnitEntry.UnitFolder toDto(UnitFolder unitFolder) {
        return new UnitEntry.UnitFolder(
                unitFolder.name(),
                unitFolder.children().stream()
                        .map(DeploymentManagementController::toDto)
                        .collect(Collectors.toList())
        );
    }

    private static UnitEntry toDto(org.apache.ignite.internal.deployunit.structure.UnitEntry entry) {
        if (entry instanceof UnitFile) {
            return new UnitEntry.UnitFile(entry.name(), entry.size());
        } else {
            return toDto((UnitFolder) entry);
        }
    }

    @Override
    public void cleanResources() {
        deployment = null;
        tempStorageProvider = null;
    }
}
