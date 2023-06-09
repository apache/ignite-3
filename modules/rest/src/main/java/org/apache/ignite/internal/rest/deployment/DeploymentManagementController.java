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

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.multipart.CompletedFileUpload;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.deployunit.DeploymentUnit;
import org.apache.ignite.internal.deployunit.IgniteDeployment;
import org.apache.ignite.internal.deployunit.UnitStatuses;
import org.apache.ignite.internal.rest.api.deployment.DeploymentCodeApi;
import org.apache.ignite.internal.rest.api.deployment.DeploymentStatus;
import org.apache.ignite.internal.rest.api.deployment.UnitStatus;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;

/**
 * Implementation of {@link DeploymentCodeApi}.
 */
@SuppressWarnings("OptionalContainsCollection")
@Controller("/management/v1/deployment")
public class DeploymentManagementController implements DeploymentCodeApi {
    private final IgniteDeployment deployment;

    public DeploymentManagementController(IgniteDeployment deployment) {
        this.deployment = deployment;
    }

    @Override
    public CompletableFuture<Boolean> deploy(String unitId, String unitVersion, Publisher<CompletedFileUpload> unitContent) {
        CompletableFuture<DeploymentUnit> result = new CompletableFuture<>();
        unitContent.subscribe(new CompletedFileUploadSubscriber(result));
        return result.thenCompose(deploymentUnit -> deployment.deployAsync(unitId, Version.parseVersion(unitVersion), deploymentUnit));
    }

    @Override
    public CompletableFuture<Boolean> undeploy(String unitId, String unitVersion) {
        return deployment.undeployAsync(unitId, Version.parseVersion(unitVersion));
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
            Version parsedVersion = Version.parseVersion(version.get());
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
            Version parsedVersion = Version.parseVersion(version.get());
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
        Map<String, DeploymentStatus> versionToDeploymentStatus = new HashMap<>();
        Set<Version> versions = statuses.versions();
        for (Version version : versions) {
            DeploymentStatus status = statuses.status(version);
            if (statusFilter.test(status)) {
                versionToDeploymentStatus.put(version.render(), status);
            }
        }
        if (versionToDeploymentStatus.isEmpty()) {
            return null;
        }
        return new UnitStatus(statuses.id(), versionToDeploymentStatus);
    }

    private static Predicate<DeploymentStatus> createStatusFilter(Optional<List<DeploymentStatus>> statuses) {
        if (statuses.isEmpty() || statuses.get().isEmpty()) {
            return status -> true;
        } else {
            EnumSet<DeploymentStatus> statusesSet = EnumSet.copyOf(statuses.get());
            return statusesSet::contains;
        }
    }
}
