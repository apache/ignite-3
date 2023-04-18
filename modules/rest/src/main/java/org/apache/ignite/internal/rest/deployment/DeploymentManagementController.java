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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.deployunit.DeploymentUnit;
import org.apache.ignite.internal.deployunit.IgniteDeployment;
import org.apache.ignite.internal.deployunit.version.Version;
import org.apache.ignite.internal.rest.api.deployment.DeploymentCodeApi;
import org.apache.ignite.internal.rest.api.deployment.DeploymentInfo;
import org.apache.ignite.internal.rest.api.deployment.UnitStatus;
import org.reactivestreams.Publisher;

/**
 * Implementation of {@link DeploymentCodeApi}.
 */
@Controller("/management/v1/deployment")
public class DeploymentManagementController implements DeploymentCodeApi {
    private final IgniteDeployment deployment;

    public DeploymentManagementController(IgniteDeployment deployment) {
        this.deployment = deployment;
    }

    @Override
    public CompletableFuture<Boolean> deploy(String unitId, String unitVersion, Publisher<CompletedFileUpload> unitContent) {
        CompletableFuture<DeploymentUnit> result = new CompletableFuture<>();
        unitContent.subscribe(new CompetedFileUploadSubscriber(result));
        return result.thenCompose(deploymentUnit -> deployment.deployAsync(unitId, parseVersion(unitVersion), deploymentUnit));
    }

    @Override
    public CompletableFuture<Void> undeploy(String unitId, String unitVersion) {
        return deployment.undeployAsync(unitId, Version.parseVersion(unitVersion));
    }

    @Override
    public CompletableFuture<Void> undeploy(String unitId) {
        return deployment.undeployAsync(unitId);
    }

    @Override
    public CompletableFuture<Collection<UnitStatus>> units() {
        return deployment.unitsAsync().thenApply(statuses -> statuses.stream().map(DeploymentManagementController::fromUnitStatus)
                .collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Collection<String>> versions(String unitId) {
        return deployment.versionsAsync(unitId)
                .thenApply(versions -> versions.stream().map(Version::render).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<UnitStatus> status(String unitId) {
        return deployment.statusAsync(unitId).thenApply(DeploymentManagementController::fromUnitStatus);
    }

    @Override
    public CompletableFuture<Collection<UnitStatus>> findByConsistentId(String consistentId) {
        return deployment.findUnitByConsistentIdAsync(consistentId)
                .thenApply(units -> units.stream().map(DeploymentManagementController::fromUnitStatus)
                        .collect(Collectors.toList()));
    }

    /**
     * Mapper method.
     *
     * @param status Unit status.
     * @return Unit status DTO.
     */
    public static UnitStatus fromUnitStatus(org.apache.ignite.internal.deployunit.UnitStatus status) {
        Map<String, DeploymentInfo> versionToDeploymentStatus = new HashMap<>();
        Set<Version> versions = status.versions();
        for (Version version : versions) {
            DeploymentInfo info = new DeploymentInfo(status.status(version), status.consistentIds(version));
            versionToDeploymentStatus.put(version.render(), info);
        }
        return new UnitStatus(status.id(), versionToDeploymentStatus);
    }

    private static Version parseVersion(String unitVersion) {
        if (unitVersion == null || unitVersion.isBlank()) {
            return Version.LATEST;
        }
        return Version.parseVersion(unitVersion);
    }
}
