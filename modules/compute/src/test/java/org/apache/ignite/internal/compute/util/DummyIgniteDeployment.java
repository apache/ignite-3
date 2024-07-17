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

package org.apache.ignite.internal.compute.util;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.deployunit.DeploymentStatus;
import org.apache.ignite.internal.deployunit.DeploymentUnit;
import org.apache.ignite.internal.deployunit.IgniteDeployment;
import org.apache.ignite.internal.deployunit.NodesToDeploy;
import org.apache.ignite.internal.deployunit.UnitStatuses;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.apache.ignite.internal.manager.ComponentContext;

/**
 * Implementation of {@link IgniteDeployment} for tests.
 */
public class DummyIgniteDeployment implements IgniteDeployment {
    private final Path unitsPath;

    public DummyIgniteDeployment(Path unitsPath) {
        this.unitsPath = unitsPath;
    }

    @Override
    public CompletableFuture<Boolean> deployAsync(
            String id,
            Version version,
            boolean force,
            DeploymentUnit deploymentUnit,
            NodesToDeploy nodesToDeploy) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<Boolean> undeployAsync(String id, Version version) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<List<UnitStatuses>> clusterStatusesAsync() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<UnitStatuses> clusterStatusesAsync(String id) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<DeploymentStatus> clusterStatusAsync(String id, Version version) {
        return completedFuture(path(id, version)).thenApply(path -> DEPLOYED);
    }

    @Override
    public CompletableFuture<List<Version>> versionsAsync(String id) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<List<UnitStatuses>> nodeStatusesAsync() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<UnitStatuses> nodeStatusesAsync(String id) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<DeploymentStatus> nodeStatusAsync(String id, Version version) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<Boolean> onDemandDeploy(String id, Version version) {
        return completedFuture(path(id, version))
                .thenCompose(path -> completedFuture(Files.exists(path)));
    }

    @Override
    public CompletableFuture<Version> detectLatestDeployedVersion(String id) {
        Path path = unitsPath.resolve(id);
        if (!Files.exists(path)) {
            return failedFuture(new DeploymentUnitNotFoundException(id));
        }

        return CompletableFuture.supplyAsync(() -> Arrays.stream(path.toFile().listFiles())
                .filter(File::isDirectory)
                .map(File::getName)
                .map(Version::parseVersion)
                .max(Version::compareTo)
                .orElseThrow());
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }

    private Path path(String id, Version version) {
        Path path = unitsPath.resolve(id).resolve(version.toString());
        if (Files.exists(path)) {
            return path;
        } else {
            throw new DeploymentUnitNotFoundException(id, version);
        }
    }
}
