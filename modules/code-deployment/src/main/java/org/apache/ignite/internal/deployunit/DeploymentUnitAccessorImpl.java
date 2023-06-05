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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.UPLOADING;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitUnavailableException;
import org.apache.ignite.internal.util.RefCountedObjectPool;

/**
 * Implementation of {@link DeploymentUnitAccessor}.
 */
public class DeploymentUnitAccessorImpl implements DeploymentUnitAccessor {
    private final RefCountedObjectPool<DeploymentUnit, DisposableDeploymentUnit> pool = new RefCountedObjectPool<>();

    private final IgniteDeployment deployment;

    public DeploymentUnitAccessorImpl(IgniteDeployment deployment) {
        this.deployment = deployment;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Version> detectLatestDeployedVersion(String id) {
        return deployment.clusterStatusesAsync(id)
                .thenApply(statuses -> {
                    return statuses.versions()
                            .stream()
                            .filter(version -> statuses.status(version) == DEPLOYED || statuses.status(version) == UPLOADING)
                            .max(Version::compareTo)
                            .orElseThrow(() -> new DeploymentUnitNotFoundException(id));
                });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<DisposableDeploymentUnit> acquire(DeploymentUnit unit) {
        return checkStatus(unit).thenCompose(v -> localPath(unit)
                .thenApply(path -> pool.acquire(unit, () -> {
                    return new DisposableDeploymentUnit(
                            unit,
                            path,
                            () -> pool.release(unit));
                }))
        );
    }

    @Override
    public boolean isAcquired(DeploymentUnit unit) {
        return pool.isAcquired(unit);
    }


    /**
     * Deploys deployment unit to the cluster and returns path to the deployment unit.
     */
    private CompletableFuture<Path> localPath(DeploymentUnit unit) {
        return deployment.onDemandDeploy(unit.name(), unit.version())
                .thenCompose(v -> deployment.path(unit.name(), unit.version()));
    }

    /**
     * Checks that deployment unit is deployed to the cluster and throws exception if it is not.
     *
     * @throws DeploymentUnitUnavailableException if deployment unit is not deployed to the cluster.
     */
    private CompletableFuture<Void> checkStatus(DeploymentUnit unit) {
        return deployment.clusterStatusAsync(unit.name(), unit.version())
                .thenCompose(clusterStatus -> {
                    if (clusterStatus == null) {
                        return failedFuture(new DeploymentUnitNotFoundException(unit.name(), unit.version()));
                    } else if (clusterStatus == UPLOADING || clusterStatus == DEPLOYED) {
                        return completedFuture(null);
                    } else {
                        return deployment.nodeStatusAsync(unit.name(), unit.version())
                                .thenCompose(nodeStatus -> failedFuture(
                                        new DeploymentUnitUnavailableException(
                                                unit.name(),
                                                unit.version(),
                                                clusterStatus,
                                                nodeStatus
                                        )
                                ));
                    }
                });
    }
}
