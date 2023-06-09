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
import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.UPLOADING;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitUnavailableException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.RefCountedObjectPool;

/**
 * Implementation of {@link DeploymentUnitAccessor}.
 */
public class DeploymentUnitAccessorImpl implements DeploymentUnitAccessor {

    private static final IgniteLogger LOG = Loggers.forClass(DeploymentUnitAccessorImpl.class);
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
    public DisposableDeploymentUnit acquire(DeploymentUnit unit) {
        return pool.acquire(unit, ignored -> new DisposableDeploymentUnit(
                        unit,
                        deployment.path(unit.name(), unit.version()),
                        () -> pool.release(unit)
                )
        );
    }

    @Override
    public boolean isAcquired(DeploymentUnit unit) {
        return pool.isAcquired(unit);
    }


    /**
     * Deploys deployment unit to the cluster and returns path to the deployment unit.
     */
    @Override
    public CompletableFuture<Void> onDemandDeploy(DeploymentUnit unit) {
        return deployment.onDemandDeploy(unit.name(), unit.version())
                .thenCompose(result -> {
                    if (result) {
                        return completedFuture(null);
                    } else {
                        LOG.error("Failed to deploy on demand deployment unit {}:{}", unit.name(), unit.version());
                        return deployment.clusterStatusAsync(unit.name(), unit.version())
                                .thenCombine(deployment.nodeStatusAsync(unit.name(), unit.version()), (clusterStatus, nodeStatus) -> {
                                    if (clusterStatus == null) {
                                        return CompletableFuture.<Void>failedFuture(
                                                new DeploymentUnitNotFoundException(unit.name(), unit.version()));
                                    } else {
                                        return CompletableFuture.<Void>failedFuture(
                                                new DeploymentUnitUnavailableException(
                                                        unit.name(),
                                                        unit.version(),
                                                        clusterStatus,
                                                        nodeStatus
                                                )
                                        );
                                    }
                                }).thenCompose(it -> it);
                    }
                });
    }
}
