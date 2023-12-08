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

package org.apache.ignite.internal.compute.loader;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.deployunit.DeploymentStatus;
import org.apache.ignite.internal.deployunit.DeploymentUnitAccessor;
import org.apache.ignite.internal.deployunit.DisposableDeploymentUnit;
import org.apache.ignite.internal.deployunit.IgniteDeployment;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitUnavailableException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.RefCountedObjectPool;
import org.apache.ignite.lang.ErrorGroups.Compute;

/**
 * Manages job context.
 */
public class JobContextManager {
    private static final IgniteLogger LOG = Loggers.forClass(JobContextManager.class);

    private final RefCountedObjectPool<List<DeploymentUnit>, JobClassLoader> classLoaderPool = new RefCountedObjectPool<>();

    private final IgniteDeployment deployment;
    /**
     * The deployer service.
     */
    private final DeploymentUnitAccessor deploymentUnitAccessor;

    /**
     * The class loader factory.
     */
    private final JobClassLoaderFactory classLoaderFactory;

    /**
     * Constructor.
     *
     * @param deployment The deployment.
     * @param deploymentUnitAccessor The deployer service.
     * @param classLoaderFactory The class loader factory.
     */
    public JobContextManager(
            IgniteDeployment deployment,
            DeploymentUnitAccessor deploymentUnitAccessor,
            JobClassLoaderFactory classLoaderFactory
    ) {
        this.deployment = deployment;
        this.deploymentUnitAccessor = deploymentUnitAccessor;
        this.classLoaderFactory = classLoaderFactory;
    }

    /**
     * Acquires a class loader for the given deployment units.
     *
     * @param units The deployment units.
     * @return The class loader.
     */
    public CompletableFuture<JobContext> acquireClassLoader(List<DeploymentUnit> units) {
        return normalizeVersions(units)
                .thenCompose(normalizedUnits -> checkUnitStatuses(normalizedUnits).thenApply(v -> normalizedUnits))
                .thenCompose(normalizedUnits -> onDemandDeploy(normalizedUnits).thenApply(v -> normalizedUnits))
                .thenApply(normalizedUnits -> classLoaderPool.acquire(normalizedUnits, this::createClassLoader))
                .thenApply(loader -> new JobContext(loader, this::releaseClassLoader))
                .whenComplete((context, error) -> {
                    if (error != null) {
                        LOG.error("Failed to acquire class loader for units: " + units, error);
                    } else {
                        LOG.debug("Acquired class loader for units: " + units);
                    }
                });
    }

    /**
     * Creates a class loader for the given deployment units. The units will be acquired. The class loader will be closed when it is not
     * used by any other job.
     */
    private JobClassLoader createClassLoader(List<DeploymentUnit> units) {
        List<DisposableDeploymentUnit> disposableDeploymentUnits = units.stream()
                .map(deploymentUnitAccessor::acquire)
                .collect(Collectors.toList());
        return classLoaderFactory.createClassLoader(disposableDeploymentUnits);
    }

    /**
     * Releases a class loader. If the class loader is not used by any other job, it will be closed and the deployment units will be
     * released.
     */
    private void releaseClassLoader(JobContext jobContext) {
        List<DeploymentUnit> units = jobContext.classLoader().units().stream()
                .map(DisposableDeploymentUnit::unit)
                .collect(Collectors.toList());
        if (classLoaderPool.release(units)) {
            jobContext.classLoader().close();
        }
    }

    /**
     * Check if the deployment units are deployed to the cluster. Only deployed units can be used in compute.
     */
    private CompletableFuture<Void> checkUnitStatuses(List<DeploymentUnit> units) {
        return mapList(units, this::checkUnitStatus, CompletableFuture::allOf);
    }

    private CompletableFuture<Void> checkUnitStatus(DeploymentUnit unit) {
        return deployment.clusterStatusAsync(unit.name(), unit.version())
                .thenCompose(clusterStatus -> {
                    if (clusterStatus == DeploymentStatus.DEPLOYED) {
                        return nullCompletedFuture();
                    } else if (clusterStatus == null) {
                        return failedFuture(new DeploymentUnitNotFoundException(unit.name(), unit.version()));
                    } else {
                        return deployment.nodeStatusAsync(unit.name(), unit.version())
                                .thenCompose(nodeStatus -> failedFuture(new DeploymentUnitUnavailableException(
                                        unit.name(),
                                        unit.version(),
                                        clusterStatus,
                                        nodeStatus
                                )));
                    }
                });
    }

    private CompletableFuture<List<DeploymentUnit>> normalizeVersions(List<DeploymentUnit> units) {
        return mapList(units, this::normalizeVersion, CompletableFutures::allOf);
    }

    private CompletableFuture<Void> onDemandDeploy(List<DeploymentUnit> units) {
        return mapList(
                units,
                unit -> deployment.onDemandDeploy(unit.name(), unit.version())
                        .thenAccept(result -> {
                            if (!result) {
                                throw new IgniteInternalException(
                                        Compute.CLASS_LOADER_ERR, "Failed to deploy on demand unit: " + unit.render()
                                );
                            }
                        }),
                CompletableFuture::allOf
        );
    }

    private CompletableFuture<DeploymentUnit> normalizeVersion(DeploymentUnit unit) {
        if (unit.version() == Version.LATEST) {
            return deployment.detectLatestDeployedVersion(unit.name())
                    .thenApply(version -> new DeploymentUnit(unit.name(), version));
        } else {
            return completedFuture(unit);
        }
    }

    private static <I, O, R> CompletableFuture<R> mapList(
            List<I> list,
            Function<I, CompletableFuture<O>> mapper,
            Function<CompletableFuture<O>[], CompletableFuture<R>> collector
    ) {
        CompletableFuture<O>[] futures = new CompletableFuture[list.size()];

        for (int i = 0; i < list.size(); i++) {
            futures[i] = mapper.apply(list.get(i));
        }

        return collector.apply(futures);
    }
}
