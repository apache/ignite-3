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

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.deployunit.DeploymentUnitAccessor;
import org.apache.ignite.internal.deployunit.DisposableDeploymentUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.RefCountedObjectPool;

/**
 * Manages job context.
 */
public class JobContextManager {
    private static final IgniteLogger LOG = Loggers.forClass(JobContextManager.class);

    private final RefCountedObjectPool<List<DeploymentUnit>, JobClassLoader> classLoaderPool = new RefCountedObjectPool<>();

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
     * @param deploymentUnitAccessor The deployer service.
     * @param classLoaderFactory The class loader factory.
     */
    public JobContextManager(DeploymentUnitAccessor deploymentUnitAccessor, JobClassLoaderFactory classLoaderFactory) {
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
                .thenCompose(normalizedUnits -> onDemandDeploy(normalizedUnits).thenApply(v -> normalizedUnits))
                .thenApply(normalizedUnits -> classLoaderPool.acquire(normalizedUnits, this::createClassLoader))
                .whenComplete((ctx, err) -> {
                    if (err != null) {
                        LOG.error("Failed to acquire class loader for units: " + units, err);
                    } else {
                        LOG.debug("Acquired class loader for units: " + units);
                    }
                })
                .thenApply(loader -> new JobContext(loader, this::releaseClassLoader));
    }

    private JobClassLoader createClassLoader(List<DeploymentUnit> units) {
        List<DisposableDeploymentUnit> disposableDeploymentUnits = units.stream()
                .map(deploymentUnitAccessor::acquire)
                .collect(Collectors.toList());
        return classLoaderFactory.createClassLoader(disposableDeploymentUnits);
    }

    /**
     * Releases a class loader for the given deployment units.
     */
    private void releaseClassLoader(JobContext jobContext) {
        List<DeploymentUnit> units = jobContext.classLoader().units().stream()
                .map(DisposableDeploymentUnit::unit)
                .collect(Collectors.toList());
        if (classLoaderPool.release(units)) {
            jobContext.classLoader().close();
        }
    }

    private CompletableFuture<List<DeploymentUnit>> normalizeVersions(List<DeploymentUnit> units) {
        return mapList(units, DeploymentUnit.class, this::normalizeVersion);
    }

    private CompletableFuture<Void> onDemandDeploy(List<DeploymentUnit> units) {
        return mapList(units, Void.class, deploymentUnitAccessor::onDemandDeploy)
                .thenApply(ignored -> null);
    }

    private CompletableFuture<DeploymentUnit> normalizeVersion(DeploymentUnit unit) {
        if (unit.version() == Version.LATEST) {
            return deploymentUnitAccessor.detectLatestDeployedVersion(unit.name())
                    .thenApply(version -> new DeploymentUnit(unit.name(), version));
        } else {
            return CompletableFuture.completedFuture(unit);
        }
    }

    private static <I, O> CompletableFuture<List<O>> mapList(
            List<I> list,
            Class<O> outputClass,
            Function<I, CompletableFuture<O>> mapper
    ) {
        O[] accumulator = (O[]) Array.newInstance(outputClass, list.size());
        CompletableFuture<Void>[] futures = IntStream.range(0, list.size())
                .mapToObj(id -> mapper.apply(list.get(id))
                        .thenAccept(result -> accumulator[id] = result))
                .<CompletableFuture<Void>>toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures)
                .thenApply(ignored -> Arrays.asList(accumulator));
    }
}
