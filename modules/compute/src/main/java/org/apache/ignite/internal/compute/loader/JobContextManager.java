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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.deployunit.DeploymentUnitAccessor;
import org.apache.ignite.internal.deployunit.DisposableDeploymentUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.RefCountedObjectPool;
import org.apache.ignite.lang.IgniteInternalException;

/**
 * Manages job context.
 */
public class JobContextManager {
    private static final IgniteLogger LOG = Loggers.forClass(JobContextManager.class);

    private final Executor executorService = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() * 2,
            new NamedThreadFactory("job-context-manager-pool", LOG)
    );

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
        DeploymentUnit[] accumulator = new DeploymentUnit[units.size()];
        return CompletableFuture.supplyAsync(() -> units, executorService)
                .thenCompose(it -> mapList(it, this::normalizeVersion, accumulator))
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Couldn't normalize deployment units: {}, exception: ", units, e);
                    }
                })
                .thenApply(v -> Arrays.asList(accumulator))
                .thenApply(deploymentUnits -> classLoaderPool.acquire(deploymentUnits, () -> createClassLoader(deploymentUnits)))
                .thenApply(loader -> new JobContext(loader, this::releaseClassLoader));
    }

    private CompletableFuture<DeploymentUnit> normalizeVersion(DeploymentUnit unit) {
        if (unit.version() == Version.LATEST) {
            return deploymentUnitAccessor.detectLatestDeployedVersion(unit.name())
                    .thenApply(version -> new DeploymentUnit(unit.name(), version));
        } else {
            return CompletableFuture.completedFuture(unit);
        }
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

    private JobClassLoader createClassLoader(List<DeploymentUnit> units) {
        DisposableDeploymentUnit[] accumulator = new DisposableDeploymentUnit[units.size()];
        try {
            return mapList(
                    units,
                    it -> deploymentUnitAccessor.acquire(it)
                            .whenComplete((v, e) -> {
                                        if (e != null) {
                                            LOG.error("Couldn't acquire deployment unit: {}, exception: {}", it, e);
                                        }
                                    }
                            ),
                    accumulator
            ).thenApply(v -> classLoaderFactory.createClassLoader(Arrays.asList(accumulator))).get();
        } catch (ExecutionException | InterruptedException e) {
            for (DisposableDeploymentUnit disposableDeploymentUnit : accumulator) {
                if (disposableDeploymentUnit != null) {
                    disposableDeploymentUnit.release();
                }
            }

            throw new IgniteInternalException(e);
        }
    }

    private static <I, O> CompletableFuture<List<O>> mapList(
            List<I> list,
            Function<I, CompletableFuture<O>> mapper,
            O[] accumulator
    ) {
        CompletableFuture<Void>[] futures = IntStream.range(0, list.size())
                .mapToObj(id -> mapper.apply(list.get(id))
                        .thenAccept(result -> accumulator[id] = result))
                .<CompletableFuture<Void>>toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures)
                .thenApply(ignored -> Arrays.asList(accumulator));
    }
}
