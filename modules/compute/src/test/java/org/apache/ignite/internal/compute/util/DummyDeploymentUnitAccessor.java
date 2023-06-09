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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.deployunit.DeploymentUnitAccessor;
import org.apache.ignite.internal.deployunit.DisposableDeploymentUnit;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.apache.ignite.internal.util.RefCountedObjectPool;

/**
 * Implementation of {@link DeploymentUnitAccessor} that uses local file system to store deployment units.
 */
public class DummyDeploymentUnitAccessor implements DeploymentUnitAccessor {
    private final RefCountedObjectPool<DeploymentUnit, DisposableDeploymentUnit> pool = new RefCountedObjectPool<>();

    private final Path unitsPath;

    public DummyDeploymentUnitAccessor(Path unitsPath) {
        this.unitsPath = unitsPath;
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
    public DisposableDeploymentUnit acquire(DeploymentUnit unit) {
        return pool.acquire(unit, ignored -> new DisposableDeploymentUnit(unit, path(unit), () -> pool.release(unit)));
    }

    @Override
    public boolean isAcquired(DeploymentUnit unit) {
        return pool.isAcquired(unit);
    }

    @Override
    public CompletableFuture<Void> onDemandDeploy(DeploymentUnit unit) {
        return completedFuture(path(unit))
                .thenCompose(path -> {
                    if (Files.exists(path)) {
                        return completedFuture(null);
                    } else {
                        return failedFuture(new DeploymentUnitNotFoundException(unit.name(), unit.version()));
                    }
                });
    }

    private Path path(DeploymentUnit unit) {
        Path path = unitsPath.resolve(unit.name()).resolve(unit.version().toString());
        if (Files.exists(path)) {
            return path;
        } else {
            throw new DeploymentUnitNotFoundException(unit.name(), unit.version());
        }
    }
}
