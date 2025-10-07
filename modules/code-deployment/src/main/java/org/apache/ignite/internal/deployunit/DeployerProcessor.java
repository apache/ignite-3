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

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.ignite.internal.deployunit.DeployerProcessor.DeployArg;
import org.apache.ignite.internal.deployunit.tempstorage.TempStorage;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link DeploymentUnitProcessor} that deploys deployment unit content to the file system.
 *
 * <p>This processor extracts and deploys files from deployment units to a specified target directory.
 * It handles both regular deployment units (containing individual files as input streams) and ZIP-based
 * deployment units (containing compressed archives that need extraction).
 *
 * <p>The deployment process ensures atomic file operations by:
 * <ul>
 *     <li>First copying files to temporary locations with a {@code .tmp} suffix</li>
 *     <li>Then atomically moving them to their final destinations</li>
 *     <li>Creating necessary parent directories as needed</li>
 * </ul>
 *
 * <p>This approach prevents partial deployments and ensures that files are either fully deployed
 * or not deployed at all, maintaining consistency during the deployment process.
 *
 * <p>Type parameters:
 * <ul>
 *     <li>{@code Path} - the argument type representing the target deployment directory</li>
 * </ul>
 */
class DeployerProcessor implements DeploymentUnitProcessor<DeployArg, Boolean> {
    private static final IgniteLogger LOG = Loggers.forClass(DeployerProcessor.class);

    private final Executor executor;

    public DeployerProcessor(Executor executor) {
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Boolean> processFilesContent(FilesDeploymentUnit unit, DeployArg deployArg) {
        return wrap(() -> {
            for (Entry<String, Path> e : unit.content().entrySet()) {
                Files.move(e.getValue(), deployArg.unitFolder.resolve(e.getKey()), ATOMIC_MOVE, REPLACE_EXISTING);
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> processStreamContent(StreamDeploymentUnit unit, DeployArg deployArg) {
        CompletableFuture<?>[] array = unit.content()
                .entrySet()
                .stream()
                .map(e ->
                        deployArg.tempStorage.store(e.getKey(), e.getValue())
                                .thenCompose(path -> doDeploy(deployArg.unitFolder, e.getKey(), path))
                ).toArray(CompletableFuture[]::new);
        return allOf(array).handle(((unused, throwable) -> throwable == null));
    }

    @Override
    public CompletableFuture<Boolean> processContentWithUnzip(ZipDeploymentUnit unit, DeployArg deployArg) {
        ZipInputStream zis = unit.zis();
        try {
            ZipEntry nextEntry = zis.getNextEntry();
            return processZipFile(nextEntry, zis, deployArg)
                    .handle((unused, throwable) -> throwable == null);
        } catch (IOException e) {
            return falseCompletedFuture();
        }
    }

    private CompletableFuture<Void> processZipFile(@Nullable ZipEntry ze, ZipInputStream zis, DeployArg deployArg) {
        if (ze == null) {
            return nullCompletedFuture();
        }
        String entryName = ze.getName();
        if (ze.isDirectory()) {
            try {
                // To support empty dirs.
                Path entryPath = deployArg.unitFolder.resolve(entryName);
                Files.createDirectories(entryPath);
                return processZipFile(zis.getNextEntry(), zis, deployArg);
            } catch (IOException e) {
                return failedFuture(e);
            }
        } else {
            return deployArg.tempStorage.store(entryName, zis)
                    .thenCompose(path -> doDeploy(deployArg.unitFolder, entryName, path))
                    .thenCompose(unused -> {
                        try {
                            return processZipFile(zis.getNextEntry(), zis, deployArg);
                        } catch (IOException e) {
                            return failedFuture(e);
                        }
                    });
        }
    }

    private CompletableFuture<Boolean> wrap(RunnableX runnableX) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                runnableX.run();
                return true;
            } catch (Throwable t) {
                LOG.error("Failed to process deploy action.", t);
                return false;
            }
        }, executor);
    }

    private CompletableFuture<Boolean> doDeploy(Path unitFolder, String entryName, Path deployment) {
        return wrap(() -> {
            Path unitPath = unitFolder.resolve(entryName);
            Files.createDirectories(unitPath.getParent());
            Files.move(deployment, unitPath, ATOMIC_MOVE, REPLACE_EXISTING);
        });
    }

    static class DeployArg {
        private final Path unitFolder;

        private final TempStorage tempStorage;

        DeployArg(Path unitFolder, TempStorage tempStorage) {
            this.unitFolder = unitFolder;
            this.tempStorage = tempStorage;
        }
    }
}
