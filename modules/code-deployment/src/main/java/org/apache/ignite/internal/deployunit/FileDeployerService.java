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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Service for file deploying on local File System.
 */
public class FileDeployerService {
    private static final IgniteLogger LOG = Loggers.forClass(FileDeployerService.class);

    private static final String TMP_SUFFIX = ".tmp";

    private static final int DEPLOYMENT_EXECUTOR_SIZE = 4;

    /**
     * Folder for units.
     */
    private Path unitsFolder;


    private final ExecutorService executor;

    /** Constructor. */
    public FileDeployerService(String nodeName) {
        executor = Executors.newFixedThreadPool(
                DEPLOYMENT_EXECUTOR_SIZE,
                NamedThreadFactory.create(nodeName, "deployment", LOG)
        );
    }

    public void initUnitsFolder(Path unitsFolder) {
        this.unitsFolder = unitsFolder;
    }

    /**
     * Deploys provided unit on local fs.
     *
     * @param id Deploy unit identifier.
     * @param version Deploy unit version.
     * @param deploymentUnit Deployment unit.
     * @return Future with deploy result.
     */
    public CompletableFuture<Boolean> deploy(String id, Version version, DeploymentUnit deploymentUnit) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Path unitFolder = unitPath(id, version);

                Files.createDirectories(unitFolder);

                for (Entry<String, InputStream> entry : deploymentUnit.content().entrySet()) {
                    String fileName = entry.getKey();
                    Path unitPath = unitFolder.resolve(fileName);
                    Path unitPathTmp = unitFolder.resolve(fileName + TMP_SUFFIX);
                    Files.copy(entry.getValue(), unitPathTmp, REPLACE_EXISTING);
                    Files.move(unitPathTmp, unitPath, ATOMIC_MOVE, REPLACE_EXISTING);
                }
                return true;
            } catch (IOException e) {
                LOG.error("Failed to deploy unit " + id + ":" + version, e);
                return false;
            }
        }, executor);
    }

    /**
     * Undeploys unit with provided identifier and version.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @return Future with undeploy result
     */
    public CompletableFuture<Boolean> undeploy(String id, Version version) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                IgniteUtils.deleteIfExistsThrowable(unitPath(id, version));
                return true;
            } catch (IOException e) {
                LOG.debug("Failed to undeploy unit " + id + ":" + version, e);
                return false;
            }
        }, executor);
    }

    /**
     * Reads from local FileSystem and returns deployment unit content.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @return Deployment unit content.
     */
    public CompletableFuture<UnitContent> getUnitContent(String id, Version version) {
        return CompletableFuture.supplyAsync(() -> {
            Map<String, byte[]> result = new HashMap<>();
            try {
                Files.walkFileTree(unitPath(id, version), new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        result.put(file.getFileName().toString(), Files.readAllBytes(file));
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException e) {
                LOG.debug("Failed to get content for unit " + id + ":" + version, e);
            }
            return new UnitContent(result);
        }, executor);
    }

    /**
     * Returns path to unit folder.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @param checkExistence If {@code true} then check that unit exists.
     * @return Path to unit folder.
     * @throws DeploymentUnitNotFoundException If unit doesn't exist and {@code checkExistence} is {@code true}.
     */
    Path unitPath(String id, Version version, boolean checkExistence) {
        Path path = unitPath(id, version);
        if (checkExistence && !Files.exists(path)) {
            throw new DeploymentUnitNotFoundException(id, version);
        }
        return path;
    }

    /**
     * Returns path to unit folder.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @return Path to unit folder.
     */
    Path unitPath(String id, Version version) {
        return unitsFolder.resolve(id).resolve(version.render());
    }

    /**
     * Stops the deployer service.
     */
    public void stop() {
        executor.shutdown();
    }
}
