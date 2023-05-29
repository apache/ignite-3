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
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

import java.io.IOException;
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


    private final ExecutorService executor = Executors.newFixedThreadPool(
            DEPLOYMENT_EXECUTOR_SIZE, new NamedThreadFactory("deployment", LOG));

    public void initUnitsFolder(Path unitsFolder) {
        this.unitsFolder = unitsFolder;
    }

    /**
     * Deploy provided unit on local fs.
     *
     * @param id Deploy unit identifier.
     * @param version Deploy unit version.
     * @param unitContent Map of deploy unit file names to file content.
     * @return Future with deploy result.
     */
    public CompletableFuture<Boolean> deploy(String id, String version, UnitContent unitContent) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Path unitFolder = unitsFolder
                        .resolve(id)
                        .resolve(version);

                Files.createDirectories(unitFolder);

                for (Entry<String, byte[]> entry : unitContent) {
                    String fileName = entry.getKey();
                    Path unitPath = unitFolder.resolve(fileName);
                    Path unitPathTmp = unitFolder.resolve(fileName + TMP_SUFFIX);
                    Files.write(unitPathTmp, entry.getValue(), CREATE, SYNC, TRUNCATE_EXISTING);
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
     * Undeploy unit with provided identifier and version.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @return Future with undeploy result
     */
    public CompletableFuture<Boolean> undeploy(String id, String version) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Path unitPath = unitsFolder
                        .resolve(id)
                        .resolve(version);

                IgniteUtils.deleteIfExistsThrowable(unitPath);
                return true;
            } catch (IOException e) {
                LOG.debug("Failed to get content for unit " + id + ":" + version, e);
                return false;
            }
        }, executor);
    }

    /**
     * Read from local FileSystem and returns deployment unit content.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @return Deployment unit content.
     */
    public CompletableFuture<UnitContent> getUnitContent(String id, String version) {
        return CompletableFuture.supplyAsync(() -> {
            Map<String, byte[]> result = new HashMap<>();
            try {
                Path unitPath = unitsFolder
                        .resolve(id)
                        .resolve(version);

                Files.walkFileTree(unitPath, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        result.put(file.getFileName().toString(), Files.readAllBytes(file));
                        return FileVisitResult.CONTINUE;
                    }
                });

            } catch (IOException e) {
                LOG.debug("Failed to undeploy unit " + id + ":" + version, e);
            }
            return new UnitContent(result);
        }, executor);
    }
}
