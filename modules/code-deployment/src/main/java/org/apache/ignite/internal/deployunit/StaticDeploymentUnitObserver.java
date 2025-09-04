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

import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.deployment.version.Version.parseVersion;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.util.CompletableFutures.allOf;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.deployunit.metastore.DeploymentUnitStore;
import org.apache.ignite.internal.deployunit.metastore.status.UnitNodeStatus;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

public class StaticDeploymentUnitObserver {
    private static final IgniteLogger LOG = Loggers.forClass(StaticDeploymentUnitObserver.class);

    private final DeploymentUnitStore deploymentUnitStore;

    private final String nodeName;

    private final Path deploymentUnitsRoot;

    public StaticDeploymentUnitObserver(
            DeploymentUnitStore deploymentUnitStore,
            String nodeName,
            Path deploymentUnitsRoot
    ) {
        this.deploymentUnitStore = deploymentUnitStore;
        this.nodeName = nodeName;
        this.deploymentUnitsRoot = deploymentUnitsRoot;
    }

    public CompletableFuture<Void> observeAndRegisterStaticUnits() {
        Map<String, List<Version>> staticUnits = collectStaticUnits();



        return deploymentUnitStore.getNodeStatuses(nodeName).thenCompose(statuses -> {
            List<CompletableFuture<?>> futures = new ArrayList<>();

            for (UnitNodeStatus status : statuses) {
                staticUnits.get(status.id()).remove(status.version());
                if (staticUnits.get(status.id()).isEmpty()) {
                    staticUnits.remove(status.id());
                }
            }

            staticUnits.forEach((id, versions) -> {
                versions.forEach(version -> {
                    LOG.info("Start processing unit {}:{}", id, version);
                    CompletableFuture<Boolean> future = deploymentUnitStore.createClusterStatus(id, version, Set.of(nodeName))
                            .thenCompose(status -> {
                                if (status == null) {
                                    return deploymentUnitStore.getClusterStatus(id, version).thenCompose(it ->
                                            deploymentUnitStore.createNodeStatus(nodeName, id, version, it.opId(), DEPLOYED)
                                    );
                                } else {
                                    return deploymentUnitStore.createNodeStatus(nodeName, id, version, status.opId(), DEPLOYED);
                                }
                            })
                            .whenComplete((result, t) -> LOG.info("Finished static status creating {}:{} with result {}", t, id, version, result));
                    futures.add(future);
                });
            });

            return allOf(futures);
        });


    }

    private Map<String, List<Version>> collectStaticUnits() {
        Map<String, List<Version>> units = new HashMap<>();
        List<Path> unitFolders = allSubdirectories(deploymentUnitsRoot);
        for (Path unitFolder : unitFolders) {
            List<Path> versions = allSubdirectories(unitFolder);
            units.put(
                    unitFolder.getFileName().toString(),
                    versions.stream()
                            .map(versionFolder -> parseVersion(versionFolder.getFileName().toString()))
                            .collect(Collectors.toList())
            );
        }
        return units;
    }

    private static List<Path> allSubdirectories(Path folder) {
        List<Path> subfolders = new ArrayList<>();
        try {
            Files.walkFileTree(folder, Set.of(), 2, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    if (!dir.equals(folder)) {
                        subfolders.add(dir);
                    }
                    return super.preVisitDirectory(dir, attrs);
                }
            });
        } catch (IOException e) {

        }
        return subfolders;
    }
}
