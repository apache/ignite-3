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

import static java.util.Collections.emptyList;
import static org.apache.ignite.deployment.version.Version.parseVersion;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.UPLOADING;
import static org.apache.ignite.internal.util.CompletableFutures.allOf;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitReadException;
import org.apache.ignite.internal.deployunit.metastore.DeploymentUnitStore;
import org.apache.ignite.internal.deployunit.metastore.status.UnitNodeStatus;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Observes a predefined directory with statically provisioned deployment units and registers their presence in the deployment store.
 *
 * <p>The observer scans the {@code deploymentUnitsRoot} directory, expecting the following structure:
 * {@code <unitId>/<version>/} For every discovered {@code unitId}-{@code version} pair that is not yet registered for this node, the
 * observer creates (or reuses) a cluster status and then creates the node status marked as {@link DeploymentStatus#DEPLOYED}.
 */
public class StaticUnitDeployer {
    private static final IgniteLogger LOG = Loggers.forClass(StaticUnitDeployer.class);

    private final DeploymentUnitStore deploymentUnitStore;

    private final String nodeName;

    private final Path deploymentUnitsRoot;

    /**
     * Constructor.
     */
    public StaticUnitDeployer(
            DeploymentUnitStore deploymentUnitStore,
            String nodeName,
            Path deploymentUnitsRoot
    ) {
        this.deploymentUnitStore = deploymentUnitStore;
        this.nodeName = nodeName;
        this.deploymentUnitsRoot = deploymentUnitsRoot;
    }

    /**
     * Performs startup deployment unit synchronization.
     *
     * <ol>
     *     <li>Scans the filesystem for statically deployed units and registers them if not yet present in the store.</li>
     *     <li>Checks for units marked as DEPLOYED in metastorage but missing on disk, and resets them to UPLOADING to trigger on-demand
     *     deployment from another node.</li>
     * </ol>
     */
    CompletableFuture<Void> syncDeployedUnits() {
        StaticUnits unitsOnDisk = collectStaticUnits();

        return deploymentUnitStore.getNodeStatuses(nodeName).thenCompose(statuses -> {
            List<String> recoveredUnits = new ArrayList<>();
            List<CompletableFuture<Boolean>> recoveredUnitFutures = new ArrayList<>();

            // Process existing statuses: filter out from static units and recover missing units
            for (UnitNodeStatus status : statuses) {
                String id = status.id();
                Version version = status.version();

                unitsOnDisk.filter(id, version);

                if (status.status() == DEPLOYED) {
                    Path unitPath = deploymentUnitsRoot.resolve(id).resolve(version.render());
                    if (Files.notExists(unitPath)) {
                        recoveredUnits.add(id + ":" + version);
                        recoveredUnitFutures.add(checkAndRecoverUnit(status));
                    }
                }
            }

            LOG.info("Start processing static deployment units {}", unitsOnDisk);
            List<CompletableFuture<Boolean>> staticUnits = new ArrayList<>();
            unitsOnDisk.forEach((id, version) -> staticUnits.add(deployStaticUnit(id, version)));

            return allOf(List.of(
                    process(
                            recoveredUnitFutures,
                            "Finished recovered units deploy {}",
                            "Units recovery failed {}",
                            recoveredUnits::toString
                    ),
                    process(
                            staticUnits,
                            "Finished static units deploy {}",
                            "Static deployment failed {}",
                            unitsOnDisk::toString
                    )
            ));
        });
    }

    private static CompletableFuture<Void> process(
            List<CompletableFuture<Boolean>> futures,
            String onFinishMessage,
            String onErrorMessage,
            Supplier<String> infoSupplier
    ) {
        return allOf(futures).handle((unused, throwable) -> {
            if (!futures.isEmpty()) {
                if (throwable == null) {
                    LOG.info(onFinishMessage, infoSupplier);
                } else {
                    LOG.error(onErrorMessage, throwable, infoSupplier);
                }
            }
            return null;
        });
    }

    private CompletableFuture<Boolean> deployStaticUnit(String id, Version version) {
        return deploymentUnitStore.createClusterStatus(id, version, Set.of(nodeName))
                .thenCompose(status -> {
                    if (status == null) {
                        return deploymentUnitStore.getClusterStatus(id, version).thenCompose(it ->
                                deploymentUnitStore.createNodeStatus(nodeName, id, version, it.opId(), DEPLOYED)
                        );
                    } else {
                        return deploymentUnitStore.createNodeStatus(nodeName, id, version, status.opId(), DEPLOYED);
                    }
                });
    }

    private CompletableFuture<Boolean> checkAndRecoverUnit(UnitNodeStatus status) {
        String id = status.id();
        Version version = status.version();

        LOG.info("Unit {}:{} is in DEPLOYED state but files are missing, resetting to UPLOADING", id, version);

        // Delete the old status and recreate with UPLOADING state, since updateNodeStatus doesn't allow backward transitions
        return deploymentUnitStore.removeNodeStatus(nodeName, id, version, status.opId())
                .thenCompose(removed -> {
                    if (removed) {
                        return deploymentUnitStore.getClusterStatus(id, version)
                                .thenCompose(clusterStatus -> {
                                    if (clusterStatus == null) {
                                        LOG.warn("Cluster status not found for {}:{}, skipping recovery", id, version);
                                        return falseCompletedFuture();
                                    }
                                    return deploymentUnitStore.createNodeStatus(nodeName, id, version, clusterStatus.opId(),
                                            UPLOADING);
                                });
                    }
                    LOG.warn("Unit status {}:{} was not removed, skipping recovery", id, version);
                    return falseCompletedFuture();
                });
    }

    private StaticUnits collectStaticUnits() {
        StaticUnits units = new StaticUnits();
        List<Path> unitFolders = allSubdirectories(deploymentUnitsRoot);
        for (Path unitFolder : unitFolders) {
            List<Path> versions = allSubdirectories(unitFolder);
            for (Path versionFolder : versions) {
                units.register(
                        unitFolder.getFileName().toString(),
                        parseVersion(versionFolder.getFileName().toString())
                );
            }

        }
        return units;
    }

    private static List<Path> allSubdirectories(Path folder) {
        if (Files.notExists(folder)) {
            return emptyList();
        }
        try {
            List<Path> subfolders = new ArrayList<>();
            Files.walkFileTree(folder, Set.of(), 1, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (attrs.isDirectory()) {
                        subfolders.add(file);
                    }
                    return super.visitFile(file, attrs);
                }
            });
            return subfolders;
        } catch (IOException e) {
            LOG.error("Failed to collect static deployment unit folders.", e);
            throw new DeploymentUnitReadException(e);
        }
    }

    private static class StaticUnits {
        private final Map<String, Set<Version>> units = new HashMap<>();

        void filter(String id, Version version) {
            Set<Version> versions = units.get(id);
            if (versions != null) {
                versions.remove(version);
            }
        }

        void register(String id, Version version) {
            units.computeIfAbsent(id, k -> new HashSet<>()).add(version);
        }

        void forEach(BiConsumer<String, Version> consumer) {
            for (Map.Entry<String, Set<Version>> entry : units.entrySet()) {
                for (Version version : entry.getValue()) {
                    consumer.accept(entry.getKey(), version);
                }
            }
        }

        @Override
        public String toString() {
            return units.entrySet().stream()
                    .flatMap(e -> e.getValue().stream().map(version -> e.getKey() + ":" + version))
                    .collect(Collectors.joining(", "));
        }
    }
}
