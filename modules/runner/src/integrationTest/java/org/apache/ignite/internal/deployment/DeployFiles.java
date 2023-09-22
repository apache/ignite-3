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

package org.apache.ignite.internal.deployment;

import static org.apache.ignite.internal.deployunit.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.deployunit.DeploymentUnit;
import org.apache.ignite.internal.deployunit.NodesToDeploy;
import org.apache.ignite.internal.deployunit.UnitStatuses;
import org.apache.ignite.internal.deployunit.UnitStatuses.UnitStatusesBuilder;

class DeployFiles {
    private static final int BASE_REPLICA_TIMEOUT = 30;

    private static final long SMALL_IN_BYTES = 1024L;

    private static final long MEDIUM_IN_BYTES = 1024L * 1024L;

    private static final long BIG_IN_BYTES = 100 * 1024L * 1024L;

    private final Path workDir;

    private DeployFile smallFile;

    private DeployFile mediumFile;

    private DeployFile bigFile;

    // TODO https://issues.apache.org/jira/browse/IGNITE-20204
    DeployFiles(Path workDir) {
        this.workDir = workDir;
    }

    private static DeployFile create(Path path, long size, int replicaTimeout) {
        try {
            return new DeployFile(path, size, replicaTimeout);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    DeployFile smallFile() {
        if (smallFile == null) {
            smallFile = create(workDir.resolve("small.txt"), SMALL_IN_BYTES, BASE_REPLICA_TIMEOUT);
        }
        return smallFile;
    }

    DeployFile mediumFile() {
        if (mediumFile == null) {
            mediumFile = create(workDir.resolve("medium.txt"), MEDIUM_IN_BYTES, BASE_REPLICA_TIMEOUT * 2);
        }
        return mediumFile;
    }

    DeployFile bigFile() {
        if (bigFile == null) {
            bigFile = create(workDir.resolve("big.txt"), BIG_IN_BYTES, BASE_REPLICA_TIMEOUT * 3);
        }
        return bigFile;
    }

    private Unit deployAndVerify(String id, Version version, DeployFile file, IgniteImpl entryNode) {
        return deployAndVerify(id, version, false, file, entryNode);
    }

    private Unit deployAndVerify(String id, Version version, boolean force, DeployFile file, IgniteImpl entryNode) {
        return deployAndVerify(id, version, force, List.of(file), entryNode);
    }

    public Unit deployAndVerify(String id, Version version, boolean force, List<DeployFile> files, IgniteImpl entryNode) {
        return deployAndVerify(id, version, force, files, new NodesToDeploy(List.of()), entryNode);
    }

    public Unit deployAndVerify(
            String id,
            Version version,
            boolean force,
            List<DeployFile> files,
            NodesToDeploy nodesToDeploy,
            IgniteImpl entryNode
    ) {
        List<Path> paths = files.stream()
                .map(DeployFile::file)
                .collect(Collectors.toList());

        CompletableFuture<Boolean> deploy;

        DeploymentUnit deploymentUnit = fromPaths(paths);
        deploy = entryNode.deployment()
                .deployAsync(id, version, force, deploymentUnit, nodesToDeploy)
                .whenComplete((res, err) -> {
                    try {
                        deploymentUnit.close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        assertThat(deploy, willBe(true));

        Unit unit = new Unit(entryNode, workDir, id, version, files);

        Path nodeUnitDirectory = unit.getNodeUnitDirectory(entryNode);

        for (DeployFile file : files) {
            Path filePath = nodeUnitDirectory.resolve(file.file().getFileName());
            assertTrue(Files.exists(filePath));
        }

        return unit;
    }

    public Unit deployAndVerifySmall(String id, Version version, IgniteImpl entryNode) {
        return deployAndVerify(id, version, smallFile(), entryNode);
    }

    public Unit deployAndVerifyMedium(String id, Version version, IgniteImpl entryNode) {
        return deployAndVerify(id, version, mediumFile(), entryNode);
    }

    public Unit deployAndVerifyBig(String id, Version version, IgniteImpl entryNode) {
        return deployAndVerify(id, version, bigFile(), entryNode);
    }

    public static UnitStatuses buildStatus(String id, Unit... units) {
        UnitStatusesBuilder builder = UnitStatuses.builder(id);
        for (Unit unit : units) {
            builder.append(unit.version(), DEPLOYED);
        }

        return builder.build();
    }

    private static DeploymentUnit fromPaths(List<Path> paths) {
        Objects.requireNonNull(paths);
        Map<String, InputStream> map = new HashMap<>();
        try {
            for (Path path : paths) {
                map.put(path.getFileName().toString(), Files.newInputStream(path));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new DeploymentUnit(map);
    }
}
