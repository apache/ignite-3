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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.createZipFile;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.zip.ZipInputStream;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.deployunit.DeploymentUnit;
import org.apache.ignite.internal.deployunit.NodesToDeploy;
import org.apache.ignite.internal.deployunit.StreamDeploymentUnit;
import org.apache.ignite.internal.deployunit.UnitStatuses;
import org.apache.ignite.internal.deployunit.UnitStatuses.UnitStatusesBuilder;
import org.apache.ignite.internal.deployunit.ZipDeploymentUnit;

class DeployFiles {
    private static final int BASE_REPLICA_TIMEOUT = 30;

    private static final long SMALL_IN_BYTES = 1024L;

    private static final long MEDIUM_IN_BYTES = 1024L * 1024L;

    private static final long BIG_IN_BYTES = 100 * 1024L * 1024L;

    private final Path workDir;

    private DeployFile smallFile;

    private DeployFile mediumFile;

    private DeployFile bigFile;

    private DeployFile flatZipFile;

    private DeployFile treeZipFile;

    // TODO https://issues.apache.org/jira/browse/IGNITE-20204
    DeployFiles(Path workDir) {
        this.workDir = workDir;
    }

    private static DeployFile create(Path path, long size, int replicaTimeout) {
        try {
            return new DeployFile(path, false, size, replicaTimeout);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private DeployFile createZip(Path path, Map<String, Long> content, int replicaTimeout) {
        try {
            createZipFile(content, path);
            return new DeployFile(path, true, content.values().stream().mapToLong(l -> l).sum(), replicaTimeout);
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

    DeployFile flatZipFile() {
        if (flatZipFile == null) {
            flatZipFile = createZip(
                    workDir.resolve("flat.zip"),
                    Map.of(
                            "a1", SMALL_IN_BYTES,
                            "a2", SMALL_IN_BYTES,
                            "a3", SMALL_IN_BYTES,
                            "a4", SMALL_IN_BYTES
                    ),
                    BASE_REPLICA_TIMEOUT
            );
        }
        return flatZipFile;
    }

    DeployFile treeZipFile() {
        if (treeZipFile == null) {
            treeZipFile = createZip(
                    workDir.resolve("tree.zip"),
                    Map.of(
                            "a1/a2", SMALL_IN_BYTES,
                            "b1", SMALL_IN_BYTES,
                            "c1/c2/c3/c4", MEDIUM_IN_BYTES,
                            "d1/d2", SMALL_IN_BYTES,
                            "d1/a2", SMALL_IN_BYTES
                    ),
                    BASE_REPLICA_TIMEOUT * 3
            );
        }
        return treeZipFile;
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
        DeploymentUnit deploymentUnit = fromFiles(files);
        CompletableFuture<Boolean> deploy = entryNode.deployment()
                .deployAsync(id, version, force, deploymentUnit, nodesToDeploy)
                .whenComplete((res, err) -> {
                    try {
                        deploymentUnit.close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        assertThat(deploy, willBe(true));

        Unit unit = new Unit(entryNode, id, version, files);

        for (DeployFile file : files) {
            unit.verify(file, entryNode);
            unit.verifyByRest(entryNode);
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

    public Unit deployAndVerifyFlatZip(String id, Version version, IgniteImpl entryNode) {
        return deployAndVerify(id, version, flatZipFile(), entryNode);
    }

    public Unit deployAndVerifyTreeZip(String id, Version version, IgniteImpl entryNode) {
        return deployAndVerify(id, version, treeZipFile(), entryNode);
    }

    public static UnitStatuses buildStatus(String id, Unit... units) {
        UnitStatusesBuilder builder = UnitStatuses.builder(id);
        for (Unit unit : units) {
            builder.append(unit.version(), DEPLOYED);
        }

        return builder.build();
    }

    public static void staticDeploy(String id, Version version, DeployFile file, Path workDir) throws IOException {
        Path deploymentRootFolder = workDir.resolve("deployment");
        Path unitFile = deploymentRootFolder.resolve(id).resolve(version.render()).resolve(file.file().getFileName());
        Files.createDirectories(unitFile.getParent());
        Files.copy(file.file(), unitFile);
    }

    private static DeploymentUnit fromFiles(List<DeployFile> files) {
        Map<String, InputStream> map = new HashMap<>();
        ZipInputStream zis = null;
        try {
            for (DeployFile file : files) {
                if (file.zip()) {
                    if (zis != null) {
                        fail("Only single zip file deploy is supported.");
                    }
                    zis = new ZipInputStream(Files.newInputStream(file.file()));
                } else {
                    map.put(file.file().getFileName().toString(), Files.newInputStream(file.file()));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (zis != null) {
            if (!map.isEmpty()) {
                fail("Mixed zip and plain files deploy is not supported.");
            }
            return new ZipDeploymentUnit(zis);
        } else {
            return new StreamDeploymentUnit(map);
        }
    }
}
