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

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.UPLOADING;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.deployunit.DeploymentInfo;
import org.apache.ignite.internal.deployunit.DeploymentUnit;
import org.apache.ignite.internal.deployunit.IgniteDeployment;
import org.apache.ignite.internal.deployunit.UnitStatus;
import org.apache.ignite.internal.deployunit.UnitStatus.UnitStatusBuilder;
import org.apache.ignite.internal.deployunit.configuration.DeploymentConfiguration;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.apache.ignite.internal.deployunit.version.Version;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link IgniteDeployment}.
 */
public class ItDeploymentUnitTest extends ClusterPerTestIntegrationTest {
    private static final int BASE_REPLICA_TIMEOUT = 30;

    private static final long SMALL_IN_BYTES = 1024L;

    private static final long MEDIUM_IN_BYTES = 1024L * 1024L;

    private static final long BIG_IN_BYTES = 1024L * 1024L * 1024L;

    private DeployFile smallFile;

    private DeployFile mediumFile;

    private DeployFile bigFile;

    private List<DeployFile> allFiles;

    @BeforeEach
    public void generateDummy() throws IOException {
        smallFile = create("small.txt", SMALL_IN_BYTES, BASE_REPLICA_TIMEOUT);
        mediumFile = create("medium.txt", MEDIUM_IN_BYTES, BASE_REPLICA_TIMEOUT * 2);
        // TODO https://issues.apache.org/jira/browse/IGNITE-19009
        // bigFile = create("big.txt", BIG_IN_BYTES, BASE_REPLICA_TIMEOUT * 3);
        allFiles = List.of(smallFile, mediumFile);
    }

    private DeployFile create(String name, long size, int replicaTimeout) throws IOException {
        DeployFile deployFile = new DeployFile(workDir.resolve(name), size, replicaTimeout);
        deployFile.ensureExists();
        return deployFile;
    }

    @Test
    public void testDeploy() {
        String id = "test";
        Unit unit = deployAndVerifySmall(id, Version.parseVersion("1.1.0"), 1);

        IgniteImpl cmg = cluster.node(0);
        waitUnitReplica(cmg, unit);

        UnitStatus status = buildStatus(id, unit);

        await().timeout(2, SECONDS)
                .pollDelay(500, MILLISECONDS)
                .until(() -> node(2).deployment().unitsAsync(), willBe(Collections.singletonList(status)));
    }

    @Test
    public void deployDirectory() {
        String id = "test";
        Unit unit = deployAndVerify(id, Version.parseVersion("1.1.0"), false, allFiles, 1);

        IgniteImpl cmg = cluster.node(0);
        waitUnitReplica(cmg, unit);

        UnitStatus status = buildStatus(id, unit);

        await().timeout(2, SECONDS)
                .pollDelay(500, MILLISECONDS)
                .until(() -> node(2).deployment().unitsAsync(), willBe(Collections.singletonList(status)));
    }

    @Test
    public void testDeployUndeploy() {
        Unit unit = deployAndVerifySmall("test", Version.parseVersion("1.1.0"), 1);

        IgniteImpl cmg = cluster.node(0);
        waitUnitReplica(cmg, unit);

        unit.undeploy();
        waitUnitClean(cmg, unit);

        CompletableFuture<List<UnitStatus>> list = node(2).deployment().unitsAsync();
        assertThat(list, willBe(Collections.emptyList()));
    }

    @Test
    public void testDeployTwoUnits() {
        String id = "test";
        Unit unit1 = deployAndVerifySmall(id, Version.parseVersion("1.1.0"), 1);
        Unit unit2 = deployAndVerifySmall(id, Version.parseVersion("1.1.1"), 2);

        IgniteImpl cmg = cluster.node(0);
        waitUnitReplica(cmg, unit1);
        waitUnitReplica(cmg, unit2);

        UnitStatus status = buildStatus(id, unit1, unit2);

        await().timeout(2, SECONDS)
                .pollDelay(100, MILLISECONDS)
                .until(() -> node(2).deployment().statusAsync(id), willBe(status));

        CompletableFuture<List<Version>> versions = node(2).deployment().versionsAsync(unit1.id);
        assertThat(versions, willBe(List.of(unit1.version, unit2.version)));
    }

    @Test
    public void testDeployTwoUnitsAndUndeployOne() {
        String id = "test";
        Unit unit1 = deployAndVerifySmall(id, Version.parseVersion("1.1.0"), 1);
        Unit unit2 = deployAndVerifySmall(id, Version.parseVersion("1.1.1"), 2);

        IgniteImpl cmg = cluster.node(0);
        waitUnitReplica(cmg, unit1);
        waitUnitReplica(cmg, unit2);

        UnitStatus status = buildStatus(id, unit1, unit2);

        await().timeout(2, SECONDS)
                .pollDelay(500, MILLISECONDS)
                .until(() -> node(2).deployment().statusAsync(id), willBe(status));

        unit2.undeploy();
        CompletableFuture<List<Version>> newVersions = node(2).deployment().versionsAsync(unit1.id);
        assertThat(newVersions, willBe(List.of(unit1.version)));
    }

    @Test
    public void testDeploymentStatus() {
        String id = "test";
        Version version = Version.parseVersion("1.1.0");
        Unit unit = deployAndVerifyMedium(id, version, 1);

        CompletableFuture<UnitStatus> status = node(2).deployment().statusAsync(id);
        assertThat(status.thenApply(status1 -> status1.status(version)), willBe(UPLOADING));

        IgniteImpl cmg = cluster.node(0);
        waitUnitReplica(cmg, unit);

        await().timeout(2, SECONDS)
                .pollDelay(300, MILLISECONDS)
                .until(() -> node(2).deployment().statusAsync(id)
                        .thenApply(status1 -> status1.status(version)), willBe(DEPLOYED));

        assertThat(unit.undeployAsync(), willSucceedFast());

        waitUnitClean(unit.deployedNode, unit);
        waitUnitClean(cmg, unit);

        assertThat(node(2).deployment().statusAsync(id)
                .thenApply(status1 -> status1.status(version)), willThrowFast(DeploymentUnitNotFoundException.class));
    }

    @Test
    public void testFindByConsistentId() {
        String id = "test";
        String version = "1.1.0";
        Unit unit = deployAndVerifyMedium(id, Version.parseVersion(version), 1);

        IgniteImpl cmg = cluster.node(0);
        waitUnitReplica(cmg, unit);

        IgniteImpl node = unit.deployedNode;

        CompletableFuture<List<UnitStatus>> nodes = node.deployment().findUnitByConsistentIdAsync(node.name());
        assertThat(nodes, willBe(Collections.singletonList(
                        UnitStatus.builder(id)
                                .append(unit.version,
                                        DeploymentInfo.builder()
                                                .status(DEPLOYED)
                                                .addConsistentId(unit.deployedNode.name())
                                                .addConsistentId(cmg.name())
                                                .build()
                                ).build()
                        )
                )
        );

        nodes = node.deployment().findUnitByConsistentIdAsync(cmg.name());
        assertThat(nodes, willBe(Collections.singletonList(
                        UnitStatus.builder(id)
                                .append(unit.version,
                                        DeploymentInfo.builder()
                                                .status(DEPLOYED)
                                                .addConsistentId(unit.deployedNode.name())
                                                .addConsistentId(cmg.name())
                                                .build()
                                ).build()
                )
        ));

        nodes = node.deployment().findUnitByConsistentIdAsync("not-existed-node");
        assertThat(nodes, willBe(Collections.emptyList()));
    }

    @Test
    public void testRedeploy() {
        String id = "test";
        String version = "1.1.0";
        Unit smallUnit = deployAndVerify(id, Version.parseVersion(version), smallFile, 1);

        IgniteImpl cmg = cluster.node(0);
        waitUnitReplica(cmg, smallUnit);

        Unit mediumUnit = deployAndVerify(id, Version.parseVersion(version), true, mediumFile, 1);
        waitUnitReplica(cmg, mediumUnit);

        waitUnitClean(smallUnit.deployedNode, smallUnit);
        waitUnitClean(cmg, smallUnit);
    }

    private UnitStatus buildStatus(String id, Unit... units) {
        IgniteImpl cmg = cluster.node(0);

        UnitStatusBuilder builder = UnitStatus.builder(id);
        for (Unit unit : units) {
            builder.append(unit.version,
                    DeploymentInfo.builder()
                            .status(DEPLOYED)
                            .addConsistentId(unit.deployedNode.name())
                            .addConsistentId(cmg.name())
                            .build()
            );
        }

        return builder.build();
    }

    private Unit deployAndVerify(String id, Version version, DeployFile file, int nodeIndex) {
        return deployAndVerify(id, version, false, file, nodeIndex);
    }

    private Unit deployAndVerify(String id, Version version, boolean force, DeployFile file, int nodeIndex) {
        return deployAndVerify(id, version, force, List.of(file), nodeIndex);
    }

    private Unit deployAndVerify(String id, Version version, boolean force, List<DeployFile> files, int nodeIndex) {
        IgniteImpl entryNode = node(nodeIndex);

        List<Path> paths = files.stream()
                .map(deployFile -> deployFile.file)
                .collect(Collectors.toList());

        CompletableFuture<Boolean> deploy = entryNode.deployment()
                .deployAsync(id, version, force, fromPaths(paths));

        assertThat(deploy, willBe(true));

        Unit unit = new Unit(entryNode, id, version, files);

        Path nodeUnitDirectory = getNodeUnitDirectory(entryNode, id, version);

        for (DeployFile file : files) {
            Path filePath = nodeUnitDirectory.resolve(file.file.getFileName());
            assertTrue(Files.exists(filePath));
        }

        return unit;
    }

    private Unit deployAndVerifySmall(String id, Version version, int nodeIndex) {
        return deployAndVerify(id, version, smallFile, nodeIndex);
    }

    private Unit deployAndVerifyMedium(String id, Version version, int nodeIndex) {
        return deployAndVerify(id, version, mediumFile, nodeIndex);
    }

    private Unit deployAndVerifyBig(String id, Version version, int nodeIndex) {
        return deployAndVerify(id, version, bigFile, nodeIndex);
    }

    private Path getNodeUnitDirectory(IgniteImpl node, String unitId, Version unitVersion) {
        String deploymentFolder = node.nodeConfiguration()
                .getConfiguration(DeploymentConfiguration.KEY)
                .deploymentLocation().value();
        Path resolve = workDir.resolve(node.name()).resolve(deploymentFolder);
        return resolve.resolve(unitId)
                .resolve(unitVersion.render());
    }

    private void waitUnitReplica(IgniteImpl ignite, Unit unit) {
        Path unitDirectory = getNodeUnitDirectory(ignite, unit.id, unit.version);

        int combinedTimeout = unit.files.stream().map(file -> file.replicaTimeout).reduce(Integer::sum).get();

        await().timeout(combinedTimeout, SECONDS)
                .pollDelay(1, SECONDS)
                .ignoreException(IOException.class)
                .until(() -> {
                    for (DeployFile file : unit.files) {
                        Path filePath = unitDirectory.resolve(file.file.getFileName());
                        if (Files.notExists(filePath) || Files.size(filePath) != file.expectedSize) {
                            return false;
                        }
                    }

                    return true;
                });
    }

    private void waitUnitClean(IgniteImpl ignite, Unit unit) {
        Path unitDirectory = getNodeUnitDirectory(ignite, unit.id, unit.version);

        int combinedTimeout = unit.files.stream().map(file -> file.replicaTimeout).reduce(Integer::sum).get();

        await().timeout(combinedTimeout, SECONDS)
                .pollDelay(2, SECONDS)
                .until(() -> {
                    for (DeployFile file : unit.files) {
                        Path filePath = unitDirectory.resolve(file.file.getFileName());
                        if (Files.exists(filePath)) {
                            return false;
                        }
                    }

                    return true;
                });
    }

    class Unit {
        private final IgniteImpl deployedNode;

        private final String id;

        private final Version version;

        private final List<DeployFile> files;

        Unit(IgniteImpl deployedNode, String id, Version version, List<DeployFile> files) {
            this.deployedNode = deployedNode;
            this.id = id;
            this.version = version;
            this.files = files;
        }

        CompletableFuture<Void> undeployAsync() {
            return deployedNode.deployment().undeployAsync(id, version);
        }

        void undeploy() {
            deployedNode.deployment().undeployAsync(id, version);
            waitUnitClean(deployedNode, this);
        }
    }

    private static class DeployFile {
        private final Path file;

        private final long expectedSize;

        private final int replicaTimeout;

        private DeployFile(Path file, long expectedSize, int replicaTimeout) {
            this.file = file;
            this.expectedSize = expectedSize;
            this.replicaTimeout = replicaTimeout;
        }

        public void ensureExists() throws IOException {
            ensureFile(file, expectedSize);
        }

        private static void ensureFile(Path path, long size) throws IOException {
            if (!Files.exists(path)) {
                try (SeekableByteChannel channel = Files.newByteChannel(path, WRITE, CREATE)) {
                    channel.position(size - 4);

                    ByteBuffer buf = ByteBuffer.allocate(4).putInt(2);
                    buf.rewind();
                    channel.write(buf);
                }
            }
        }
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
        return () -> map;
    }
}
