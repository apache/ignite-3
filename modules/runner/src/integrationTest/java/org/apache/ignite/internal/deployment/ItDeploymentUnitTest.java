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
import static org.apache.ignite.deployment.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.deployment.DeploymentStatus.REMOVING;
import static org.apache.ignite.deployment.DeploymentStatus.UPLOADING;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willFailFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.deployment.DeploymentInfo;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.deployment.UnitStatus;
import org.apache.ignite.deployment.UnitStatus.UnitStatusBuilder;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.deployunit.configuration.DeploymentConfiguration;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link org.apache.ignite.deployment.IgniteDeployment}.
 */
public class ItDeploymentUnitTest extends ClusterPerTestIntegrationTest {
    private static final int BASE_REPLICA_TIMEOUT = 30;

    private static final long SMALL_IN_BYTES = 1024L;

    private static final long MEDIUM_IN_BYTES = 1024L * 1024L;

    private static final long BIG_IN_BYTES = 1024L * 1024L * 1024L;

    private DeployFile smallFile;

    private DeployFile mediumFile;

    private DeployFile bigFile;

    @BeforeEach
    public void generateDummy() throws IOException {
        smallFile = create("small.txt", SMALL_IN_BYTES, BASE_REPLICA_TIMEOUT);
        mediumFile = create("medium.txt", MEDIUM_IN_BYTES, BASE_REPLICA_TIMEOUT * 2);
        bigFile = create("big.txt", BIG_IN_BYTES, BASE_REPLICA_TIMEOUT * 3);
    }

    private DeployFile create(String name, long size, int replicaTimeout) throws IOException {
        DeployFile deployFile = new DeployFile(workDir.resolve(name), size, replicaTimeout);
        deployFile.ensureExist();
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
    public void testDeployUndeploy() {
        Unit unit = deployAndVerifySmall("test", Version.parseVersion("1.1.0"), 1);
        unit.undeploy();

        IgniteImpl cmg = cluster.node(0);
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

        assertThat(node(1).deployment().statusAsync(id)
                .thenApply(status1 -> status1.status(version)), willBe(REMOVING));

        waitUnitClean(unit.deployedNode, unit);
        waitUnitClean(cmg, unit);

        assertThat(node(2).deployment().statusAsync(id)
                .thenApply(status1 -> status1.status(version)), willFailFast(DeploymentUnitNotFoundException.class));
    }

    @Test
    public void testFindByConsistentId() {
        String id = "test";
        String version = "1.1.0";
        Unit unit = deployAndVerify(id, Version.parseVersion(version), mediumFile, 1);

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
        IgniteImpl entryNode = node(nodeIndex);

        CompletableFuture<Boolean> deploy = entryNode.deployment()
                .deployAsync(id, version, fromPath(file.file));

        assertThat(deploy, willBe(true));

        Unit unit = new Unit(entryNode, id, version, file);
        Path nodeUnitFile = getNodeUnitFile(unit);
        assertTrue(Files.exists(nodeUnitFile));

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

    private Path getNodeUnitFile(Unit unit) {
        return getNodeUnitFile(unit.deployedNode, unit.id, unit.version, unit.file);
    }

    private Path getNodeUnitFile(IgniteImpl node, String unitId, Version unitVersion, DeployFile file) {
        String deploymentFolder = node.nodeConfiguration()
                .getConfiguration(DeploymentConfiguration.KEY)
                .deploymentLocation().value();
        Path resolve = workDir.resolve(node.name()).resolve(deploymentFolder);
        return resolve.resolve(unitId)
                .resolve(unitVersion.render())
                .resolve(file.file.getFileName());
    }

    private void waitUnitReplica(IgniteImpl ignite, Unit unit) {
        Path unitPath = getNodeUnitFile(ignite, unit.id, unit.version, unit.file);

        await().timeout(unit.file.replicaTimeout, SECONDS)
                .pollDelay(1, SECONDS)
                .ignoreException(IOException.class)
                .until(() -> Files.exists(unitPath) && Files.size(unitPath) == unit.file.expectedSize);
    }

    private void waitUnitClean(IgniteImpl ignite, Unit unit) {
        Path unitPath = getNodeUnitFile(ignite, unit.id, unit.version, unit.file);

        await().timeout(unit.file.replicaTimeout, SECONDS)
                .pollDelay(2, SECONDS)
                .until(() -> !Files.exists(unitPath));
    }

    class Unit {
        private final IgniteImpl deployedNode;

        private final String id;

        private final Version version;

        private final DeployFile file;

        Unit(IgniteImpl deployedNode, String id, Version version, DeployFile file) {
            this.deployedNode = deployedNode;
            this.id = id;
            this.version = version;
            this.file = file;
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

        public void ensureExist() throws IOException {
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

    private static DeploymentUnit fromPath(Path path) {
        Objects.requireNonNull(path);
        return new DeploymentUnit() {

            @Override
            public String name() {
                return path.getFileName().toString();
            }

            @Override
            public InputStream content() {
                try {
                    return new FileInputStream(path.toFile());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
