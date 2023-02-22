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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
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
import java.util.concurrent.TimeUnit;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.deployment.UnitStatus;
import org.apache.ignite.deployment.UnitStatus.UnitStatusBuilder;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.AbstractClusterIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.deployunit.configuration.DeploymentConfiguration;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link org.apache.ignite.deployment.IgniteDeployment}.
 */
public class ItDeploymentUnitTest extends AbstractClusterIntegrationTest {
    private static final long REPLICA_TIMEOUT = TimeUnit.SECONDS.toMillis(30);
    private static final long SIZE_IN_BYTES = 1024L;

    private Path dummyFile;

    @BeforeEach
    public void generateDummy() throws IOException {
        dummyFile = workDir.resolve("dummy.txt");

        if (!Files.exists(dummyFile)) {
            try (SeekableByteChannel channel = Files.newByteChannel(dummyFile, WRITE, CREATE)) {
                channel.position(SIZE_IN_BYTES - 4);

                ByteBuffer buf = ByteBuffer.allocate(4).putInt(2);
                buf.rewind();
                channel.write(buf);
            }
        }
    }

    @Test
    public void testDeploy() throws Exception {
        Unit unit = deployAndVerify("test", Version.parseVersion("1.1.0"), 1);

        IgniteImpl cmg = cluster.node(0);
        waitUnitReplica(cmg, unit);

        CompletableFuture<List<UnitStatus>> list = node(2).deployment().unitsAsync();
        UnitStatusBuilder builder = UnitStatus.builder(unit.id).append(unit.version, List.of(unit.deployedNode.name(), cmg.name()));
        assertThat(list, willBe(List.of(builder.build())));
    }

    @Test
    public void testDeployUndeploy() throws Exception {
        Unit unit = deployAndVerify("test", Version.parseVersion("1.1.0"), 1);
        unit.undeploy();

        CompletableFuture<List<UnitStatus>> list = node(2).deployment().unitsAsync();
        assertThat(list, willBe(Collections.emptyList()));
    }

    @Test
    public void testDeployTwoUnits() throws Exception {
        String id = "test";
        Unit unit1 = deployAndVerify(id, Version.parseVersion("1.1.0"), 1);
        Unit unit2 = deployAndVerify(id, Version.parseVersion("1.1.1"), 2);

        IgniteImpl cmg = cluster.node(0);
        waitUnitReplica(cmg, unit1);
        waitUnitReplica(cmg, unit2);

        CompletableFuture<UnitStatus> list = node(2).deployment().statusAsync(id);
        UnitStatusBuilder status = UnitStatus.builder(id)
                .append(unit1.version, List.of(unit1.deployedNode.name(), cmg.name()))
                .append(unit2.version, List.of(unit2.deployedNode.name(), cmg.name()));
        assertThat(list, willBe(status.build()));

        CompletableFuture<List<Version>> versions = node(2).deployment().versionsAsync(unit1.id);
        assertThat(versions, willBe(List.of(unit1.version, unit2.version)));
    }

    @Test
    public void testDeployTwoUnitsAndUndeployOne() throws Exception {
        Unit unit1 = deployAndVerify("test", Version.parseVersion("1.1.0"), 1);
        Unit unit2 = deployAndVerify("test", Version.parseVersion("1.1.1"), 2);

        IgniteImpl cmg = cluster.node(0);
        waitUnitReplica(cmg, unit1);
        waitUnitReplica(cmg, unit2);

        CompletableFuture<UnitStatus> list = node(2).deployment().statusAsync(unit2.id);
        UnitStatusBuilder builder = UnitStatus.builder(unit1.id)
                .append(unit1.version, List.of(unit1.deployedNode.name(), cmg.name()))
                .append(unit2.version, List.of(unit2.deployedNode.name(), cmg.name()));
        assertThat(list, willBe(builder.build()));

        unit2.undeploy();
        CompletableFuture<List<Version>> newVersions = node(2).deployment().versionsAsync(unit1.id);
        assertThat(newVersions, willBe(List.of(unit1.version)));
    }

    private Unit deployAndVerify(String id, Version version, int nodeIndex) {
        IgniteImpl entryNode = node(nodeIndex);

        CompletableFuture<Boolean> deploy = entryNode.deployment()
                .deployAsync(id, version, fromPath(dummyFile));

        assertThat(deploy, willBe(true));

        Unit unit = new Unit(entryNode, id, version);
        assertTrue(Files.exists(getNodeUnitFile(unit)));

        return unit;
    }

    private Path getNodeUnitFile(Unit unit) {
        return getNodeUnitFile(unit.deployedNode, unit.id, unit.version);
    }

    private Path getNodeUnitFile(IgniteImpl node, String unitId, Version unitVersion) {
        String deploymentFolder = node.nodeConfiguration()
                .getConfiguration(DeploymentConfiguration.KEY)
                .deploymentLocation().value();
        Path resolve = workDir.resolve(node.name()).resolve(deploymentFolder);
        return resolve.resolve(unitId)
                .resolve(unitVersion.render())
                .resolve(dummyFile.getFileName());
    }

    private void waitUnitReplica(IgniteImpl ignite, Unit unit) throws InterruptedException {
        Path unitPath = getNodeUnitFile(ignite, unit.id, unit.version);
        assertTrue(IgniteTestUtils.waitForCondition(() -> {
            try {
                return Files.exists(unitPath) && Files.size(unitPath) == SIZE_IN_BYTES;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, REPLICA_TIMEOUT));
    }

    private void waitUnitClean(IgniteImpl ignite, Unit unit) throws InterruptedException {
        Path unitPath = getNodeUnitFile(ignite, unit.id, unit.version);
        assertTrue(IgniteTestUtils.waitForCondition(() -> !Files.exists(unitPath), REPLICA_TIMEOUT));
    }

    class Unit {
        private final IgniteImpl deployedNode;

        private final String id;

        private final Version version;

        Unit(IgniteImpl deployedNode, String id, Version version) {
            this.deployedNode = deployedNode;
            this.id = id;
            this.version = version;
        }

        void undeploy() throws InterruptedException {
            deployedNode.deployment().undeployAsync(id, version);
            waitUnitClean(deployedNode, this);
        }
    }

    static DeploymentUnit fromPath(Path path) {
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
