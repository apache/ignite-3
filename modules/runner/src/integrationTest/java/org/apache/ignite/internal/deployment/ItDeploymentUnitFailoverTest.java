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
import static org.apache.ignite.internal.deployunit.DeploymentStatus.UPLOADING;
import static org.apache.ignite.internal.deployunit.InitialDeployMode.MAJORITY;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.deployunit.IgniteDeployment;
import org.apache.ignite.internal.deployunit.NodesToDeploy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link IgniteDeployment} for recovery logic.
 */
public class ItDeploymentUnitFailoverTest extends ClusterPerTestIntegrationTest {
    private DeployFiles files;

    @BeforeEach
    public void generateDummy() {
        files = new DeployFiles(workDir);
    }

    @Override
    protected int[] cmgMetastoreNodes() {
        return new int[] { 0, 1, 2 };
    }

    @Test
    public void testDeployWithNodeStop() {
        int nodeIndex = 1;
        IgniteImpl node = node(nodeIndex);
        IgniteImpl cmgNode = node(0);

        // Deploy to majority and additional node
        Unit big = files.deployAndVerify(
                "id1",
                Version.parseVersion("1.0.0"),
                false,
                List.of(files.bigFile()),
                new NodesToDeploy(List.of(node.name())),
                cmgNode
        );

        stopNode(nodeIndex);

        big.waitUnitClean(node);
        big.waitUnitReplica(cmgNode);

        node = startNode(nodeIndex);
        big.waitUnitReplica(node);
    }

    @Test
    public void testUndeployWithNodeStop() {
        int nodeIndex = 1;
        String id = "id1";
        Version version = Version.parseVersion("1.0.0");
        Unit unit = files.deployAndVerify(
                id,
                version,
                false,
                List.of(files.smallFile()),
                new NodesToDeploy(List.of(node(nodeIndex).name())),
                node(0)
        );

        await().until(() -> node(nodeIndex).deployment().clusterStatusAsync(id, version), willBe(DEPLOYED));

        stopNode(nodeIndex);

        assertThat(unit.undeployAsync(), willCompleteSuccessfully());

        IgniteImpl cmgNode = startNode(nodeIndex);
        unit.waitUnitClean(cmgNode);
    }

    @Test
    public void restoreMajorityDuringDeploy() throws Exception {
        int leaderIndex = getLeaderIndex();
        IgniteImpl leaderNode = node(leaderIndex);

        Set<String> majority = getMajority();
        assertThat(majority.size(), is(2));
        assertThat(majority, hasItem(leaderNode.name()));

        // Since the majority size is 2, there's only one non-leader there.
        int nodeIndex = IntStream.range(0, initialNodes())
                .filter(index -> index != leaderIndex && majority.contains(node(index).name()))
                .findFirst().getAsInt();

        // Since the majority size is 2, there's only one node not in the majority.
        int newMajorityIndex = IntStream.range(0, initialNodes())
                .filter(index -> !majority.contains(node(index).name()))
                .findFirst().getAsInt();

        // Deploy to majority
        String id = "id1";
        Version version = Version.parseVersion("1.0.0");
        Unit big = files.deployAndVerify(
                id,
                version,
                false,
                List.of(files.bigFile()),
                new NodesToDeploy(MAJORITY),
                leaderNode
        );

        IgniteImpl node = node(nodeIndex);
        stopNode(nodeIndex);

        assertThat(leaderNode.deployment().clusterStatusAsync(id, version), willBe(UPLOADING));

        big.waitUnitClean(node);
        big.waitUnitReplica(leaderNode);
        big.waitUnitReplica(node(newMajorityIndex));

        await().until(() -> leaderNode.deployment().clusterStatusAsync(id, version), willBe(DEPLOYED));
    }

    @Test
    public void changeLeaderDuringDeploy() throws Exception {
        int leaderIndex = getLeaderIndex();
        IgniteImpl leaderNode = node(leaderIndex);

        Set<String> majority = getMajority();
        assertThat(majority.size(), is(2));
        assertThat(majority, hasItem(leaderNode.name()));

        // Since the majority size is 2, there's only one non-leader there.
        int nodeIndex = IntStream.range(0, initialNodes())
                .filter(index -> index != leaderIndex && majority.contains(node(index).name()))
                .findFirst().getAsInt();
        IgniteImpl node = node(nodeIndex);

        // Since the majority size is 2, there's only one node not in the majority.
        int newMajorityIndex = IntStream.range(0, initialNodes())
                .filter(index -> !majority.contains(node(index).name()))
                .findFirst().getAsInt();

        // Deploy to majority through the other node
        String id = "id1";
        Version version = Version.parseVersion("1.0.0");
        Unit big = files.deployAndVerify(
                id,
                version,
                false,
                List.of(files.bigFile()),
                new NodesToDeploy(MAJORITY),
                node
        );

        stopNode(leaderIndex);

        assertThat(node.deployment().clusterStatusAsync(id, version), willBe(UPLOADING));

        big.waitUnitClean(leaderNode);
        big.waitUnitReplica(node);
        big.waitUnitReplica(node(newMajorityIndex));

        await().until(() -> node.deployment().clusterStatusAsync(id, version), willBe(DEPLOYED));
    }

    private int getLeaderIndex() {
        for (int i = 0; i < initialNodes(); i++) {
            CompletableFuture<Boolean> leaderFut = node(i).cmgManager().isCmgLeader();
            assertThat(leaderFut, willCompleteSuccessfully());
            if (leaderFut.join()) {
                return i;
            }
        }
        return -1;
    }

    private Set<String> getMajority() throws InterruptedException, ExecutionException {
        CompletableFuture<Set<String>> majorityFut = node(0).cmgManager().majority();
        assertThat(majorityFut, willCompleteSuccessfully());
        return majorityFut.get();
    }
}
