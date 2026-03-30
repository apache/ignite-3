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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.deleteIfExists;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.file.Path;
import java.util.List;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.deployunit.IgniteDeployment;
import org.apache.ignite.internal.deployunit.NodesToDeploy;
import org.apache.ignite.internal.deployunit.configuration.DeploymentExtensionConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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

    @Test
    @Disabled("IGNITE-20204")
    public void testDeployWithNodeStop() {
        int nodeIndex = 1;
        IgniteImpl node = igniteImpl(nodeIndex);
        IgniteImpl cmgNode = igniteImpl(0);

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

        node = unwrapIgniteImpl(startNode(nodeIndex));
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
                igniteImpl(0)
        );

        await().until(() -> igniteImpl(nodeIndex).deployment().clusterStatusAsync(id, version), willBe(DEPLOYED));

        stopNode(nodeIndex);

        assertThat(unit.undeployAsync(), willCompleteSuccessfully());

        IgniteImpl restartedNode = unwrapIgniteImpl(startNode(nodeIndex));
        unit.waitUnitClean(restartedNode);
    }

    @Test
    void testRecoveryAfterCleanWorkDir() {
        int nodeIndex = 1;
        IgniteImpl node = igniteImpl(nodeIndex);

        String id = "id1";
        Version version = Version.parseVersion("1.0.0");

        // Deploy a unit to the cluster including the target node
        Unit unit = files.deployAndVerify(
                id,
                version,
                false,
                List.of(files.smallFile()),
                new NodesToDeploy(List.of(node.name())),
                igniteImpl(0)
        );

        // Wait for deployment on target node
        await().until(() -> node.deployment().nodeStatusAsync(id, version), willBe(DEPLOYED));

        // Verify unit files exist on the target node
        unit.waitUnitReplica(node);

        // Get the deployment directory path before stopping the node
        Path deploymentDir = getDeploymentDirectory(node);

        // Stop the target node
        stopNode(nodeIndex);

        // Clear the deployment directory to simulate a clean restart
        deleteIfExists(deploymentDir);

        // Start the node again
        IgniteImpl restartedNode = unwrapIgniteImpl(startNode(nodeIndex));

        // Verify the unit is recovered from another node
        unit.waitUnitReplica(restartedNode);
    }

    private static Path getDeploymentDirectory(IgniteImpl ignite) {
        String deploymentFolder = ignite.nodeConfiguration()
                .getConfiguration(DeploymentExtensionConfiguration.KEY).deployment()
                .location().value();

        return ignite.workDir().resolve(deploymentFolder);
    }
}
