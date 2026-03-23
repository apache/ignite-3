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

import static org.apache.ignite.deployment.version.Version.parseVersion;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.deployment.DeployFiles.staticDeploy;
import static org.apache.ignite.internal.deployment.UnitStatusMatchers.unitVersionStatusIs;
import static org.apache.ignite.internal.deployment.UnitStatusMatchers.versionStatuses;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.deployunit.IgniteDeployment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test suite for static unit deployment mechanism.
 */
public class ItStaticDeploymentTest extends ClusterPerClassIntegrationTest {
    private DeployFiles files;

    @BeforeEach
    public void generateDummy() {
        files = new DeployFiles(WORK_DIR);
    }

    @Override
    protected boolean needInitializeCluster() {
        return false;
    }

    @Test
    public void testStaticDeploy() throws IOException {
        DeployFile smallFile = files.smallFile();

        Path node0WorkDir = CLUSTER.nodeWorkDir(0);
        Path node1WorkDir = CLUSTER.nodeWorkDir(1);
        Path node2WorkDir = CLUSTER.nodeWorkDir(2);

        staticDeploy("unit1", parseVersion("1.0.0"), smallFile, node0WorkDir);
        staticDeploy("unit1", parseVersion("1.1.0"), smallFile, node0WorkDir);
        staticDeploy("unit1", parseVersion("1.1.0"), smallFile, node1WorkDir);

        staticDeploy("unit2", parseVersion("1.0.0"), smallFile, node0WorkDir);
        staticDeploy("unit2", parseVersion("1.0.0"), smallFile, node1WorkDir);

        staticDeploy("unit3", parseVersion("1.0.0"), smallFile, node0WorkDir);
        staticDeploy("unit3", parseVersion("1.0.0"), smallFile, node1WorkDir);
        staticDeploy("unit3", parseVersion("1.0.0"), smallFile, node2WorkDir);
        staticDeploy("unit3", parseVersion("1.0.1"), smallFile, node2WorkDir);

        CLUSTER.startAndInit(3);

        IgniteDeployment node0 = unwrapIgniteImpl(CLUSTER.node(0)).deployment();
        IgniteDeployment node1 = unwrapIgniteImpl(CLUSTER.node(1)).deployment();
        IgniteDeployment node2 = unwrapIgniteImpl(CLUSTER.node(2)).deployment();

        assertThat(node0.nodeStatusAsync("unit1", parseVersion("1.0.0")), willBe(DEPLOYED));
        assertThat(node0.nodeStatusAsync("unit1", parseVersion("1.1.0")), willBe(DEPLOYED));
        assertThat(node1.nodeStatusAsync("unit1", parseVersion("1.1.0")), willBe(DEPLOYED));

        assertThat(node0.nodeStatusAsync("unit2", parseVersion("1.0.0")), willBe(DEPLOYED));
        assertThat(node1.nodeStatusAsync("unit2", parseVersion("1.0.0")), willBe(DEPLOYED));

        assertThat(node0.nodeStatusAsync("unit3", parseVersion("1.0.0")), willBe(DEPLOYED));
        assertThat(node1.nodeStatusAsync("unit3", parseVersion("1.0.0")), willBe(DEPLOYED));
        assertThat(node2.nodeStatusAsync("unit3", parseVersion("1.0.0")), willBe(DEPLOYED));
        assertThat(node2.nodeStatusAsync("unit3", parseVersion("1.0.1")), willBe(DEPLOYED));

        await().until(() -> node0.clusterStatusesAsync("unit1"), willBe(
                versionStatuses(containsInAnyOrder(
                        unitVersionStatusIs(parseVersion("1.0.0"), DEPLOYED),
                        unitVersionStatusIs(parseVersion("1.1.0"), DEPLOYED)
                ))
        ));

        await().until(() -> node0.clusterStatusesAsync("unit2"), willBe(
                versionStatuses(contains(
                        unitVersionStatusIs(parseVersion("1.0.0"), DEPLOYED)
                ))
        ));

        await().until(() -> node0.clusterStatusesAsync("unit3"), willBe(
                versionStatuses(containsInAnyOrder(
                        unitVersionStatusIs(parseVersion("1.0.0"), DEPLOYED),
                        unitVersionStatusIs(parseVersion("1.0.1"), DEPLOYED)
                ))
        ));

        CLUSTER.shutdown();
    }
}
