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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.deployment.DeployFiles.buildStatus;
import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.UPLOADING;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.deployunit.IgniteDeployment;
import org.apache.ignite.internal.deployunit.InitialDeployMode;
import org.apache.ignite.internal.deployunit.UnitStatuses;
import org.apache.ignite.internal.rest.api.deployment.DeploymentStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link IgniteDeployment}.
 */
public class ItDeploymentUnitTest extends ClusterPerTestIntegrationTest {
    private DeployFiles files;


    @BeforeEach
    public void generateDummy() {
        files = new DeployFiles(workDir);
    }

    @Test
    public void testDeploy() {
        String id = "test";
        Unit unit = files.deployAndVerifySmall(id, Version.parseVersion("1.1.0"), cluster.node(1));

        IgniteImpl cmg = cluster.node(0);
        unit.waitUnitReplica(cmg);

        UnitStatuses status = buildStatus(id, unit);

        await().timeout(2, SECONDS)
                .pollDelay(500, MILLISECONDS)
                .until(() -> node(2).deployment().clusterStatusesAsync(), willBe(Collections.singletonList(status)));
    }

    @Test
    public void deployDirectory() {
        String id = "test";
        Unit unit = files.deployAndVerify(id,
                Version.parseVersion("1.1.0"),
                false,
                List.of(files.smallFile(), files.mediumFile()),
                cluster.node(1)
        );

        IgniteImpl cmg = cluster.node(0);
        unit.waitUnitReplica(cmg);

        UnitStatuses status = buildStatus(id, unit);

        await().timeout(2, SECONDS)
                .pollDelay(500, MILLISECONDS)
                .until(() -> node(2).deployment().clusterStatusesAsync(), willBe(Collections.singletonList(status)));
    }

    @Test
    public void testDeployUndeploy() {
        Unit unit = files.deployAndVerifySmall("test", Version.parseVersion("1.1.0"), cluster.node(1));

        IgniteImpl cmg = cluster.node(0);
        unit.waitUnitReplica(cmg);

        unit.undeploy();
        unit.waitUnitClean(cmg);

        CompletableFuture<List<UnitStatuses>> list = node(2).deployment().clusterStatusesAsync();
        assertThat(list, willBe(Collections.emptyList()));
    }

    @Test
    public void testDeployTwoUnits() {
        String id = "test";
        Unit unit1 = files.deployAndVerifySmall(id, Version.parseVersion("1.1.0"), cluster.node(1));
        Unit unit2 = files.deployAndVerifySmall(id, Version.parseVersion("1.1.1"), cluster.node(2));

        IgniteImpl cmg = cluster.node(0);
        unit1.waitUnitReplica(cmg);
        unit2.waitUnitReplica(cmg);

        UnitStatuses status = buildStatus(id, unit1, unit2);

        await().timeout(2, SECONDS)
                .pollDelay(100, MILLISECONDS)
                .until(() -> node(2).deployment().clusterStatusesAsync(id), willBe(status));

        CompletableFuture<List<Version>> versions = node(2).deployment().versionsAsync(unit1.id());
        assertThat(versions, willBe(List.of(unit1.version(), unit2.version())));
    }

    @Test
    public void testDeployTwoUnitsAndUndeployOne() {
        String id = "test";
        Unit unit1 = files.deployAndVerifySmall(id, Version.parseVersion("1.1.0"), cluster.node(1));
        Unit unit2 = files.deployAndVerifySmall(id, Version.parseVersion("1.1.1"), cluster.node(2));

        IgniteImpl cmg = cluster.node(0);
        unit1.waitUnitReplica(cmg);
        unit2.waitUnitReplica(cmg);

        UnitStatuses status = buildStatus(id, unit1, unit2);

        await().timeout(2, SECONDS)
                .pollDelay(500, MILLISECONDS)
                .until(() -> node(2).deployment().clusterStatusesAsync(id), willBe(status));

        unit2.undeploy();
        CompletableFuture<List<Version>> newVersions = node(2).deployment().versionsAsync(unit1.id());
        assertThat(newVersions, willBe(List.of(unit1.version())));
    }

    @Test
    public void testDeploymentStatus() {
        String id = "test";
        Version version = Version.parseVersion("1.1.0");
        Unit unit = files.deployAndVerifyMedium(id, version, cluster.node(1));

        CompletableFuture<DeploymentStatus> status = node(2).deployment().clusterStatusAsync(id, version);
        assertThat(status, willBe(UPLOADING));

        IgniteImpl cmg = cluster.node(0);
        unit.waitUnitReplica(cmg);

        await().timeout(2, SECONDS)
                .pollDelay(300, MILLISECONDS)
                .until(() -> node(2).deployment().clusterStatusAsync(id, version), willBe(DEPLOYED));

        assertThat(unit.undeployAsync(), willSucceedFast());

        unit.waitUnitClean(unit.deployedNode());
        unit.waitUnitClean(cmg);

        assertThat(node(2).deployment().clusterStatusAsync(id, version), willBe(nullValue()));
    }

    @Test
    public void testRedeploy() {
        String id = "test";
        String version = "1.1.0";
        Unit smallUnit = files.deployAndVerifySmall(id, Version.parseVersion(version), cluster.node(1));

        IgniteImpl cmg = cluster.node(0);
        smallUnit.waitUnitReplica(cmg);

        Unit mediumUnit = files.deployAndVerify(id, Version.parseVersion(version), true, List.of(files.mediumFile()), cluster.node(1));
        mediumUnit.waitUnitReplica(cmg);

        smallUnit.waitUnitClean(smallUnit.deployedNode());
        smallUnit.waitUnitClean(cmg);
    }

    @Test
    public void testOnDemandDeploy() {
        String id = "test";
        Version version = Version.parseVersion("1.1.0");
        Unit smallUnit = files.deployAndVerifySmall(id, version, cluster.node(1));

        IgniteImpl cmg = cluster.node(0);
        smallUnit.waitUnitReplica(cmg);

        IgniteImpl onDemandDeployNode = cluster.node(2);
        CompletableFuture<Boolean> onDemandDeploy = onDemandDeployNode.deployment().onDemandDeploy(id, version);

        assertThat(onDemandDeploy, willBe(true));
        smallUnit.waitUnitReplica(onDemandDeployNode);
    }

    @Test
    public void testOnDemandDeployToDeployedNode() {
        String id = "test";
        Version version = Version.parseVersion("1.1.0");
        Unit smallUnit = files.deployAndVerifySmall(id, version, cluster.node(1));

        IgniteImpl cmg = cluster.node(0);
        smallUnit.waitUnitReplica(cmg);

        IgniteImpl onDemandDeployNode = cluster.node(1);
        CompletableFuture<Boolean> onDemandDeploy = onDemandDeployNode.deployment().onDemandDeploy(id, version);

        assertThat(onDemandDeploy, willBe(true));
        smallUnit.waitUnitReplica(onDemandDeployNode);
    }

    @Test
    public void testDeployToCmg() {
        String id = "test";
        Version version = Version.parseVersion("1.1.0");
        Unit smallUnit = files.deployAndVerifySmall(id, version, cluster.node(0));

        await().untilAsserted(() -> {
            CompletableFuture<List<UnitStatuses>> list = node(0).deployment().clusterStatusesAsync();
            assertThat(list, willBe(List.of(UnitStatuses.builder(id).append(version, DEPLOYED).build())));
        });
    }

    @Test
    public void testDeployToSpecificNode() {
        String id = "test";
        Version version = Version.parseVersion("1.1.0");
        Unit smallUnit = files.deployAndVerify(
                id, version, false, List.of(files.smallFile()),
                null, List.of(node(1).name()),
                node(0)
        );

        smallUnit.waitUnitReplica(node(1));

        await().untilAsserted(() -> {
            CompletableFuture<List<UnitStatuses>> list = node(0).deployment().clusterStatusesAsync();
            assertThat(list, willBe(List.of(UnitStatuses.builder(id).append(version, DEPLOYED).build())));
        });
    }

    @Test
    public void testDeployToAll() {
        String id = "test";
        Version version = Version.parseVersion("1.1.0");
        Unit smallUnit = files.deployAndVerify(
                id, version, false, List.of(files.smallFile()),
                InitialDeployMode.ALL, List.of(),
                node(0)
        );

        smallUnit.waitUnitReplica(node(1));
        smallUnit.waitUnitReplica(node(2));

        await().untilAsserted(() -> {
            CompletableFuture<List<UnitStatuses>> list = node(0).deployment().clusterStatusesAsync();
            assertThat(list, willBe(List.of(UnitStatuses.builder(id).append(version, DEPLOYED).build())));
        });
    }
}
