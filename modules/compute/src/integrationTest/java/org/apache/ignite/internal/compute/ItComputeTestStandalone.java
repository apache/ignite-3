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

package org.apache.ignite.internal.compute;

import static org.apache.ignite.internal.deployunit.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.OBSOLETE;
import static org.apache.ignite.internal.deployunit.InitialDeployMode.MAJORITY;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.deployunit.NodesToDeploy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for Compute functionality in standalone Ignite node.
 */
@SuppressWarnings("resource")
class ItComputeTestStandalone extends ItComputeBaseTest {
    private final DeploymentUnit unit = new DeploymentUnit("jobs", Version.parseVersion("1.0.0"));

    private final List<DeploymentUnit> units = List.of(unit);

    @BeforeEach
    void deploy() throws IOException {
        deployJar(node(0), unit.name(), unit.version(), "ignite-integration-test-jobs-1.0-SNAPSHOT.jar");
    }

    @AfterEach
    void undeploy() {
        IgniteImpl entryNode = node(0);

        try {
            entryNode.deployment().undeployAsync(unit.name(), unit.version()).join();
        } catch (Exception ignored) {
            // ignored
        }
        await().until(
                () -> entryNode.deployment().clusterStatusAsync(unit.name(), unit.version()),
                willBe(nullValue())
        );
    }

    @Override
    protected List<DeploymentUnit> units() {
        return units;
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19623")
    @Override
    void executesFailingJobOnRemoteNodes() {
        super.executesFailingJobOnRemoteNodes();
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19623")
    @Override
    void executesFailingJobOnRemoteNodesAsync() {
        super.executesFailingJobOnRemoteNodesAsync();
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19623")
    @Override
    void broadcastsFailingJob() throws Exception {
        super.broadcastsFailingJob();
    }

    @Test
    void executesJobWithNonExistingUnit() {
        IgniteImpl entryNode = node(0);

        List<DeploymentUnit> nonExistingUnits = List.of(new DeploymentUnit("non-existing", "1.0.0"));
        CompletableFuture<String> result = entryNode.compute().executeAsync(
                Set.of(entryNode.node()),
                JobDescriptor.builder(concatJobClassName()).units(nonExistingUnits).build(),
                "a", 42);

        CompletionException ex0 = assertThrows(CompletionException.class, result::join);

        assertComputeException(
                ex0,
                ClassNotFoundException.class,
                "org.apache.ignite.internal.compute.ConcatJob. Deployment unit non-existing:1.0.0 doesn't exist"
        );
    }

    @Test
    void executesJobWithLatestUnitVersion() throws IOException {
        List<DeploymentUnit> jobUnits = List.of(new DeploymentUnit("latest-unit", Version.LATEST));

        IgniteImpl entryNode = node(0);

        DeploymentUnit firstVersion = new DeploymentUnit("latest-unit", Version.parseVersion("1.0.0"));
        deployJar(entryNode, firstVersion.name(), firstVersion.version(), "ignite-unit-test-job1-1.0-SNAPSHOT.jar");

        JobDescriptor job = JobDescriptor.builder("org.apache.ignite.internal.compute.UnitJob").units(jobUnits).build();
        CompletableFuture<Integer> result1 = entryNode.compute().executeAsync(Set.of(entryNode.node()), job);
        assertThat(result1, willBe(1));

        DeploymentUnit secondVersion = new DeploymentUnit("latest-unit", Version.parseVersion("1.0.1"));
        deployJar(entryNode, secondVersion.name(), secondVersion.version(), "ignite-unit-test-job2-1.0-SNAPSHOT.jar");

        CompletableFuture<String> result2 = entryNode.compute().executeAsync(Set.of(entryNode.node()), job);
        assertThat(result2, willBe("Hello World!"));
    }

    @Test
    void undeployAcquiredUnit() {
        IgniteImpl entryNode = node(0);

        CompletableFuture<Void> job = entryNode.compute().executeAsync(
                Set.of(entryNode.node()),
                JobDescriptor.builder(SleepJob.class).units(units).build(),
                3L);

        assertThat(entryNode.deployment().undeployAsync(unit.name(), unit.version()), willCompleteSuccessfully());

        assertThat(entryNode.deployment().clusterStatusAsync(unit.name(), unit.version()), willBe(OBSOLETE));
        assertThat(entryNode.deployment().nodeStatusAsync(unit.name(), unit.version()), willBe(OBSOLETE));

        await().failFast("The unit must not be removed until the job is completed", () -> {
            assertThat(entryNode.deployment().clusterStatusAsync(unit.name(), unit.version()), willBe(OBSOLETE));
            assertThat(entryNode.deployment().nodeStatusAsync(unit.name(), unit.version()), willBe(OBSOLETE));
        }).until(() -> job, willCompleteSuccessfully());

        await().until(
                () -> entryNode.deployment().clusterStatusAsync(unit.name(), unit.version()),
                willBe(nullValue())
        );
    }

    @Test
    void executeJobWithObsoleteUnit() {
        IgniteImpl entryNode = node(0);
        JobDescriptor job = JobDescriptor.builder(SleepJob.class).units(units).build();

        CompletableFuture<Void> successJob = entryNode.compute().executeAsync(Set.of(entryNode.node()), job,2L);

        assertThat(entryNode.deployment().undeployAsync(unit.name(), unit.version()), willCompleteSuccessfully());

        CompletableFuture<Void> failedJob = entryNode.compute().executeAsync(Set.of(entryNode.node()), job, 2L);

        CompletionException ex0 = assertThrows(CompletionException.class, failedJob::join);
        assertComputeException(
                ex0,
                ClassNotFoundException.class,
                "Deployment unit jobs:1.0.0 can't be used: [clusterStatus = OBSOLETE, nodeStatus = OBSOLETE]"
        );

        assertThat(successJob, willCompleteSuccessfully());
    }

    private static void deployJar(IgniteImpl node, String unitId, Version unitVersion, String jarName) throws IOException {
        try (InputStream jarStream = ItComputeTestStandalone.class.getClassLoader().getResourceAsStream("units/" + jarName)) {
            CompletableFuture<Boolean> deployed = node.deployment().deployAsync(
                    unitId,
                    unitVersion,
                    new org.apache.ignite.internal.deployunit.DeploymentUnit(Map.of(jarName, jarStream)),
                    new NodesToDeploy(MAJORITY)
            );

            assertThat(deployed, willBe(true));
            await().until(() -> node.deployment().clusterStatusAsync(unitId, unitVersion), willBe(DEPLOYED));
        }
    }
}
