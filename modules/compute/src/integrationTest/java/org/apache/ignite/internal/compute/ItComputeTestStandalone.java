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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.OBSOLETE;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.UPLOADING;
import static org.apache.ignite.internal.deployunit.InitialDeployMode.MAJORITY;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.will;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.deployunit.NodesToDeploy;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for Compute functionality in standalone Ignite node.
 */
@SuppressWarnings("ThrowableNotThrown")
class ItComputeTestStandalone extends ItComputeBaseTest {
    private final DeploymentUnit unit = new DeploymentUnit("jobs", Version.parseVersion("1.0.0"));

    private final List<DeploymentUnit> units = List.of(unit);

    @Override
    protected int[] cmgMetastoreNodes() {
        return new int[] { 0, 1 }; // Majority will be 0, 1
    }

    @BeforeEach
    void deploy() throws IOException {
        deployJar(node(0), unit.name(), unit.version(), "ignite-integration-test-jobs-1.0-SNAPSHOT.jar");
    }

    @AfterEach
    void undeploy() {
        IgniteImpl entryNode = unwrapIgniteImpl(node(0));

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
    void executeMultipleRemoteJobs() {
        IgniteImpl ignite = unwrapIgniteImpl(node(0));
        CompletableFuture<Set<String>> majority = ignite.clusterManagementGroupManager().majority();
        assertThat(majority, will(contains(node(0).name(), node(1).name())));

        assertThat(unwrapIgniteImpl(node(0)).deployment().nodeStatusAsync(unit.name(), unit.version()), willBe(DEPLOYED));
        assertThat(unwrapIgniteImpl(node(1)).deployment().nodeStatusAsync(unit.name(), unit.version()), willBe(oneOf(UPLOADING, DEPLOYED)));
        assertThat(unwrapIgniteImpl(node(2)).deployment().nodeStatusAsync(unit.name(), unit.version()), willBe(nullValue()));

        // Execute concurrently on majority, unit should be in the uploading or deployed state at this point
        List<CompletableFuture<String>> resultsMajority = IntStream.range(0, 3).mapToObj(i -> compute().executeAsync(
                JobTarget.node(clusterNode(1)),
                JobDescriptor.builder(toStringJobClass()).units(units()).build(),
                42
        )).collect(Collectors.toList());

        // Execute concurrently on non-majority, unit is missing, will trigger on-demand deploy
        List<CompletableFuture<String>> resultsMissing = IntStream.range(0, 3).mapToObj(i -> compute().executeAsync(
                JobTarget.node(clusterNode(2)),
                JobDescriptor.builder(toStringJobClass()).units(units()).build(),
                42
        )).collect(Collectors.toList());

        assertThat(resultsMajority, everyItem(will(equalTo("42"))));
        assertThat(resultsMissing, everyItem(will(equalTo("42"))));
    }

    @Test
    void executesJobWithNonExistingUnit() {
        Ignite entryNode = node(0);

        List<DeploymentUnit> nonExistingUnits = List.of(new DeploymentUnit("non-existing", "1.0.0"));
        CompletableFuture<String> result = entryNode.compute().executeAsync(
                JobTarget.node(clusterNode(entryNode)),
                JobDescriptor.builder(toStringJobClass()).units(nonExistingUnits).build(),
                null
        );

        assertThat(result, willThrow(computeJobFailedException(
                ClassNotFoundException.class.getName(),
                "org.apache.ignite.internal.compute.ToStringJob. Deployment unit non-existing:1.0.0 doesn't exist"
        )));
    }

    @Test
    void executeJobWithoutUnit() throws IOException {
        IgniteImpl entryNode = unwrapIgniteImpl(node(0));

        deployJar(entryNode, "unit", Version.parseVersion("1.0.0"), "ignite-unit-test-job1-1.0-SNAPSHOT.jar");

        IgniteTestUtils.assertThrows(
                ComputeException.class,
                () -> {
                    entryNode.compute().execute(
                            JobTarget.node(clusterNode(entryNode)),
                            JobDescriptor.builder("org.apache.ignite.internal.compute.UnitJob").build(),
                            null
                    );
                },
                "Cannot load job class by name 'org.apache.ignite.internal.compute.UnitJob'."
                        + " Deployment units list is empty.");
    }

    @Test
    void executesJobWithLatestUnitVersion() throws IOException {
        List<DeploymentUnit> jobUnits = List.of(new DeploymentUnit("latest-unit", Version.LATEST));

        Ignite entryNode = node(0);

        DeploymentUnit firstVersion = new DeploymentUnit("latest-unit", Version.parseVersion("1.0.0"));
        deployJar(entryNode, firstVersion.name(), firstVersion.version(), "ignite-unit-test-job1-1.0-SNAPSHOT.jar");

        JobDescriptor job = JobDescriptor.builder("org.apache.ignite.internal.compute.UnitJob").units(jobUnits).build();
        CompletableFuture<Integer> result1 = entryNode.compute().executeAsync(JobTarget.node(clusterNode(entryNode)), job, null);
        assertThat(result1, willBe(1));

        DeploymentUnit secondVersion = new DeploymentUnit("latest-unit", Version.parseVersion("1.0.1"));
        deployJar(entryNode, secondVersion.name(), secondVersion.version(), "ignite-unit-test-job2-1.0-SNAPSHOT.jar");

        CompletableFuture<String> result2 = entryNode.compute().executeAsync(JobTarget.node(clusterNode(entryNode)), job, null);
        assertThat(result2, willBe("Hello World!"));
    }

    @Test
    void undeployAcquiredUnit() {
        IgniteImpl entryNode = unwrapIgniteImpl(node(0));

        CompletableFuture<Void> job = entryNode.compute().executeAsync(
                JobTarget.node(clusterNode(entryNode)),
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
        IgniteImpl entryNode = unwrapIgniteImpl(node(0));
        JobDescriptor<Long, Void> job = JobDescriptor.builder(SleepJob.class).units(units).build();

        CompletableFuture<Void> successJob = entryNode.compute().executeAsync(JobTarget.node(clusterNode(entryNode)), job, 2L);

        assertThat(entryNode.deployment().undeployAsync(unit.name(), unit.version()), willCompleteSuccessfully());

        CompletableFuture<Void> failedJob = entryNode.compute().executeAsync(JobTarget.node(clusterNode(entryNode)), job, 2L);

        assertThat(failedJob, willThrow(computeJobFailedException(
                ClassNotFoundException.class.getName(),
                "Deployment unit jobs:1.0.0 can't be used: [clusterStatus = OBSOLETE, nodeStatus = OBSOLETE]"
        )));

        assertThat(successJob, willCompleteSuccessfully());
    }

    private static void deployJar(Ignite node, String unitId, Version unitVersion, String jarName) throws IOException {
        IgniteImpl igniteImpl = unwrapIgniteImpl(node);

        try (InputStream jarStream = ItComputeTestStandalone.class.getClassLoader().getResourceAsStream("units/" + jarName)) {
            CompletableFuture<Boolean> deployed = igniteImpl.deployment().deployAsync(
                    unitId,
                    unitVersion,
                    new org.apache.ignite.internal.deployunit.DeploymentUnit(Map.of(jarName, jarStream)),
                    new NodesToDeploy(MAJORITY)
            );

            assertThat(deployed, willBe(true));
            await().until(() -> igniteImpl.deployment().clusterStatusAsync(unitId, unitVersion), willBe(DEPLOYED));
        }
    }
}
