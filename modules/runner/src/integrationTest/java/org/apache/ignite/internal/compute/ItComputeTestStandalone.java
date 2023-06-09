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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.app.IgniteImpl;
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
    void setUp() throws IOException {
        deployJar(node(0), unit.name(), unit.version(), "ignite-jobs-1.0-SNAPSHOT.jar");
    }

    @Override
    protected List<DeploymentUnit> units() {
        return units;
    }

    @Override
    protected String concatJobClassName() {
        return "org.example.ConcatJob";
    }

    @Override
    protected String getNodeNameJobClassName() {
        return "org.example.GetNodeNameJob";
    }

    @Override
    protected String failingJobClassName() {
        return "org.example.FailingJob";
    }

    @Override
    protected String jobExceptionClassName() {
        return "org.example.JobException";
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
    void broadcastsFailingJob() throws Exception {
        super.broadcastsFailingJob();
    }

    @Test
    void executesJobWithNonExistingUnit() {
        IgniteImpl entryNode = node(0);

        List<DeploymentUnit> nonExistingUnits = List.of(new DeploymentUnit("non-existing", "1.0.0"));
        CompletableFuture<String> result = entryNode.compute()
                .execute(Set.of(entryNode.node()), nonExistingUnits, concatJobClassName(), "a", 42);

        assertThat(
                result,
                willThrow(
                        ClassNotFoundException.class,
                        "org.example.ConcatJob. Deployment unit non-existing:1.0.0 doesn’t exist"
                )
        );
    }

    @Test
    void executesJobWithLatestUnitVersion() throws IOException {
        List<DeploymentUnit> jobUnits = List.of(new DeploymentUnit("latest-unit", Version.LATEST));

        IgniteImpl entryNode = node(0);

        deployJar(entryNode, "latest-unit", Version.parseVersion("1.0.0"), "unit1-1.0-SNAPSHOT.jar");
        CompletableFuture<Integer> result1 = entryNode.compute()
                .execute(Set.of(entryNode.node()), jobUnits, "org.my.job.compute.unit.UnitJob");
        assertThat(result1, willBe(1));

        deployJar(entryNode, "latest-unit", Version.parseVersion("2.0.0"), "unit2-1.0-SNAPSHOT.jar");
        CompletableFuture<String> result2 = entryNode.compute()
                .execute(Set.of(entryNode.node()), jobUnits, "org.my.job.compute.unit.UnitJob");
        assertThat(result2, willBe("Hello World!"));

    }

    private static void deployJar(IgniteImpl node, String unitId, Version unitVersion, String jarName) throws IOException {
        try (InputStream jarStream = ItComputeTestStandalone.class.getClassLoader().getResourceAsStream("units/" + jarName)) {
            CompletableFuture<Boolean> deployed = node.deployment().deployAsync(
                    unitId,
                    unitVersion,
                    () -> Map.of(jarName, jarStream)
            );

            assertThat(deployed, willBe(true));
        }
    }
}
