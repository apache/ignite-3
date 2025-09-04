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

package org.apache.ignite.internal.deployunit;

import static org.apache.ignite.deployment.version.Version.parseVersion;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
public class StaticDeploymentUnitObserverTest {
    private StaticDeploymentUnitObserver observer;

    private StubDeploymentUnitStore deploymentUnitStore;

    @WorkDirectory
    private Path workDir;

    @BeforeEach
    public void setup() {
        deploymentUnitStore = new StubDeploymentUnitStore();
        this.observer = new StaticDeploymentUnitObserver(deploymentUnitStore, "node1", workDir);
    }

    @Test
    public void test() throws IOException {
        Files.createDirectories(workDir.resolve("unit1").resolve("1.0.0"));
        Files.createDirectories(workDir.resolve("unit1").resolve("1.0.1"));
        Files.createDirectories(workDir.resolve("unit1").resolve("1.1.1"));
        Files.createDirectories(workDir.resolve("unit2").resolve("1.0.0"));
        Files.createDirectories(workDir.resolve("unit3").resolve("1.1.0"));

        assertThat(observer.observeAndRegisterStaticUnits(), willCompleteSuccessfully());

        CompletableFuture<List<Version>> unit1Statuses = deploymentUnitStore.getNodeStatuses("node1", "unit1")
                .thenApply(statuses -> statuses.stream().map(UnitStatus::version).collect(Collectors.toList()));
        assertThat(unit1Statuses, willBe(containsInAnyOrder(parseVersion("1.0.0"), parseVersion("1.0.1"), parseVersion("1.1.1"))));

        CompletableFuture<List<Version>> unit2Statuses = deploymentUnitStore.getNodeStatuses("node1", "unit2")
                .thenApply(statuses -> statuses.stream().map(UnitStatus::version).collect(Collectors.toList()));

        assertThat(unit2Statuses, willBe(containsInAnyOrder(parseVersion("1.0.0"))));

        CompletableFuture<List<Version>> unit3Statuses = deploymentUnitStore.getNodeStatuses("node1", "unit3")
                .thenApply(statuses -> statuses.stream().map(UnitStatus::version).collect(Collectors.toList()));
        assertThat(unit3Statuses, willBe(containsInAnyOrder(parseVersion("1.1.0"))));
    }
}
