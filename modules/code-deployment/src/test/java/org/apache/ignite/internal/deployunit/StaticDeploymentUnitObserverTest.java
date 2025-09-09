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
import static org.apache.ignite.internal.deployment.UnitStatusMatchers.deploymentStatusIs;
import static org.apache.ignite.internal.deployment.UnitStatusMatchers.versionIs;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.UPLOADING;
import static org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager.create;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.notNullValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.deployunit.metastore.DeploymentUnitStore;
import org.apache.ignite.internal.deployunit.metastore.DeploymentUnitStoreImpl;
import org.apache.ignite.internal.deployunit.metastore.status.UnitClusterStatus;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test suite for {@link StaticUnitDeployer}.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class StaticDeploymentUnitObserverTest extends BaseIgniteAbstractTest {
    private StaticUnitDeployer observer;

    private DeploymentUnitStore deploymentUnitStore;

    @WorkDirectory
    private Path workDir;
    private StandaloneMetaStorageManager metastore;

    @BeforeEach
    public void setup() {
        metastore = create("node1");
        assertThat(metastore.startAsync(new ComponentContext()), willCompleteSuccessfully());
        deploymentUnitStore = new DeploymentUnitStoreImpl(metastore);

        this.observer = new StaticUnitDeployer(deploymentUnitStore, "node1", workDir);
    }

    @AfterEach
    public void cleanup() {
        assertThat(metastore.stopAsync(), willCompleteSuccessfully());
    }

    @Test
    public void simpleStaticDeployTest() throws IOException {
        Files.createDirectories(workDir.resolve("unit1").resolve("1.0.0"));
        Files.createDirectories(workDir.resolve("unit1").resolve("1.0.1"));
        Files.createDirectories(workDir.resolve("unit1").resolve("1.1.1"));
        Files.createDirectories(workDir.resolve("unit2").resolve("1.0.0"));
        Files.createDirectories(workDir.resolve("unit3").resolve("1.1.0"));

        assertThat(observer.searchAndDeployStaticUnits(), willCompleteSuccessfully());

        assertThat(
                deploymentUnitStore.getNodeStatuses("node1", "unit1"),
                willBe(containsInAnyOrder(
                        versionIs(parseVersion("1.0.0")),
                        versionIs(parseVersion("1.0.1")),
                        versionIs(parseVersion("1.1.1"))
                ))
        );

        assertThat(
                deploymentUnitStore.getNodeStatuses("node1", "unit2"),
                willBe(contains(versionIs(parseVersion("1.0.0"))))
        );

        assertThat(
                deploymentUnitStore.getNodeStatuses("node1", "unit3"),
                willBe(contains(versionIs(parseVersion("1.1.0"))))
        );
    }

    @Test
    public void skipAlreadyDeployedTest() throws Exception {
        Files.createDirectories(workDir.resolve("unit1").resolve("1.0.0"));
        CompletableFuture<@Nullable UnitClusterStatus> clusterStatus = deploymentUnitStore.createClusterStatus(
                "unit1",
                parseVersion("1.0.0"),
                Set.of("node1")
        );

        assertThat(clusterStatus, willBe(notNullValue()));
        deploymentUnitStore.createNodeStatus("node1", "unit1", parseVersion("1.0.0"), clusterStatus.get().opId(), UPLOADING);

        Files.createDirectories(workDir.resolve("unit1").resolve("1.0.1"));
        Files.createDirectories(workDir.resolve("unit2").resolve("1.0.0"));

        assertThat(observer.searchAndDeployStaticUnits(), willCompleteSuccessfully());

        assertThat(
                deploymentUnitStore.getNodeStatuses("node1", "unit1"),
                willBe(containsInAnyOrder(versionIs(parseVersion("1.0.0")), versionIs(parseVersion("1.0.1"))))
        );

        // Due to static deploy process should be skipped
        assertThat(deploymentUnitStore.getClusterStatus("unit1", parseVersion("1.0.0")), willBe(deploymentStatusIs(UPLOADING)));
        assertThat(deploymentUnitStore.getNodeStatus("node1", "unit1", parseVersion("1.0.0")), willBe(deploymentStatusIs(UPLOADING)));
    }
}
