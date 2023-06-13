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

import static org.apache.ignite.internal.deployunit.InitialDeployMode.MAJORITY;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.version.Version;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for Compute functionality in standalone Ignite node.
 */
@SuppressWarnings("resource")
class ItComputeTestStandalone extends ItComputeBaseTest {
    private final URL jobsResource = ItComputeTestStandalone.class.getClassLoader().getResource("units/ignite-jobs-1.0-SNAPSHOT.jar");

    private final String unitId = "jobs";

    private final Version unitVersion = Version.parseVersion("1.0.0");

    private final List<DeploymentUnit> units = List.of(new DeploymentUnit(unitId, unitVersion));

    @BeforeEach
    void setUp() throws IOException {
        try (InputStream jarStream = jobsResource.openStream()) {
            CompletableFuture<Boolean> deployAsync = node(0).deployment().deployAsync(
                    unitId,
                    unitVersion,
                    () -> Map.of("ignite-jobs-1.0-SNAPSHOT.jar", jarStream),
                    MAJORITY
            );
            assertThat(deployAsync, willCompleteSuccessfully());
        }

        cluster.runningNodes().forEach(node -> {
            CompletableFuture<Boolean> deployAsync = node.deployment().onDemandDeploy(
                    unitId,
                    unitVersion
            );
            assertThat(deployAsync, willCompleteSuccessfully());
        });
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
}
