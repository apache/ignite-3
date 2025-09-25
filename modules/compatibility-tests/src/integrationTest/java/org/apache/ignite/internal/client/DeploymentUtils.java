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

package org.apache.ignite.internal.client;

import static org.apache.ignite.internal.client.ClientCompatibilityTests.JOBS_UNIT;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.getResourcePath;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.sneakyThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import java.io.File;
import java.nio.file.Path;
import java.util.List;
import org.apache.ignite.internal.cli.call.cluster.unit.DeployUnitClient;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.DeployMode;

/**
 * Utility class for deploying jobs.
 */
class DeploymentUtils {
    static void deployJobs() {
        File jobsJar = Path.of(
                getResourcePath(ClientCompatibilityTests.class, ""),
                "../../../libs/ignite-integration-test-jobs-1.0-SNAPSHOT.jar").toFile();

        deployUnit(List.of(jobsJar), JOBS_UNIT.name(), JOBS_UNIT.version().render());
    }

    private static void deployUnit(List<File> unitFiles, String unitName, String unitVersion) {
        // TODO IGNITE-26418 Netty buffer leaks in REST API
        ResourceLeakDetector.setLevel(Level.DISABLED);

        DeployUnitClient deployUnitClient = new DeployUnitClient(new ApiClient());

        try {
            Boolean deployRes = deployUnitClient.deployUnit(unitName, unitFiles, unitVersion, DeployMode.ALL, List.of());
            assertTrue(deployRes);
        } catch (ApiException e) {
            sneakyThrow(e);
        }
    }
}
