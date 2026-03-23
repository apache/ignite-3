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

package org.apache.ignite.internal.jobs;

import static org.apache.ignite.internal.jobs.Jobs.JOBS_UNIT;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.sneakyThrow;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.internal.IgniteCluster;
import org.apache.ignite.internal.cli.call.cluster.unit.DeployUnitClient;
import org.apache.ignite.rest.client.api.DeploymentApi;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.DeployMode;
import org.apache.ignite.rest.client.model.DeploymentStatus;
import org.apache.ignite.rest.client.model.UnitStatus;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class for deploying jobs.
 */
public class DeploymentUtils {
    /** Deploys all jobs in the module. */
    public static void deployJobs() {
        try {
            Path tmpJobsJarFile = Files.createTempFile("tests", "ignite-integration-test-jobs.jar");

            try (InputStream jobsJar = DeploymentUtils.class.getClassLoader()
                    .getResourceAsStream("units/ignite-integration-test-jobs-1.0-SNAPSHOT.jar")) {
                assert jobsJar != null : "Jobs jar is not found withing resources of the test. Are you sure your classpath is correct?";
                Files.copy(jobsJar, tmpJobsJarFile, StandardCopyOption.REPLACE_EXISTING);

                deployUnit(List.of(tmpJobsJarFile.toFile()), JOBS_UNIT.name(), JOBS_UNIT.version().render());
            } finally {
                Files.delete(tmpJobsJarFile);
            }
        } catch (IOException e) {
            sneakyThrow(e);
        }
    }

    private static void deployUnit(List<File> unitFiles, String unitName, String unitVersion) {
        DeployUnitClient deployUnitClient = new DeployUnitClient(new ApiClient());

        try {
            Boolean deployRes = deployUnitClient.deployUnit(unitName, unitFiles, unitVersion, DeployMode.ALL, List.of());
            assertTrue(deployRes);

            await().until(() -> ensureAllNodesDeployedUnit(unitName, unitVersion));
        } catch (ApiException e) {
            sneakyThrow(e);
        }
    }

    private static boolean ensureAllNodesDeployedUnit(String unitName, String unitVersion) {
        try {
            DeploymentApi api = new DeploymentApi(new ApiClient());

            List<DeploymentStatus> statuses = Arrays.asList(DeploymentStatus.values());
            List<UnitStatus> unitStatuses = Stream.concat(
                    api.listNodeStatusesByUnit(unitName, unitVersion, statuses).stream(),
                    api.listClusterStatusesByUnit(unitName, unitVersion, statuses).stream()
            ).collect(Collectors.toList());

            return unitStatuses.stream()
                    .allMatch(unitStatus -> unitStatus.getVersionToStatus()
                            .stream()
                            .allMatch(vts ->
                                    Objects.equals(vts.getVersion(), unitVersion)
                                            && vts.getStatus() == DeploymentStatus.DEPLOYED)
                    );
        } catch (Exception e) {
            sneakyThrow(e);
            return false;
        }
    }

    /** Run deployed job. */
    public static <T, R> R runJob(IgniteCluster cluster, Class<? extends ComputeJob<T, R>> jobClass, @Nullable T arg) {
        try (IgniteClient client = cluster.createClient()) {
            JobDescriptor<T, R> job = JobDescriptor.builder(jobClass)
                    .units(JOBS_UNIT)
                    .build();

            JobTarget jobTarget = JobTarget.anyNode(client.cluster().nodes());

            return client.compute().execute(jobTarget, job, arg);
        }
    }
}
