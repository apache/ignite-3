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

package org.apache.ignite.migrationtools.tests.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.ignite.migrationtools.tests.bases.MigrationTestBase;
import org.apache.ignite.migrationtools.tests.clusters.FullSampleCluster;
import org.apache.ignite.migrationtools.tests.containers.Ignite3ClusterContainer;
import org.apache.ignite.migrationtools.tests.containers.MigrationToolsContainer;
import org.apache.ignite.migrationtools.tests.e2e.framework.core.DiscoveryUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** IgniteClientCredentialsTest. */
@ExtendWith(FullSampleCluster.class)
@Testcontainers
public class IgniteClientCredentialsTest {
    private static final String TABLE_NAME = DiscoveryUtils.discoverClasses().get(0).getTableName();

    private static final Map.Entry<String, String> credentials = Map.entry("testUserName", "testPassword123");

    private static final Network network = Network.newNetwork();

    @Container
    private static final Ignite3ClusterContainer AI3_CLUSTER = new Ignite3ClusterContainer(network)
            .withCredentials(credentials)
            .withLabel(IgniteClientCredentialsTest.class.getSimpleName());

    @Container
    private static final MigrationToolsContainer migrationToolsContainer = new MigrationToolsContainer(network);

    @Test
    void failsWithoutCredentials() throws Exception {
        var logsFolder = Path.of(
                "build/test-logs/",
                IgniteClientCredentialsTest.class.getSimpleName(),
                "failsWithoutCredentials");
        if (!Files.isDirectory(logsFolder)) {
            Files.createDirectories(logsFolder);
        }

        ExecResult execStatus = MigrationTestBase.migrateCache(
                migrationToolsContainer,
                0,
                TABLE_NAME,
                "IGNORE_COLUMN",
                AI3_CLUSTER.getInternalAddress(),
                null,
                logsFolder);

        assertThat(execStatus.getExitCode()).isNotZero();
        assertThat(execStatus.getStderr()).contains("Invalid client authentication credentials");
    }

    @Test
    void failsWithWrongPassword() throws Exception {
        var logsFolder = Path.of(
                "build/test-logs/",
                IgniteClientCredentialsTest.class.getSimpleName(),
                "failsWithWrongPassword");
        if (!Files.isDirectory(logsFolder)) {
            Files.createDirectories(logsFolder);
        }

        ExecResult execStatus = MigrationTestBase.migrateCache(
                migrationToolsContainer,
                0,
                TABLE_NAME,
                "IGNORE_COLUMN",
                AI3_CLUSTER.getInternalAddress(),
                Map.entry(credentials.getKey(), "wrongPassword"),
                logsFolder);

        assertThat(execStatus.getExitCode()).isNotZero();
        assertThat(execStatus.getStderr()).contains("Invalid client authentication credentials");
    }

    @Test
    void successWithCorrectCredentials() throws Exception {
        var logsFolder = Path.of(
                "build/test-logs/",
                IgniteClientCredentialsTest.class.getSimpleName(),
                "successWithCorrectCredentials");
        if (!Files.isDirectory(logsFolder)) {
            Files.createDirectories(logsFolder);
        }

        ExecResult execStatus = MigrationTestBase.migrateCache(
                migrationToolsContainer,
                0,
                TABLE_NAME,
                "IGNORE_COLUMN",
                AI3_CLUSTER.getInternalAddress(),
                credentials,
                logsFolder);

        assertThat(execStatus.getExitCode()).isZero();
    }
}
