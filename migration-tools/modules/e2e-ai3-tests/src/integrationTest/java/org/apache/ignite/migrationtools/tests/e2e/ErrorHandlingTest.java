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
import org.apache.ignite.migrationtools.tests.bases.MigrationTestBase;
import org.apache.ignite.migrationtools.tests.clusters.FullSampleCluster;
import org.apache.ignite.migrationtools.tests.containers.Ignite3ClusterContainer;
import org.apache.ignite.migrationtools.tests.containers.MigrationToolsContainer;
import org.apache.ignite.migrationtools.tests.e2e.impl.MyOrganizationsCacheTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** IgniteClientCredentialsTest. */
@ExtendWith(FullSampleCluster.class)
@Testcontainers
public class ErrorHandlingTest {
    private static final Network network = Network.newNetwork();

    @Container
    private static final Ignite3ClusterContainer AI3_CLUSTER = new Ignite3ClusterContainer(network);

    @Container
    private static final MigrationToolsContainer migrationToolsContainer = new MigrationToolsContainer(network);

    @Test
    void recordMismatchTest() throws Exception {
        var logsFolder = Path.of(
                "build/test-logs/",
                ErrorHandlingTest.class.getSimpleName(),
                "recordMismatchTest");
        if (!Files.isDirectory(logsFolder)) {
            Files.createDirectories(logsFolder);
        }

        ExecResult execStatus = MigrationTestBase.migrateCache(
                migrationToolsContainer,
                0,
                new MyOrganizationsCacheTest().getTableName(),
                "ABORT",
                AI3_CLUSTER.getInternalAddress(),
                null,
                logsFolder);

        assertThat(execStatus.getExitCode()).isNotZero();
        assertThat(execStatus.getStderr())
                .contains("Error while migrating cache")
                .contains("Mismatch between cache records and the target table definition")
                .contains("The following fields were present on the record but not found in the table: lastUpdated, addr, type");
    }
}
