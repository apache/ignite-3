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

package org.apache.ignite.migrationtools.tests.bases;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.migrationtools.tests.clusters.FullSampleCluster;
import org.apache.ignite.migrationtools.tests.containers.Ignite3ClusterContainer;
import org.apache.ignite.migrationtools.tests.containers.MigrationToolsContainer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.ExecConfig;
import org.testcontainers.containers.ExecConfig.ExecConfigBuilder;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Base class for the migration tests.
 */
@DisabledIfSystemProperty(
        named = "tests.containers.support",
        matches = "false",
        disabledReason = "Lack of support in TeamCity for testcontainers")
@ExtendWith(FullSampleCluster.class)
@Testcontainers
public class MigrationTestBase {
    private static final Logger LOGGER = LogManager.getLogger(MigrationTestBase.class);

    private static boolean DEBUG_MODE = Boolean.parseBoolean(System.getProperty("debugMode", "false"));

    private static final Path MIGRATION_TOOLS_CLI_FOLDER = Path.of("target/dependency/migration-tools-cli.jar");

    // TODO: The output of list-caches must be easily parsable so that we can extract the nodeNames
    protected static final List<String> NODE_NAMES = List.of(
            "ad26bff6-5ff5-49f1-9a61-425a827953ed",
            "c1099d16-e7d7-49f4-925c-53329286c444",
            "7b880b69-8a9e-4b84-b555-250d365e2e67"
    );

    protected static final Network network = Network.newNetwork();

    // TODO: Add a cluster instead of a single instance. Create a ignite stuff perhaps.
    @Container
    protected static final Ignite3ClusterContainer AI3_CLUSTER = new Ignite3ClusterContainer(network);

    @Container
    protected static final MigrationToolsContainer MIGRATION_TOOLS_CONTAINER = new MigrationToolsContainer(network);

    @AfterAll
    static void tearDownNetwork() {
        if (network != null) {
            network.close();
        }
    }

    /** Assert that the migration is successfull. */
    public static void migrationIsSuccessfull(String cacheName, String migrationMode) throws IOException, InterruptedException {
        // TODO: Refactor
        String clusterAddress = "ai3.node.1:10800";

        // migrationArgs.add(1, "");
        // boolean debugEnabled = Optional.ofNullable(System.getProperty("debug.migration"))
        //        .map(Boolean::parseBoolean)
        //        .orElse(false);

        var logsFolder = Path.of("build/test-logs/");
        if (!Files.isDirectory(logsFolder)) {
            Files.createDirectory(logsFolder);
        }

        for (int i = 0; i < NODE_NAMES.size(); i++) {
            ExecResult execResult = migrateCache(MIGRATION_TOOLS_CONTAINER, i, cacheName, migrationMode, clusterAddress, null, logsFolder);

            assertThat(execResult.getExitCode()).as("Migration command should have finished successfully").isZero();
        }
    }

    /**
     * Executes the migration cache command in a container running the migration tools.
     *
     * @param migrationToolsContainer Migration tools container.
     * @param nodeIdx Index of the node in the sample cluster to migrate from.
     * @param cacheName Cacha name.
     * @param migrationMode Migration Mode.
     * @param clusterAddress Cluster address.
     * @param credentials Cluster credentials, if any.
     * @param logsFolder Logs folder to print the logs.
     * @return The execution result.
     * @throws IOException IOException.
     * @throws InterruptedException InterruptionException.
     */
    public static ExecResult migrateCache(
            MigrationToolsContainer migrationToolsContainer,
            int nodeIdx,
            String cacheName,
            String migrationMode,
            String clusterAddress,
            @Nullable Map.Entry<String, String> credentials,
            Path logsFolder
    ) throws IOException, InterruptedException {
        String nodeId = NODE_NAMES.get(nodeIdx);

        var fmt = "migration-%s-%d.%s";
        var logPath = logsFolder.resolve(Path.of(String.format(fmt, cacheName, nodeIdx, "log")));
        var stdoutPath = logsFolder.resolve(Path.of(String.format(fmt, cacheName, nodeIdx, "stdout")));
        var stderrPath = logsFolder.resolve(Path.of(String.format(fmt, cacheName, nodeIdx, "stderr")));
        try (var outStream = Files.newOutputStream(stdoutPath); var errStream = Files.newOutputStream(stderrPath)) {
            long startTime = System.currentTimeMillis();

            List<String> arguments = new ArrayList<>(List.of(
                    "migration-tools",
                    "persistent-data",
                    "/storage",
                    nodeId,
                    "/config-file.xml",
                    "migrate-cache",
                    cacheName,
                    clusterAddress,
                    "--mode",
                    migrationMode
            ));

            ExecConfigBuilder execConfigBuilder = ExecConfig.builder();

            if (credentials != null) {
                arguments.add("--client.basicAuthenticator.username");
                arguments.add(credentials.getKey());

                execConfigBuilder.envVars(Map.of("IGNITE_CLIENT_SECRET", credentials.getValue()));
            }

            execConfigBuilder.command(arguments.toArray(String[]::new));

            ExecConfig execConfig = execConfigBuilder.build();

            var migrationCmd = migrationToolsContainer.execInContainer(execConfig);

            long finishTime = System.currentTimeMillis() - startTime;

            outStream.write(migrationCmd.getStdout().getBytes(StandardCharsets.UTF_8));
            errStream.write(migrationCmd.getStderr().getBytes(StandardCharsets.UTF_8));
            migrationToolsContainer.copyFileFromContainer("/root/.ignite-migration-tools/logs/ignite-0.log", logPath.toString());

            LOGGER.info("Finished migrating cache: {};{};{};{}", nodeId, cacheName, migrationMode, finishTime);

            return migrationCmd;
        }
    }
}
