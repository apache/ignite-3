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
import java.util.Collections;
import java.util.List;
import org.apache.ignite.migrationtools.tests.clusters.FullSampleCluster;
import org.apache.ignite.migrationtools.tests.containers.Ignite3ClusterContainer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

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

    public static final String DOCKER_IMAGE_NAME = "ai3-migration-tools:" + System.getProperty("migration-tools.docker.version", "latest");

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
    protected static final GenericContainer migrationToolsContainer;

    static {
        // TODO: This should receive the correct version.
        migrationToolsContainer = new GenericContainer<>(DOCKER_IMAGE_NAME)
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("/bin/bash"))
            .withNetwork(network)
            .withCommand("-c", "sleep infinity")
            .withCopyFileToContainer(MountableFile.forHostPath(FullSampleCluster.CLUSTER_CFG_PATH), "/config-file.xml")
            .withFileSystemBind(FullSampleCluster.TEST_CLUSTER_PATH.toString(), "/storage", BindMode.READ_WRITE)
            .withEnv("CONFIG_URI", "/config-file.xml");

        if (DEBUG_MODE) {
            migrationToolsContainer.addEnv("EXTRA_JVM_OPTS", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005");
            migrationToolsContainer.setPortBindings(Collections.singletonList("5005:5005"));
        }
    }

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
            String nodeId = NODE_NAMES.get(i);

            var fmt = "migration-%s-%d.%s";
            var logPath = logsFolder.resolve(Path.of(String.format(fmt, cacheName, i, "log")));
            var stdoutPath = logsFolder.resolve(Path.of(String.format(fmt, cacheName, i, "stdout")));
            var stderrPath = logsFolder.resolve(Path.of(String.format(fmt, cacheName, i, "stderr")));
            try (var outStream = Files.newOutputStream(stdoutPath); var errStream = Files.newOutputStream(stderrPath)) {
                long startTime = System.currentTimeMillis();

                var migrationCmd = migrationToolsContainer.execInContainer(
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
                );

                long finishTime = System.currentTimeMillis() - startTime;

                outStream.write(migrationCmd.getStdout().getBytes(StandardCharsets.UTF_8));
                errStream.write(migrationCmd.getStderr().getBytes(StandardCharsets.UTF_8));
                migrationToolsContainer.copyFileFromContainer("/root/.ignite-migration-tools/logs/ignite-0.log", logPath.toString());

                assertThat(migrationCmd.getExitCode()).as("Migration command should have finished successfully").isZero();

                LOGGER.info("Finished migrating cache: {};{};{};{}", nodeId, cacheName, migrationMode, finishTime);
            }
        }
    }



}
