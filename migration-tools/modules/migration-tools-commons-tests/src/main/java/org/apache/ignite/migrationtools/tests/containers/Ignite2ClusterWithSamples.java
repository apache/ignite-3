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

package org.apache.ignite.migrationtools.tests.containers;

import static java.lang.Thread.sleep;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.ignite.migrationtools.tests.clusters.FullSampleCluster;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.MountableFile;

/** Ignite2ClusterWithSamples. */
public abstract class Ignite2ClusterWithSamples implements BeforeAllCallback {

    private final Path targetPath;

    public Ignite2ClusterWithSamples(Path targetPath) {
        this.targetPath = targetPath;
    }

    /**
     * TODO: Copied from IgniteTestUtils should reuse this.
     *
     * <p>This method is deprecated in favor of the Awaitility library; use {@link Awaitility#await()} instead.
     */
    @Deprecated
    public static boolean waitForCondition(BooleanSupplier cond, long sleepMillis, long timeoutMillis) throws InterruptedException {
        long stop = System.currentTimeMillis() + timeoutMillis;

        while (System.currentTimeMillis() < stop) {
            if (cond.getAsBoolean()) {
                return true;
            }

            sleep(sleepMillis);
        }

        return false;
    }

    protected abstract Ignite2ClusterContainer createClusterContainers();

    private void recreateClusterFolder() throws InterruptedException, IOException {
        // Recreate the model cluster.
        // TODO: This must be improved.
        System.out.println("Recreating the sample-cluster");
        System.out.println("Current Directory; " + Path.of(".").toAbsolutePath().toString());

        Files.createDirectory(targetPath);

        // Get from the outside
        try (var cluster = createClusterContainers()) {
            cluster.start();

            // Put the seed data container log in another folder
            try (var seedLogOs = Files.newOutputStream(targetPath.resolve("seeddata-container.log"))) {
                Network clusterNetwork = cluster.getNetwork();
                try (var seedDataContainer = createSeedDataCountainer(clusterNetwork)) {
                    seedDataContainer.start();
                    seedDataContainer.followOutput((Consumer<OutputFrame>) frame -> {
                        if (frame.getType() != OutputFrame.OutputType.END) {
                            try {
                                seedLogOs.write(frame.getBytes());
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });

                    // Wait for the container to stop.
                    var maxWaitSeconds = Integer.parseInt(System.getProperty("ai2.sampleCluster.recreate.seeding.maxwaitseconds", "7200"));
                    var pollingSeconds = Integer.parseInt(System.getProperty("ai2.sampleCluster.recreate.seeding.pollingseconds", "180"));
                    boolean success = waitForCondition(
                            () -> !seedDataContainer.isRunning(), pollingSeconds * 1_000, maxWaitSeconds * 1_000);

                    // TODO: Status are not working. Should be a bug in test-containers??
                    var state = seedDataContainer.getContainerInfo().getState();
                    System.out.println("Seed data status: " + state);

                    Assertions.assertTrue(success, "Timeout waiting for seed data to finish.");
                    Assertions.assertEquals(0, state.getExitCodeLong(), "Seed data container exit code is not 0");

                    System.out.println("Finished waiting for seed data container to run");
                }
            }

            System.out.println("Stopping the containers");
        }
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        // TODO: Find a better way to produce the example cluster.
        // Currently we have to manually create a cluster and run the sample cluster generator in the tools folder.
        if (!Files.exists(targetPath)) {
            recreateClusterFolder();
        }
    }

    protected GenericContainer createSeedDataCountainer(Network network) {
        return new GenericContainer("ai3-migration-tools/e2e-tests:latest")
                .withCopyFileToContainer(MountableFile.forHostPath(FullSampleCluster.CLUSTER_CFG_PATH), "/opt/app/config.xml")
                .withNetwork(network);
    }
}
