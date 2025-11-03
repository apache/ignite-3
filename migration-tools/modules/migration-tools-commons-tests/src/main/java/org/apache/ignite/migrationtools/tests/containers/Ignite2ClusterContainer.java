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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.migrationtools.tests.containers.Ignite2ClusterWithSamples.waitForCondition;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/** Container of an Ignite 2 cluster. */
public class Ignite2ClusterContainer implements Startable {
    private static final Logger LOGGER = LogManager.getLogger(Ignite2ClusterContainer.class);
    private static final int THIN_CLIENT_PORT = 10_800;

    public final Network network;

    private final List<GenericContainer<?>> containers;

    private final boolean storagePathMappedToExternal;

    private final String igniteHome;

    /**
     * Port on host which binds container's 10800.
     */
    private int thinClientMappedPort;

    /**
     * Docker host address.
     */
    private String dockerHost;

    public Ignite2ClusterContainer(Path cfgFilePath, @Nullable Path storagePathOnHost, List<String> nodeIds) {
        this(Network.newNetwork(), cfgFilePath, storagePathOnHost, nodeIds);
    }

    /**
     * Constructor.
     *
     * @param network Network.
     * @param cfgFilePath Configuration Path.
     * @param storagePathOnHost Storage path mounted on the host.
     * @param nodeIds List of node consistent ids.
     */
    public Ignite2ClusterContainer(Network network, Path cfgFilePath, @Nullable Path storagePathOnHost, List<String> nodeIds) {
        this.network = network;
        this.containers = new ArrayList<>(nodeIds.size());
        this.storagePathMappedToExternal = storagePathOnHost != null;

        String dockerImageName = System.getProperty("ignite2.docker.image");
        assert dockerImageName != null : "ignite2.docker.image must be defined";
        DockerImageName dockerImage = DockerImageName.parse(dockerImageName);

        this.igniteHome = "/opt/ignite/apache-ignite";

        for (int i = 0; i < nodeIds.size(); i++) {
            String hostname = "node" + (1 + i);
            String nodeId = nodeIds.get(i);

            var nodeContainer = createIgnite2Container(
                    dockerImage,
                    hostname,
                    nodeId,
                    cfgFilePath,
                    storagePathOnHost
            );

            containers.add(nodeContainer);
        }

        // Expose a SQL port on the first node.
        this.containers.get(0).withExposedPorts(THIN_CLIENT_PORT);
    }

    public Network getNetwork() {
        return network;
    }

    private GenericContainer<?> createIgnite2Container(
            DockerImageName dockerImage,
            String hostName,
            String nodeId,
            Path cfgFilePath,
            @Nullable Path storagePathOnHost
    ) {
        Consumer<OutputFrame> logConsumer = new CheckpointerLogConsumer();
        String heapSize = System.getProperty("ai2.sampleCluster.Xmx", "10g");

        GenericContainer<?> container = new GenericContainer<>(dockerImage);

        if (storagePathMappedToExternal) {
            container.withFileSystemBind(storagePathOnHost.toString(), "/storage", BindMode.READ_WRITE)
                    .withEnv("IGNITE_WORK_DIR", "/storage");
        }

        return container
                .withLabel("ai2.sample-cluster.node", hostName)
                .withNetwork(network)
                .withNetworkAliases(hostName)
                .withCopyFileToContainer(MountableFile.forHostPath(cfgFilePath), "/config-file.xml")
                .withEnv("CONFIG_URI", "/config-file.xml")
                .withEnv("IGNITE_QUIET", "false")
                .withEnv("IGNITE_NODE_NAME", nodeId)
                .withEnv("JVM_OPTS", String.format("-Xmx%s", heapSize))
                .withEnv("TZ", ZoneId.systemDefault().toString())
                .withLogConsumer(logConsumer)
                .waitingFor(Wait.forLogMessage(".*Node started .*", 1));
    }

    @Override
    public void start() {
        Startables.deepStart(this.containers).join();

        GenericContainer<?> firstContainer = containers.get(0);

        thinClientMappedPort = firstContainer.getMappedPort(THIN_CLIENT_PORT);
        dockerHost = firstContainer.getHost();
    }

    @Override
    public void stop() {
        doStop(true);
    }

    /**
     * Stop the cluster.
     *
     * @param waitForCheckpoints Whether should wait for checkpoints to be finished or not.
     */
    public void doStop(boolean waitForCheckpoints) {
        if (waitForCheckpoints) {
            try {
                waitForNextCheckpoint();
            } catch (Exception ex) {
                LOGGER.error("Error waiting for checkpoints: ", ex);
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        // TODO: CHMOD This is a hack for team city
        if (storagePathMappedToExternal) {
            try {
                var firstContainer = this.containers.get(0);
                var chmodOp = firstContainer.execInContainer("chmod", "-R", "777", "/storage");
                assertThat(chmodOp.getExitCode()).withFailMessage("CHMOD must be successfull").isZero();
            } catch (IOException ex) {
                LOGGER.error("Error executing chmod", ex);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                LOGGER.error("Interrupted while executing chmod", ex);
            }
        }

        // TODO: Close all at the same time.
        for (GenericContainer<?> node : this.containers) {
            node.stop();
        }
    }

    /**
     * Waits for the next checkpoint to happen.
     *
     * <p>TODO: https://issues.apache.org/jira/browse/IGNITE-26177
     *
     * @throws InterruptedException when the thread was interrupted while waiting.
     */
    @SuppressWarnings("PMD.UnnecessaryCast")
    public void waitForNextCheckpoint() throws InterruptedException {
        // TODO: Make a specific checkpoint log implementation.
        // TODO: In the future, we will be able to call shutdown(false) on the container to properly shutdown the nodes.

        LOGGER.info("Waiting for checkpoints to happen. This might take a while.");
        var nodeCheckpointCheckers = this.containers.stream()
                .map(container -> {
                    String nodeLabel = container.getLabels().get("ai2.sample-cluster.node");
                    CheckpointerLogConsumer logConsumer = (CheckpointerLogConsumer) container.getLogConsumers().get(0);
                    AtomicBoolean checkpointFinished = new AtomicBoolean(false);
                    Runnable onCheckpoint = () -> {
                        LOGGER.info("Checkpoint finished on node: {}", nodeLabel);
                        checkpointFinished.set(true);
                    };

                    logConsumer.onCheckpoint(onCheckpoint);
                    return (BooleanSupplier) () -> {
                        if (!container.isRunning() && checkpointFinished.compareAndSet(false, true)) {
                            LOGGER.warn("Container stopped running before the checkpoint was finished: {}", nodeLabel);
                        }

                        if (checkpointFinished.get()) {
                            logConsumer.removeListener(onCheckpoint);
                            return true;
                        } else {
                            return false;
                        }
                    };
                })
                .collect(toList());

        var maxWaitSeconds = Integer.parseInt(System.getProperty("ai2.sampleCluster.checkpointChecker.maxwaitseconds", "360"));
        var pollingSeconds = Integer.parseInt(System.getProperty("ai2.sampleCluster.checkpointChecker.pollingseconds", "10"));
        boolean success = waitForCondition(() -> nodeCheckpointCheckers.stream().allMatch(BooleanSupplier::getAsBoolean),
                pollingSeconds * 1_000, maxWaitSeconds * 1_000);

        LOGGER.info("Finished waiting for checkpoints: {}", success);
    }

    /**
     * Returns host's port which can be used for thin client connection.
     *
     * @return Host's port for thin client connection.
     */
    public int thinClientMappedPort() {
        return thinClientMappedPort;
    }

    /**
     * Returns docker host address.
     *
     * @return Docker host address.
     */
    public String dockerHost() {
        return dockerHost;
    }

    /**
     * Copies the supplied file to all the containers classpath.
     *
     * @param fileToCopy File to copy.
     */
    public void withFileInClasspath(MountableFile fileToCopy) {
        for (var container : this.containers) {
            container.withCopyFileToContainer(fileToCopy, this.igniteHome + "/libs/");
        }
    }

    private static class CheckpointerLogConsumer implements Consumer<OutputFrame> {

        private List<Runnable> listeners = new CopyOnWriteArrayList<>();

        @Override
        public void accept(OutputFrame frame) {
            if (listeners.isEmpty()) {
                return;
            }

            if (frame.getType() == OutputFrame.OutputType.STDOUT || frame.getType() == OutputFrame.OutputType.STDERR) {
                String msg = frame.getUtf8String();
                if (msg.contains("Checkpoint finished") || msg.contains("Skipping checkpoint (no pages were modified)")) {
                    // Notify listeners
                    for (Runnable listener : listeners) {
                        listener.run();
                    }
                }
            }
        }

        public void onCheckpoint(Runnable action) {
            this.listeners.add(action);
        }

        public void removeListener(Runnable action) {
            this.listeners.remove(action);
        }
    }

}
