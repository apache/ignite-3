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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite3.client.IgniteClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.MountableFile;

/** Ignite 3 cluster container. */
public class Ignite3ClusterContainer implements Startable {
    private static Path COMPUTE_LIBS_FOLDER = Path.of("build/dependency");

    private static final Logger LOGGER = LogManager.getLogger(Ignite3ClusterContainer.class);

    public final Network network;

    public final GenericContainer node;

    private BufferedWriter logWriter;

    private @Nullable Map.Entry<String, String> credentials;

    public Ignite3ClusterContainer() {
        this(Network.newNetwork());
    }

    /**
     * Constructor.
     *
     * @param network The network to deploy the containers on.
     */
    public Ignite3ClusterContainer(Network network) {
        this.network = network;
        String imageName = dockerImageName();
        this.node = new GenericContainer(imageName)
                .withNetwork(network)
                .withNetworkAliases("ai3.node.1")
                .withCommand("--node-name clusterNode1")
                .withExposedPorts(10800)
                .waitingFor(Wait.forLogMessage(".*Components started.*", 1));
    }

    /** Sets the credentials for this cluster. */
    public Ignite3ClusterContainer withCredentials(@Nullable Map.Entry<String, String> credentials) {
        this.credentials = credentials;
        return this;
    }

    /**
     * Adds the compute adapter libraries to the container.
     *
     * @param extraComputeLibs Extra 3rd party libraries to add.
     * @return The cluster container.
     */
    public Ignite3ClusterContainer withComputeAdapterLibs(Collection<MountableFile> extraComputeLibs) {
        this.node.addEnv("IGNITE_INTERFACE_ADAPTER_COMPUTE_FOLDER", "/opt/ignite/compute-libs");

        try {
            // Copy bootstrap class (classworlds, compute-bootstrap)
            Files.list(COMPUTE_LIBS_FOLDER.resolve("lib"))
                    .map(MountableFile::forHostPath)
                    .forEach(mf -> this.node.withCopyFileToContainer(mf, "/opt/ignite/lib/"));

            // Copy compute libs (cache-api, ignite-core, compute-core)
            var baseComputeLibs = Files.list(COMPUTE_LIBS_FOLDER.resolve("compute-libs")).map(MountableFile::forHostPath);

            // And extraComputeLibs
            Stream.concat(baseComputeLibs, extraComputeLibs.stream())
                    .forEach(mf -> this.node.withCopyFileToContainer(mf, "/opt/ignite/compute-libs/"));
        } catch (IOException ex) {
            throw new RuntimeException("Problems copying compute libs to container", ex);
        }

        return this;
    }

    /** Enables debug. */
    public Ignite3ClusterContainer withDebug() {
        this.node.addEnv("IGNITE3_EXTRA_JVM_ARGS", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:8000");
        this.node.addExposedPort(8000);
        return this;
    }

    @Override
    public void start() {
        this.node.start();

        Path parentFolder = Path.of("build/test-logs");
        Path logFilePath = parentFolder.resolve("ignite-3-" + this.node.getContainerId().substring(0, 8));
        CountDownLatch startupLatch = new CountDownLatch(1);

        try {
            Files.createDirectories(parentFolder);
            this.logWriter = Files.newBufferedWriter(logFilePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.node.followOutput((Consumer<OutputFrame>) frame -> {
            var type = frame.getType();
            if (type == OutputFrame.OutputType.STDOUT || type == OutputFrame.OutputType.STDERR) {
                String msg = frame.getUtf8String();
                try {
                    this.logWriter.write(msg);
                } catch (IOException e) {
                    LOGGER.error("Error writing cluster logs to file: ", e);
                }

                if (msg.contains("[IgniteServerImpl] Apache Ignite started successfully!")) {
                    startupLatch.countDown();
                }
            }
        });

        Container.ExecResult execRes = null;
        try {
            List<String> args = new ArrayList<>(
                    List.of(
                            "/opt/ignite3cli/bin/ignite3",
                            "cluster",
                            "init",
                            "--name=ignite-cluster",
                            "--url=http://127.0.0.1:10300",
                            "--metastorage-group=clusterNode1")
            );

            if (credentials != null) {
                String credentialsArg = String.format(
                        "--config=ignite{security{enabled:true,authentication.providers:[{name:default,type:basic,users:"
                        + "[{username:%s,password:%s}]}]}}", credentials.getKey(), credentials.getValue());

                args.add(credentialsArg);
            }

            execRes = this.node.execInContainer(args.toArray(String[]::new));

            if (execRes.getExitCode() != 0) {
                throw new RuntimeException("Could not init cluster: " + execRes.getStderr());
            }

            boolean started = startupLatch.await(10_000, TimeUnit.MILLISECONDS);
            if (!started) {
                throw new RuntimeException("Failed to catch cluster started signal after cluster init");
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        this.node.stop();
        if (this.logWriter != null) {
            try {
                this.logWriter.close();
            } catch (IOException e) {
                LOGGER.error("Exception closing the cluster log file: ", e);
            }
        }
    }

    public String getInternalAddress() {
        return this.node.getNetworkAliases().get(0) + ":" + 10800;
    }

    public String getAddress() {
        return this.node.getHost() + ":" + this.node.getFirstMappedPort();
    }

    public IgniteClient.Builder clientBuilder() {
        return IgniteClient.builder().addresses(getAddress());
    }

    public IgniteClient buildClient() {
        return clientBuilder().build();
    }

    /**
     * Retrieves the ignite 3 docker image name which should be used in the tests.
     *
     * @return The docker image name.
     */
    public static String dockerImageName() {
        String imageName = System.getProperty("ignite3.docker.image");
        if (imageName == null) {
            throw new IllegalArgumentException("ignite3.docker.image property must be defined");
        }

        return imageName;
    }
}
