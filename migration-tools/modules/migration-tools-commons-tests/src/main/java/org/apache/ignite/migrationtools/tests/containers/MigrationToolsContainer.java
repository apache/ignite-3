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

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import org.apache.ignite.migrationtools.tests.clusters.FullSampleCluster;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.ExecConfig;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.MountableFile;

/** Container running the migrtion tools. */
public class MigrationToolsContainer implements Startable {
    public final GenericContainer container;

    /**
     * Constructor.
     *
     * @param network Network.
     */
    public MigrationToolsContainer(Network network) {
        String imageName = System.getProperty("migrationtools.cli.docker.image");
        if (imageName == null) {
            throw new IllegalArgumentException("'migrationtools.cli.docker.image' property must be defined");
        }

        this.container = new GenericContainer<>(imageName)
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("/bin/bash"))
                .withNetwork(network)
                .withCommand("-c", "sleep infinity")
                .withCopyFileToContainer(MountableFile.forHostPath(FullSampleCluster.CLUSTER_CFG_PATH), "/config-file.xml")
                .withFileSystemBind(FullSampleCluster.TEST_CLUSTER_PATH.toString(), "/storage", BindMode.READ_WRITE)
                .withEnv("CONFIG_URI", "/config-file.xml");
    }

    /**
     * Enables debugging on port 5005.
     *
     * @return The same instance of the container;
     */
    public MigrationToolsContainer withDebug() {
        container.addEnv("EXTRA_JVM_OPTS", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005");
        container.setPortBindings(Collections.singletonList("5005:5005"));
        return this;
    }

    public ExecResult execInContainer(String... command) throws UnsupportedOperationException, IOException, InterruptedException {
        return container.execInContainer(command);
    }

    public ExecResult execInContainer(Charset outputCharset, String... command)
            throws UnsupportedOperationException, IOException, InterruptedException {
        return container.execInContainer(outputCharset, command);
    }

    public ExecResult execInContainer(ExecConfig execConfig) throws UnsupportedOperationException, IOException, InterruptedException {
        return container.execInContainer(execConfig);
    }

    public ExecResult execInContainer(Charset outputCharset, ExecConfig execConfig)
            throws UnsupportedOperationException, IOException, InterruptedException {
        return container.execInContainer(outputCharset, execConfig);
    }

    public void copyFileFromContainer(String containerPath, String destinationPath) {
        container.copyFileFromContainer(containerPath, destinationPath);
    }

    @Override
    public void start() {
        container.start();
    }

    @Override
    public void stop() {
        container.stop();
    }
}
