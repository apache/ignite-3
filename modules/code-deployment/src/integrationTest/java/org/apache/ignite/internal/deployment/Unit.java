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

package org.apache.ignite.internal.deployment;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.deployunit.configuration.DeploymentExtensionConfiguration;
import org.apache.ignite.internal.deployunit.structure.UnitEntry;
import org.apache.ignite.internal.deployunit.structure.UnitFile;
import org.apache.ignite.internal.deployunit.structure.UnitFolder;

class Unit {
    private final IgniteImpl deployedNode;

    private final String id;

    private final Version version;

    private final List<DeployFile> files;

    Unit(IgniteImpl deployedNode, String id, Version version, List<DeployFile> files) {
        this.deployedNode = deployedNode;
        this.id = id;
        this.version = version;
        this.files = files;
    }

    public String id() {
        return id;
    }

    public Version version() {
        return version;
    }

    public List<DeployFile> files() {
        return files;
    }

    IgniteImpl deployedNode() {
        return deployedNode;
    }

    CompletableFuture<Boolean> undeployAsync() {
        return deployedNode.deployment().undeployAsync(id, version);
    }

    void undeploy() {
        deployedNode.deployment().undeployAsync(id, version);
        waitUnitClean(deployedNode);
    }

    void waitUnitClean(IgniteImpl ignite) {
        Path unitDirectory = getNodeUnitDirectory(ignite);

        int combinedTimeout = files.stream().map(DeployFile::replicaTimeout).reduce(Integer::sum).get();

        await().timeout(combinedTimeout, SECONDS)
                .pollDelay(2, SECONDS)
                .until(() -> {
                    for (DeployFile file : files) {
                        Path filePath = unitDirectory.resolve(file.file().getFileName());
                        if (Files.exists(filePath)) {
                            return false;
                        }
                    }

                    return true;
                });
    }

    Path getNodeUnitDirectory(IgniteImpl ignite) {
        String deploymentFolder = ignite.nodeConfiguration()
                .getConfiguration(DeploymentExtensionConfiguration.KEY).deployment()
                .location().value();

        return ignite.workDir()
                .resolve(deploymentFolder)
                .resolve(id)
                .resolve(version.render());
    }

    public Path getNodeUnitDirectory() {
        return getNodeUnitDirectory(deployedNode);
    }

    void waitUnitReplica(IgniteImpl ignite) {
        int combinedTimeout = files.stream().map(DeployFile::replicaTimeout).reduce(Integer::sum).get();

        await().timeout(combinedTimeout, SECONDS)
                .pollDelay(1, SECONDS)
                .ignoreException(IOException.class)
                .until(() -> {
                    for (DeployFile file : files) {
                        verify(file, ignite);
                    }

                    return true;
                });
    }

    public void verify(DeployFile file, IgniteImpl entryNode) {
        Path nodeUnitDirectory = getNodeUnitDirectory(entryNode);
        if (file.zip()) {
            try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(file.file()))) {
                ZipEntry ze;
                while ((ze = zis.getNextEntry()) != null) {
                    assertTrue(Files.exists(nodeUnitDirectory.resolve(ze.getName())));
                }
            } catch (IOException e) {
                fail(e);
            }
        } else {
            try {
                Path filePath = nodeUnitDirectory.resolve(file.file().getFileName());
                assertTrue(Files.exists(filePath));
                assertThat(Files.size(filePath), is(file.expectedSize()));
            } catch (IOException e) {
                fail(e);
            }
        }
    }

    void verifyByRest(IgniteImpl entryNode) {
        Path currentDir = getNodeUnitDirectory(entryNode);

        CompletableFuture<Void> result = entryNode.deployment().nodeUnitFileStructure(id, version).thenAccept(folder -> {
            try {
                for (UnitEntry child : folder.children()) {
                    processEntry(child, currentDir);
                }
            } catch (IOException e) {
                fail(e);
            }
        });

        assertThat(result, willCompleteSuccessfully());
    }

    private static void processEntry(UnitEntry entry, Path currentDir) throws IOException {
        if (entry instanceof UnitFile) {
            UnitFile file = (UnitFile) entry;
            Path filePath = currentDir.resolve(file.name());
            assertTrue(Files.exists(filePath));
            assertThat(Files.size(filePath), is(file.size()));
        } else if (entry instanceof UnitFolder) {
            Path dir = currentDir.resolve(entry.name());
            for (UnitEntry child : ((UnitFolder) entry).children()) {
                processEntry(child, dir);
            }
        } else {
            fail(new IllegalStateException("Unit entry type not supported."));
        }
    }
}
