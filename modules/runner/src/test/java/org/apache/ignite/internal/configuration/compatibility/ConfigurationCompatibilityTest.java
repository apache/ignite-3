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

package org.apache.ignite.internal.configuration.compatibility;

import static org.apache.ignite.internal.configuration.compatibility.framework.ConfigurationSnapshotManager.loadSnapshotFromResource;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ServiceLoaderModulesProvider;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNodeSerializer;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigShuttle;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigurationSnapshotManager;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigurationTreeComparator;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigurationTreeScanner;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigurationTreeScanner.ScanContext;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataInput;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataOutput;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for configuration compatibility.
 */
public class ConfigurationCompatibilityTest extends IgniteAbstractTest {
    private static final String DEFAULT_FILE_NAME = "snapshot.bin";
    private static final String SNAPSHOTS_RESOURCE_LOCATION = "compatibility/configuration/";
    private static final Path DEFAULT_SNAPSHOT_FILE = Path.of("modules", "runner", "build", "work", DEFAULT_FILE_NAME);

    private static final IgniteLogger LOG = Loggers.forClass(ConfigurationCompatibilityTest.class);

    /**
     * This test ensures that the current configuration can be serialized and deserialized correctly.
     */
    @Test
    void testConfigurationMetadataSerialization() throws IOException {
        // Load current configuration metadata.
        List<ConfigNode> actualTrees = loadCurrentConfiguration();

        assertFalse(actualTrees.isEmpty());

        // Serialize, then deserialize the metadata.
        IgniteUnsafeDataOutput output = new IgniteUnsafeDataOutput(1024);

        ConfigNodeSerializer.writeAsJson(actualTrees, output);

        byte[] data = output.array();

        assertTrue(data.length > 0);

        List<ConfigNode> snapshot = ConfigNodeSerializer.readAsJson(new IgniteUnsafeDataInput(data));

        // Validate restored metadata.
        ConfigurationTreeComparator.compare(snapshot, actualTrees);
    }

    /**
     * This test ensures that the current configuration metadata wasn't changed. If the test fails, it means that the current configuration
     * metadata has changed, then current snapshot should be renamed to the latest release version, and a new snapshot should be created.
     *
     * @see #main(String[]) method for generating a new snapshot.
     */
    @Test
    void testConfigurationChanged() throws IOException {
        List<ConfigNode> currentMetadata = loadCurrentConfiguration();
        List<ConfigNode> snapshotMetadata = loadSnapshotFromResource(SNAPSHOTS_RESOURCE_LOCATION + DEFAULT_FILE_NAME);

        ConfigurationTreeComparator.compare(currentMetadata, snapshotMetadata);
    }

    /**
     * This test ensures that the current configuration metadata is compatible with the snapshots. If the test fails, it means that the
     * current configuration is incompatible with the snapshot, and compatibility should be fixed.
     */
    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("getSnapshots")
    void testConfigurationCompatibility(String fileName) throws IOException {
        List<ConfigNode> currentMetadata = loadCurrentConfiguration();
        List<ConfigNode> snapshotMetadata = loadSnapshotFromResource(SNAPSHOTS_RESOURCE_LOCATION + fileName);

        ConfigurationTreeComparator.ensureCompatible(currentMetadata, snapshotMetadata);
    }

    private static List<ConfigNode> loadCurrentConfiguration() {
        ConfigurationModules modules = loadConfigurationModules(ConfigurationCompatibilityTest.class.getClassLoader());

        ConfigurationModule local = modules.local();
        ConfigurationModule distributed = modules.distributed();

        return Stream.concat(
                        local.rootKeys().stream().map(rootKey -> scanRootNode(local, rootKey)),
                        distributed.rootKeys().stream().map(rootKey -> scanRootNode(distributed, rootKey))
                )
                .collect(Collectors.toList());
    }

    private static ConfigNode scanRootNode(ConfigurationModule module, RootKey rootKey) {
        ScanContext scanContext = ScanContext.create(module);

        Class<?> rootClass = rootKey.schemaClass();
        ConfigNode root = ConfigNode.createRoot(rootKey.key(), rootClass, rootKey.type(), rootKey.internal());

        ConfigurationTreeScanner.scan(root, rootClass, scanContext);

        return root;
    }

    /** Load configuration modules from classpath. */
    private static ConfigurationModules loadConfigurationModules(@Nullable ClassLoader classLoader) {
        var modulesProvider = new ServiceLoaderModulesProvider();
        List<ConfigurationModule> modules = modulesProvider.modules(classLoader);

        if (modules.isEmpty()) {
            throw new IllegalStateException("No configuration modules were loaded, this means Ignite cannot start. "
                    + "Please make sure that the classloader for loading services is correct.");
        }

        return new ConfigurationModules(modules);
    }

    /**
     * Generates a snapshot of the current configuration metadata and saves it to a file.
     */
    public static void main(String[] args) throws IOException {
        List<ConfigNode> configNodes = loadCurrentConfiguration();

        ConfigShuttle shuttle = node -> LOG.info(node.toString());
        LOG.info("DUMP TREE:");
        configNodes.forEach(c -> c.accept(shuttle));

        ConfigurationSnapshotManager.saveSnapshotToFile(configNodes, DEFAULT_SNAPSHOT_FILE);

        LOG.info("Snapshot saved to: " + DEFAULT_SNAPSHOT_FILE.toAbsolutePath());
    }

    /**
     * List directory contents for a resource folder. Not recursive. Works for regular files and also JARs.
     */
    private static Stream<Arguments> getSnapshots() throws IOException {
        Enumeration<URL> resources = ConfigurationSnapshotManager.class.getClassLoader().getResources(SNAPSHOTS_RESOURCE_LOCATION);
        URL dirUrl = resources.nextElement();
        if (dirUrl == null) {
            return Stream.empty();
        }
        if ("file".equals(dirUrl.getProtocol())) {
            Path dirPath = Path.of(dirUrl.getPath());
            return Files.list(dirPath)
                    .filter(Files::isRegularFile)
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .filter(p -> p.endsWith(".bin"))
                    .map(Arguments::of);
        } else {
            throw new UnsupportedOperationException("Unsupported protocol: " + dirUrl.getProtocol());
        }
    }
}
