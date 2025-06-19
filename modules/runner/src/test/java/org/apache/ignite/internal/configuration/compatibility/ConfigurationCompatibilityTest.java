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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ServiceLoaderModulesProvider;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNodeSerializer;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigShuttle;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigurationTreeScanner;
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
public class ConfigurationCompatibilityTest {
    private static final String SNAPSHOT_DUMP_FILE = "./modules/runner/build/compatibility/configuration/actual_snapshot.bin";
    private static final String SNAPSHOTS_DIRECTORY = "./src/test/resources/";
    private static final byte[] IGNITE_MAGIC = "IGNZIP".getBytes();

    /** Return previously saved snapshot files. */
    public static Stream<Arguments> getSnapshots() throws IOException {
        Path baseDir = Path.of(SNAPSHOTS_DIRECTORY);

        assert Files.exists(baseDir) : "No snapshots found";

        return Files.walk(baseDir)
                .filter(Files::isRegularFile)
                .filter(p -> p.getFileName().toString().endsWith(".bin"))
                .map(p -> Arguments.of(p.getFileName().toString(), p));
    }

    /**
     * This test ensures that the current configuration can be serialized and deserialized correctly.
     */
    @Test
    void testConfigurationMetadataSerialization() throws IOException {
        // Load current configuration metadata.
        List<ConfigNode> expectedNodes = loadCurrentConfiguration();

        assertFalse(expectedNodes.isEmpty());

        // Serialize, then deserialize the metadata.
        IgniteUnsafeDataOutput output = new IgniteUnsafeDataOutput(1024);

        ConfigNodeSerializer.writeAll(expectedNodes, output);

        byte[] data = output.array();

        assertTrue(data.length > 0);

        List<ConfigNode> restoredNodes = ConfigNodeSerializer.readAll(new IgniteUnsafeDataInput(data));

        // Validate restored metadata.
        validateConfigurationMetadata(expectedNodes, restoredNodes);
    }

    /**
     * This test ensures that the current configuration metadata is compatible with the snapshots.
     */
    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("getSnapshots")
    void testConfigurationCompatibility(String testName, Path snapshotFile) throws IOException {
        List<ConfigNode> currentMetadata = loadCurrentConfiguration();
        List<ConfigNode> snapshotMetadata = loadConfigurationFromFile(snapshotFile);

        validateConfigurationMetadata(currentMetadata, snapshotMetadata);
    }

    /**
     * Validates snapshot is compatible with current metadata.
     * TODO: Implement configuration compatibility validation logic.
     */
    private static void validateConfigurationMetadata(List<ConfigNode> currentMetadata, List<ConfigNode> snapshotMetadata) {
        DumpingShuttle expectedState = new DumpingShuttle();
        DumpingShuttle actualState = new DumpingShuttle();

        currentMetadata.forEach(expectedState::visit);
        snapshotMetadata.forEach(actualState::visit);

        assertEquals(expectedState.toString(), actualState.toString());
    }

    private static List<ConfigNode> loadCurrentConfiguration() {
        ConfigurationModules modules = loadConfigurationModules(ConfigurationCompatibilityTest.class.getClassLoader());

        ConfigurationModule local = modules.local();
        Set<Class<?>> localRootClasses = local.rootKeys().stream().map(RootKey::schemaClass).collect(Collectors.toSet());
        Stream<Class<?>> localNodes = local.schemaExtensions().stream()
                .filter(ext -> localRootClasses.contains(ext.getSuperclass()));

        ConfigurationModule distributed = modules.distributed();
        Set<Class<?>> distributedRootClasses = distributed.rootKeys().stream().map(RootKey::schemaClass).collect(Collectors.toSet());
        Stream<Class<?>> distributedNodes = distributed.schemaExtensions().stream()
                .filter(ext -> distributedRootClasses.contains(ext.getSuperclass()));

        return Stream.concat(localNodes, distributedNodes)
                .sorted(Comparator.comparing(Class::getCanonicalName)) // Sort for test stability.
                .map(ext -> {
                    ConfigurationRoot declaredAnnotation = ext.getSuperclass().getDeclaredAnnotation(ConfigurationRoot.class);
                    String rootName = declaredAnnotation.rootName();
                    return ConfigurationTreeScanner.scanClass(rootName, ext, null);
                })
                .collect(Collectors.toList());
    }

    /**
     * Loads configuration from the given snapshot file.
     */
    private static List<ConfigNode> loadConfigurationFromFile(Path snapshotFile) throws IOException {
        assert Files.isRegularFile(snapshotFile);

        List<ConfigNode> restoredNodes;
        try (InputStream ifStream = Files.newInputStream(snapshotFile, StandardOpenOption.READ)) {
            byte[] magic = ifStream.readNBytes(6);

            assert Arrays.equals(IGNITE_MAGIC, magic);

            ZipInputStream zinStream = new ZipInputStream(ifStream);
            zinStream.getNextEntry(); // Read the first entry in the zip file.

            IgniteUnsafeDataInput input = new IgniteUnsafeDataInput();
            input.inputStream(zinStream);

            restoredNodes = ConfigNodeSerializer.readAll(input);

            assert zinStream.getNextEntry() == null : "More than one entry in the snapshot file";
        }
        return restoredNodes;
    }

    /**
     * Generates a snapshot of the current configuration metadata and saves it to a file.
     */
    public static void main(String[] args) throws IOException {
        List<ConfigNode> configNodes = loadCurrentConfiguration();

        ConfigShuttle shuttle = node -> System.out.println(node.toString());
        System.out.println("DUMP TREE:");
        configNodes.forEach(c -> c.accept(shuttle));

        Path file = Path.of(SNAPSHOT_DUMP_FILE).toAbsolutePath();

        Files.createDirectories(file.getParent());

        try (OutputStream foStream = Files.newOutputStream(file,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            foStream.write(IGNITE_MAGIC);

            ZipOutputStream zoutStream = new ZipOutputStream(foStream);
            zoutStream.putNextEntry(new ZipEntry(file.getFileName().toString()));

            IgniteUnsafeDataOutput output = new IgniteUnsafeDataOutput(1024);
            output.outputStream(zoutStream);

            ConfigNodeSerializer.writeAll(configNodes, output);

            zoutStream.finish();
            output.flush();
        }
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

    /** Configuration tree visitor that dumps tree state to string. */
    private static class DumpingShuttle implements ConfigShuttle {
        private final StringBuilder sb = new StringBuilder();

        @Override
        public void visit(ConfigNode node) {
            sb.append(node.toString());
        }

        @Override
        public String toString() {
            return sb.toString();
        }
    }
}
