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

package org.apache.ignite.internal;

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.testframework.TestIgnitionManager.DEFAULT_CONFIG_NAME;
import static org.apache.ignite.internal.testframework.TestIgnitionManager.writeConfigurationFile;
import static org.apache.ignite.internal.testframework.TestIgnitionManager.writeConfigurationFileApplyingTestDefaults;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.app.IgniteRunner;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Represents the Ignite node running in the external process.
 */
public class RunnerNode {
    private static final Map<String, Map<String, String>> DEFAULTS_PER_VERSION = readTestDefaultsPerVersion();

    private final Process process;

    private RunnerNode(Process process) {
        this.process = process;
    }

    /**
     * Starts the Ignite in the external process.
     *
     * @param javaHome Path to the Java to run the node with.
     * @param argFile Java arguments file.
     * @param igniteVersion Version of the Ignite. Used to get the configuration defaults.
     * @param clusterConfiguration Test cluster configuration.
     * @param nodesCount Overall number of nodes.
     * @param nodeIndex Current node index.
     * @return Instance of the control object.
     * @throws IOException If an I/O exception occurs.
     */
    public static RunnerNode startNode(
            File javaHome,
            File argFile,
            String igniteVersion,
            ClusterConfiguration clusterConfiguration,
            int nodesCount,
            int nodeIndex
    ) throws IOException {
        String nodeName = clusterConfiguration.nodeNamingStrategy().nodeName(clusterConfiguration, nodeIndex);
        Path workDir = clusterConfiguration.workDir().resolve(clusterConfiguration.clusterName()).resolve(nodeName);
        String configStr = formatConfig(clusterConfiguration, nodeIndex, nodesCount);

        Files.createDirectories(workDir);
        Path configPath = workDir.resolve(DEFAULT_CONFIG_NAME);

        boolean useTestDefaults = true;
        if (useTestDefaults) {
            writeConfigurationFileApplyingTestDefaults(configStr, configPath, DEFAULTS_PER_VERSION.get(igniteVersion));
        } else {
            writeConfigurationFile(configStr, configPath);
        }

        Process process = executeRunner(javaHome, argFile, configPath, workDir, nodeName);
        return new RunnerNode(process);
    }

    /**
     * Stops the node by killing the process.
     */
    public void stop() {
        process.destroy();
    }

    private static Map<String, Map<String, String>> readTestDefaultsPerVersion() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode node = mapper.readTree(RunnerNode.class.getResource("/versions.json"));

            Map<String, Map<String, String>> result = new HashMap<>();

            node.forEach(entry -> {
                entry.get("versions").forEach(version -> {
                    JsonNode configOverrides = version.get("configOverrides");
                    if (configOverrides != null) {
                        Map<String, String> overrides = configOverrides.propertyStream().collect(Collectors.toMap(
                                Entry::getKey,
                                override -> override.getValue().asText()
                        ));
                        result.put(version.get("version").textValue(), overrides);
                    }
                });
            });
            return result;
        } catch (IOException e) {
            Loggers.forClass(RunnerNode.class).error("Failed to read versions.json", e);
            return Map.of();
        }
    }

    private static String seedAddressesString(ClusterConfiguration clusterConfiguration, int seedsCount) {
        return IntStream.range(0, seedsCount)
                .map(nodeIndex -> clusterConfiguration.basePort() + nodeIndex)
                .mapToObj(port -> "\"localhost:" + port + '\"')
                .collect(joining(", "));
    }

    private static String formatConfig(ClusterConfiguration clusterConfiguration, int nodeIndex, int nodesCount) {
        return IgniteStringFormatter.format(
                clusterConfiguration.defaultNodeBootstrapConfigTemplate(),
                clusterConfiguration.basePort() + nodeIndex,
                seedAddressesString(clusterConfiguration, nodesCount),
                clusterConfiguration.baseClientPort() + nodeIndex,
                clusterConfiguration.baseHttpPort() + nodeIndex,
                clusterConfiguration.baseHttpsPort() + nodeIndex
        );
    }

    @SuppressWarnings("UseOfProcessBuilder")
    private static Process executeRunner(
            File javaHome,
            File classPathFile,
            Path configPath,
            Path workDir,
            String nodeName
    ) throws IOException {
        ProcessBuilder pb = new ProcessBuilder(
                javaHome.toPath().resolve("bin").resolve("java").toString(),
                "@" + classPathFile,
                IgniteRunner.class.getName(),
                "--node-name", nodeName,
                "--work-dir", workDir.toString(),
                "--config-path", configPath.toString()
        );
        pb.inheritIO();
        return pb.start();
    }
}
