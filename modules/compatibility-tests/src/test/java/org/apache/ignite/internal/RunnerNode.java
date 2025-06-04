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
import static org.apache.ignite.internal.testframework.TestIgnitionManager.DEFAULT_SCALECUBE_METADATA_TIMEOUT;
import static org.apache.ignite.internal.testframework.TestIgnitionManager.writeConfigurationFile;
import static org.apache.ignite.internal.testframework.TestIgnitionManager.writeConfigurationFileApplyingTestDefaults;
import static org.apache.ignite.internal.util.Constants.MiB;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.ignite.internal.app.IgniteRunner;
import org.apache.ignite.internal.lang.IgniteStringFormatter;

class RunnerNode {
    private static final Map<String, String> DEFAULT_NODE_CONFIG_3_0_0 = Map.of(
            "ignite.network.membership.scaleCube.metadataTimeout", Integer.toString(DEFAULT_SCALECUBE_METADATA_TIMEOUT),
            "ignite.storage.profiles.default_aipersist.engine", "aipersist",
            "ignite.storage.profiles.default_aipersist.size", Integer.toString(256 * MiB),
            "ignite.storage.profiles.default_aimem.engine", "aimem",
            "ignite.storage.profiles.default_aimem.initSize", Integer.toString(256 * MiB),
            "ignite.storage.profiles.default_aimem.maxSize", Integer.toString(256 * MiB),
            "ignite.storage.profiles.default.engine", "aipersist",
            "ignite.storage.profiles.default.size", Integer.toString(256 * MiB),
            "ignite.system.properties.aipersistThrottling", "disabled"
    );

    private static final Map<String, Map<String, String>> DEFAULTS_PER_VERSION = Map.of(
            "3.0.0", DEFAULT_NODE_CONFIG_3_0_0
    );

    private final Process process;

    private RunnerNode(Process process) {
        this.process = process;
    }

    public static RunnerNode startNode(
            File javaHome,
            File classPathFile,
            String igniteVersion,
            ClusterConfiguration clusterConfiguration,
            int nodesCount,
            int nodeIndex
    ) throws IOException, InterruptedException {
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

        Process process = executeRunner(javaHome, classPathFile, configPath, workDir, nodeName);
        return new RunnerNode(process);
    }

    public void stop() {
        process.destroy();
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
