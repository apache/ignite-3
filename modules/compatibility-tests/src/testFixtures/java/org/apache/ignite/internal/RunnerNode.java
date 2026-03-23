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

import static org.apache.ignite.internal.testframework.TestIgnitionManager.DEFAULT_CONFIG_NAME;
import static org.apache.ignite.internal.testframework.TestIgnitionManager.writeConfigurationFile;
import static org.apache.ignite.internal.testframework.TestIgnitionManager.writeConfigurationFileApplyingTestDefaults;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ignite.internal.IgniteVersions.Version;
import org.apache.ignite.internal.app.IgniteRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

/**
 * Represents the Ignite node running in the external process.
 */
public class RunnerNode {
    // The name of the log4j configuration file for the runner node. This needs to be different from the default log4j2.xml so that it
    // doesn't conflict with the default log4j2-test.xml configuration from the core test fixtures.
    private static final String RUNNER_LOG_CONFIG = "log4j2-runner.xml";

    // Marker for logging formatted logs from the runner process.
    private static final Marker RUNNER_MARKER = MarkerManager.getMarker("CompatRunner");

    private final Process process;

    private final String nodeName;

    private final Logger processLogger;

    private RunnerNode(Process process, String nodeName, Logger processLogger) {
        this.process = process;
        this.nodeName = nodeName;
        this.processLogger = processLogger;
    }

    /**
     * Starts the Ignite in the external process.
     *
     * @param javaHome Path to the Java to run the node with.
     * @param argFile Java arguments file.
     * @param igniteVersion Version of the Ignite. Used to get the configuration defaults.
     * @param clusterConfiguration Test cluster configuration.
     * @param nodesCount Overall number of nodes.
     * @param nodeName Node name.
     * @return Instance of the control object.
     * @throws IOException If an I/O exception occurs.
     */
    public static RunnerNode startNode(
            File javaHome,
            File argFile,
            String igniteVersion,
            ClusterConfiguration clusterConfiguration,
            String nodeConfig,
            int nodesCount,
            String nodeName
    ) throws IOException {
        Path workDir = clusterConfiguration.workDir().resolve(clusterConfiguration.clusterName()).resolve(nodeName);

        Files.createDirectories(workDir);
        Path configPath = workDir.resolve(DEFAULT_CONFIG_NAME);

        boolean useTestDefaults = true;
        if (useTestDefaults) {
            writeConfigurationFileApplyingTestDefaults(
                    nodeConfig,
                    configPath,
                    getDefaults(igniteVersion),
                    getStorageProfiles(igniteVersion)
            );
        } else {
            writeConfigurationFile(nodeConfig, configPath);
        }

        Process process = executeRunner(javaHome, argFile, configPath, workDir, nodeName);
        Logger processLogger = LogManager.getLogger(RunnerNode.class);
        processLogger.info("Process for node {} pid {} started", nodeName, process.pid());
        createStreamGrabber(process, processLogger, process::getInputStream, "input");
        createStreamGrabber(process, processLogger, process::getErrorStream, "error");
        return new RunnerNode(process, nodeName, processLogger);
    }

    private static Thread createStreamGrabber(
            Process process,
            Logger processLogger,
            Supplier<InputStream> streamSupplier,
            String grabberType
    ) {
        Thread streamGrabber = new Thread(
                new StreamGrabberTask(streamSupplier.get(), msg -> processLogger.info(RUNNER_MARKER, msg)),
                grabberThreadName(process.pid(), grabberType)
        );
        streamGrabber.setDaemon(true);
        streamGrabber.start();
        return streamGrabber;
    }

    private static String grabberThreadName(long pid, String grabberType) {
        return "pid_" + pid + "_" + grabberType + "_grabber";
    }

    /**
     * Stops the node by killing the process.
     */
    public void stop() {
        process.destroy();

        try {
            if (!process.waitFor(30, TimeUnit.SECONDS)) {
                processLogger.info("Process did not respond to destroy, destroying forcibly: {}", nodeName);
                process.destroyForcibly();

                if (!process.waitFor(30, TimeUnit.SECONDS)) {
                    processLogger.info("Process did not respond to forced destroy: {}", nodeName);

                    return;
                }
            }

            processLogger.info("Process for node {} pid {} stopped", nodeName, process.pid());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public String nodeName() {
        return nodeName;
    }

    private static Map<String, String> getDefaults(String version) {
        return IgniteVersions.INSTANCE.getOrDefault(version, Version::configOverrides, IgniteVersions::configOverrides);
    }

    private static Map<String, String> getStorageProfiles(String version) {
        return IgniteVersions.INSTANCE.getOrDefault(version, Version::storageProfilesOverrides, IgniteVersions::storageProfilesOverrides);
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

        Path logConfig = copyLogConfigToWorkDir(workDir);
        pb.environment().put("LOG4J_CONFIGURATION_FILE", logConfig.toString());

        return pb.start();
    }

    private static Path copyLogConfigToWorkDir(Path workDir) throws IOException {
        try (InputStream is = RunnerNode.class.getClassLoader().getResourceAsStream(RUNNER_LOG_CONFIG)) {
            if (is == null) {
                throw new IllegalStateException(RUNNER_LOG_CONFIG + " is not found on classpath");
            }
            Path logConfig = workDir.resolve(RUNNER_LOG_CONFIG);
            Files.copy(is, logConfig);
            return logConfig;
        }
    }
}
