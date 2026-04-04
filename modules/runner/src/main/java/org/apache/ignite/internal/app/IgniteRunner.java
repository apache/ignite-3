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

package org.apache.ignite.internal.app;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.LogManager;
import jdk.internal.misc.Signal;
import jdk.internal.misc.Signal.Handler;
import org.apache.ignite.IgniteServer;
import org.jetbrains.annotations.Nullable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * The main entry point for running new Ignite node. Command-line arguments can be provided using environment variables
 * {@code IGNITE_CONFIG_PATH}, {@code IGNITE_WORK_DIR} and {@code IGNITE_NODE_NAME} for {@code --config-path}, {@code --work-dir} and
 * {@code --node-name} command line arguments respectively.
 */
@Command(name = "runner")
public class IgniteRunner implements Callable<IgniteServer> {
    @Option(names = "--config-path", description = "Path to node configuration file in HOCON format.", required = true)
    private Path configPath;

    @Option(names = "--work-dir", description = "Path to node working directory.", required = true)
    private Path workDir;

    @Option(names = "--node-name", description = "Node name.", required = true)
    private String nodeName;

    @Option(names = "--log-dir", description = "Path to log directory.")
    private Path logDir;

    /** Default log file name used in distribution properties files. */
    private static final String DEFAULT_LOG_FILE = "ignite3db-%g.log";

    /** Default metrics log file name used in distribution properties files. */
    private static final String DEFAULT_METRICS_LOG_FILE = "ignite3db-metrics-%g.log";

    @Override
    public IgniteServer call() throws Exception {
        updateLoggingConfiguration();

        return IgniteServer.start(nodeName, configPath.toAbsolutePath(), workDir);
    }

    /**
     * Updates JUL logging configuration to include the node name in log file paths.
     * Only modifies patterns that match the default distribution values.
     * If {@code --log-dir} is specified, the directory part is also replaced.
     */
    private void updateLoggingConfiguration() {
        String julConfigFile = System.getProperty("java.util.logging.config.file");
        if (julConfigFile == null) {
            return;
        }

        Path configFilePath = Path.of(julConfigFile);
        if (!Files.exists(configFilePath)) {
            return;
        }

        try (InputStream is = Files.newInputStream(configFilePath)) {
            // We re-read the same file that LogManager already loaded, so oldVal and newVal are always identical.
            // The BiFunction only needs to rewrite the matched pattern keys; everything else stays unchanged.
            LogManager.getLogManager().updateConfiguration(is, key -> {
                if (key.equals("java.util.logging.FileHandler.pattern")) {
                    return (oldVal, newVal) -> insertNodeNameInTheLogFilePattern(oldVal, DEFAULT_LOG_FILE);
                }

                if (key.endsWith(".LogPushFileHandler.pattern")) {
                    return (oldVal, newVal) -> insertNodeNameInTheLogFilePattern(oldVal, DEFAULT_METRICS_LOG_FILE);
                }

                return (oldVal, newVal) -> oldVal;
            });
        } catch (IOException ignored) {
            // Keep the original configuration if we can't update it.
        }
    }

    /**
     * Rewrites a log file pattern if it ends with the expected default file name.
     * Inserts the node name into the file name and optionally replaces the directory.
     *
     * @param currentPattern Current pattern value from the properties file.
     * @param defaultFileName Expected default file name (e.g. {@code ignite3db-%g.log}).
     * @return Rewritten pattern, or the original if it doesn't match the default.
     */
    private @Nullable String insertNodeNameInTheLogFilePattern(@Nullable String currentPattern, String defaultFileName) {
        if (currentPattern == null || !currentPattern.endsWith(defaultFileName)) {
            return currentPattern;
        }

        String dir = currentPattern.substring(0, currentPattern.length() - defaultFileName.length());
        if (logDir != null) {
            dir = logDir.toAbsolutePath() + "/";
        }

        // Insert node name before the extension: ignite3db-%g.log -> ignite3db-<nodeName>-%g.log
        String nameWithoutExt = defaultFileName.substring(0, defaultFileName.indexOf("-%g.log"));
        return dir + nameWithoutExt + "-" + nodeName + "-%g.log";
    }

    /**
     * Starts a new Ignite node.
     *
     * @param args CLI args to start a new node.
     * @return New Ignite node.
     */
    public static IgniteServer start(String... args) {
        CommandLine commandLine = new CommandLine(new IgniteRunner());
        commandLine.setDefaultValueProvider(new EnvironmentDefaultValueProvider());
        int exitCode = commandLine.execute(args);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
        return commandLine.getExecutionResult();
    }

    /**
     * Main method for running a new Ignite node.
     *
     * @param args CLI args to start a new node.
     */
    public static void main(String[] args) {
        IgniteServer server = start(args);
        AtomicBoolean shutdown = new AtomicBoolean(false);

        Handler handler = sig -> {
            try {
                System.out.println("Ignite node shutting down...");
                shutdown.set(true);
                server.shutdown();
            } catch (Throwable t) {
                System.out.println("Failed to shutdown: " + t.getMessage());

                t.printStackTrace(System.out);
            }

            // Copy-paste from default JVM signal handler java.lang.Terminator#setup.
            System.exit(sig.getNumber() + 0200);
        };

        Signal.handle(new Signal("INT"), handler);
        Signal.handle(new Signal("TERM"), handler);

        try {
            server.waitForInitAsync().get();
        } catch (ExecutionException | InterruptedException e) {
            if (!shutdown.get()) {
                System.out.println("Error when starting the node: " + e.getMessage());

                e.printStackTrace(System.out);

                System.exit(1);
            }
        }
    }
}
