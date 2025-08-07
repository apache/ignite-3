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

import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import jdk.internal.misc.Signal;
import jdk.internal.misc.Signal.Handler;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.NodeConfig;
import org.apache.ignite.internal.app.config.NodeConfigFactory;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * The main entry point for running new Ignite node. Command-line arguments can be provided using environment variables
 * //TODO {@code IGNITE_CONFIG_PATH},
 * {@code IGNITE_WORK_DIR} and {@code IGNITE_NODE_NAME} for {@code --config-path}, {@code --work-dir} and
 * {@code --node-name} command line arguments respectively.
 */
@Command(name = "runner")
public class IgniteRunner implements Callable<IgniteServer> {
    @ArgGroup(multiplicity = "1")
    private ConfigOption config;

    @Option(names = "--work-dir", description = "Path to node working directory.", required = true)
    private Path workDir;

    @Option(names = "--node-name", description = "Node name.", required = true)
    private String nodeName;

    @Override
    public IgniteServer call() throws Exception {
        return IgniteServer.start(nodeName, config.configPath(), workDir);
    }

    private static class ConfigOption {
        /**
         * Node URL option.
         */
        @Option(names = "--config-path", description = "Path to node configuration file in HOCON format.", required = true)
        private Path configPath;

        /**
         * Node name option.
         */
        @Option(names = "--bootstrap-config", description = "Node bootstrap configuration in HOCON format.", required = true)
        private String bootstrapConfig;

        public NodeConfig configPath() {
            if (configPath != null) {
                return NodeConfigFactory.fromFile(configPath);
            }

            return NodeConfigFactory.fromBootstrap(bootstrapConfig);
        }
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
