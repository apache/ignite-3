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

package org.apache.ignite.app;

import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.app.EnvironmentDefaultValueProvider;
import org.apache.ignite.network.NetworkAddress;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.TypeConversionException;

/**
 * The main entry point for running new Ignite node. Configuration values can be overridden using environment variables
 * or command-line arguments. Base configuration is either empty, or taken from the {@code --config-path}. Then,
 * if an environment variable with the pattern {@code IGNITE_VAR_NAME} (where VAR_NAME corresponds to {@code --var-name} command line
 * argument) is set, it overrides the value from the config. And last, if the {@code --var-name} command line argument is passed, it
 * overrides any other values.
 */
@Command(name = "runner")
public class IgniteRunner implements Callable<CompletableFuture<Ignite>> {
    @Option(names = {"--config-path"}, description = "Path to node configuration file in HOCON format.", required = true)
    private Path configPath;

    @Option(names = {"--work-dir"}, description = "Path to node working directory.", required = true)
    private Path workDir;

    @Option(names = {"--node-name"}, description = "Node name.", required = true)
    private String nodeName;

    @Override
    public CompletableFuture<Ignite> call() throws Exception {
        // If config path is specified and there are no overrides then pass it directly.
        return IgnitionManager.start(nodeName, configPath.toAbsolutePath(), workDir, null);
    }

    /**
     * Starts a new Ignite node.
     *
     * @param args CLI args to start a new node.
     * @return New Ignite node.
     */
    public static CompletableFuture<Ignite> start(String... args) {
        CommandLine commandLine = new CommandLine(new IgniteRunner());
        commandLine.setDefaultValueProvider(new EnvironmentDefaultValueProvider());
        commandLine.registerConverter(NetworkAddress.class, value -> {
            try {
                return NetworkAddress.from(value);
            } catch (IllegalArgumentException e) {
                throw new TypeConversionException(e.getMessage());
            }
        });
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
        try {
            start(args).get();
        } catch (ExecutionException | InterruptedException e) {
            System.out.println("Error when starting the node: " + e.getMessage());

            e.printStackTrace(System.out);

            System.exit(1);
        }
    }
}
