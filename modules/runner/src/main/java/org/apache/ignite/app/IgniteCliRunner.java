/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static picocli.CommandLine.Model.CommandSpec;
import static picocli.CommandLine.Model.OptionSpec;
import static picocli.CommandLine.Model.PositionalParamSpec;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import picocli.CommandLine;
import picocli.CommandLine.Model.ArgGroupSpec;

/**
 * The main entry point for run new Ignite node from CLI toolchain.
 */
public class IgniteCliRunner {
    /**
     * Main method for running a new Ignite node.
     *
     * <p>Usage: IgniteCliRunner [--config=configPath] --work-dir=workDir nodeName
     *
     * @param args CLI args to start a new node.
     */
    public static void main(String[] args) {
        try {
            start(args).get();
        } catch (CommandLine.ParameterException e) {
            System.out.println(e.getMessage());

            e.getCommandLine().usage(System.out);

            System.exit(1);
        } catch (ExecutionException | InterruptedException e) {
            System.out.println("Error when starting the node: " + e.getMessage());

            e.printStackTrace(System.out);

            System.exit(1);
        }
    }

    /**
     * Starts a new Ignite node.
     *
     * @param args CLI args to start a new node.
     * @return New Ignite node.
     */
    public static CompletableFuture<Ignite> start(String... args) {
        CommandSpec spec = CommandSpec.create();

        OptionSpec configPath = OptionSpec
                .builder("--config-path")
                .paramLabel("configPath")
                .type(Path.class)
                .description("Path to node configuration file in HOCON format.")
                .build();

        OptionSpec configStr = OptionSpec
                .builder("--config-string")
                .paramLabel("configStr")
                .type(String.class)
                .description("Configuration in HOCON format.")
                .build();

        spec.addArgGroup(
                ArgGroupSpec
                        .builder()
                        .addArg(configPath)
                        .addArg(configStr)
                        .build()
        );

        spec.addOption(
                OptionSpec
                        .builder("--work-dir")
                        .paramLabel("workDir")
                        .type(Path.class)
                        .description("Path to node working directory.")
                        .required(true)
                        .build()
        );

        spec.addPositional(
                PositionalParamSpec
                        .builder()
                        .paramLabel("nodeName")
                        .type(String.class)
                        .description("Node name.")
                        .required(true)
                        .build()
        );

        var cmd = new CommandLine(spec);

        var pr = cmd.parseArgs(args);

        var parsedArgs = new Args(
                pr.matchedPositionalValue(0, null),
                pr.matchedOptionValue("--config", null),
                pr.matchedOptionValue("--configStr", null),
                pr.matchedOptionValue("--work-dir", null)
        );

        if (parsedArgs.config != null) {
            return IgnitionManager.start(parsedArgs.nodeName, parsedArgs.config.toAbsolutePath(), parsedArgs.nodeWorkDir, null);
        } else {
            return IgnitionManager.start(parsedArgs.nodeName, parsedArgs.configStr, parsedArgs.nodeWorkDir);
        }
    }

    /**
     * Simple value object with parsed CLI args of ignite runner.
     */
    private static class Args {
        /** Name of the node. */
        private final String nodeName;

        /** Path to config file. */
        private final Path config;

        /** Config string. */
        private final String configStr;

        /** Path to node work directory. */
        private final Path nodeWorkDir;

        /**
         * Creates new instance with parsed arguments.
         *
         * @param nodeName Name of the node.
         * @param config   Path to config file.
         */
        private Args(String nodeName, Path config, String configStr, Path nodeWorkDir) {
            this.nodeName = nodeName;
            this.config = config;
            this.configStr = configStr;
            this.nodeWorkDir = nodeWorkDir;
        }
    }
}
