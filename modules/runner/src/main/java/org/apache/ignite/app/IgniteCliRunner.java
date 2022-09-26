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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigRenderOptions;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.app.EnvironmentDefaultValueProvider;
import org.apache.ignite.network.NetworkAddress;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.TypeConversionException;

/**
 * The main entry point for running new Ignite node from CLI toolchain. Configuration values can be overridden using environment variables
 * or command-line arguments. Base configuration is either empty, or taken from the {@code --config-path} or {@code --config-string}. Then,
 * if an environment variable with the pattern {@code IGNITE_VAR_NAME} (where VAR_NAME corresponds to {@code --var-name} command line
 * argument) is set, it overrides the value from the config. And last, if the {@code --var-name} command line argument is passed, it
 * overrides any other values.
 */
@Command(name = "runner")
public class IgniteCliRunner implements Callable<CompletableFuture<Ignite>> {

    // Picocli doesn't apply default values to arg groups without initial value.
    @SuppressWarnings("FieldMayBeFinal")
    @ArgGroup
    private ConfigOptions configOptions = new ConfigOptions();

    private static class ConfigOptions {
        @Option(names = {"--config-path"}, description = "Path to node configuration file in HOCON format.")
        private Path configPath;

        @Option(names = {"--config-string"}, description = "Node configuration in HOCON format.")
        private String configString;
    }

    /** List of seed nodes. */
    @Option(names = {"--join"}, description = "Seed nodes.", split = ",")
    private NetworkAddress[] seedNodes;

    @Option(names = {"--work-dir"}, description = "Path to node working directory.", required = true)
    private Path workDir;

    @Option(names = {"--node-name"}, description = "Node name.", required = true)
    private String nodeName;

    @Override
    public CompletableFuture<Ignite> call() throws Exception {
        Path configPath = configOptions.configPath;
        // If config path is specified and there are no overrides then pass it directly.
        if (configPath != null && seedNodes == null) {
            return IgnitionManager.start(nodeName, configPath.toAbsolutePath(), workDir, null);
        }
        return IgnitionManager.start(nodeName, getConfigStr(), workDir);
    }

    private String getConfigStr() {
        Config configOptions = parseConfigOptions();
        Config configArgs = parseConfigArgs();
        // Override config from file or string with command-line arguments
        Config config = configArgs.withFallback(configOptions).resolve();
        return config.isEmpty() ? null : config.root().render(ConfigRenderOptions.concise().setJson(false));
    }

    private Config parseConfigOptions() {
        Path configPath = configOptions.configPath;
        String configString = configOptions.configString;
        if (configPath != null) {
            return ConfigFactory.parseFile(configPath.toFile(), ConfigParseOptions.defaults().setAllowMissing(false));
        } else if (configString != null) {
            return ConfigFactory.parseString(configString);
        }
        return ConfigFactory.empty();
    }

    private Config parseConfigArgs() {
        Map<String, Object> configMap = new HashMap<>();
        if (seedNodes != null) {
            List<String> strings = Arrays.stream(seedNodes)
                    .map(NetworkAddress::toString)
                    .collect(Collectors.toList());
            configMap.put("network.nodeFinder.netClusterNodes", strings);
        }
        return ConfigFactory.parseMap(configMap);
    }

    /**
     * Starts a new Ignite node.
     *
     * @param args CLI args to start a new node.
     * @return New Ignite node.
     */
    public static CompletableFuture<Ignite> start(String... args) {
        CommandLine commandLine = new CommandLine(new IgniteCliRunner());
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
