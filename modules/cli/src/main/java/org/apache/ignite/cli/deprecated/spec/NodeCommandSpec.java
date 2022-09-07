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

package org.apache.ignite.cli.deprecated.spec;

import static org.apache.ignite.cli.core.style.AnsiStringSupport.ansi;

import com.jakewharton.fliptables.FlipTable;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import jakarta.inject.Inject;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.core.converters.NetworkAddressConverter;
import org.apache.ignite.cli.core.style.element.UiElements;
import org.apache.ignite.cli.deprecated.CliPathsConfigLoader;
import org.apache.ignite.cli.deprecated.IgniteCliException;
import org.apache.ignite.cli.deprecated.IgnitePaths;
import org.apache.ignite.cli.deprecated.builtins.node.NodeManager;
import org.apache.ignite.network.NetworkAddress;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Help.ColorScheme;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Commands for start/stop/list Ignite nodes on the current machine.
 */
@Command(
        name = "node",
        description = "Manages locally running Ignite nodes",
        subcommands = {
                NodeCommandSpec.StartNodeCommandSpec.class,
                NodeCommandSpec.StopNodeCommandSpec.class,
                NodeCommandSpec.NodesClasspathCommandSpec.class,
                NodeCommandSpec.ListNodesCommandSpec.class
        }
)
public class NodeCommandSpec {
    /**
     * Starts Ignite node command.
     */
    @Command(name = "start", description = "Starts an Ignite node locally")
    public static class StartNodeCommandSpec extends BaseCommand implements Callable<Integer> {
        /** Consistent id, which will be used by new node. */
        @Parameters(paramLabel = "name", description = "Name of the new node")
        public String nodeName;

        /** Loader for Ignite distributive paths. */
        @Inject
        private CliPathsConfigLoader cliPathsCfgLdr;

        /** Node manager. */
        @Inject
        private NodeManager nodeMgr;

        @ArgGroup(exclusive = false)
        private ConfigOptions configOptions;

        private static class ConfigOptions {
            @ArgGroup(exclusive = false)
            private ConfigArguments args;

            /** Path to node config. */
            @Option(names = {"-c", "--config"}, description = "Configuration file to start the node with")
            private Path configPath;
        }

        private static class ConfigArguments {
            @Option(names = {"-p", "--port"}, description = "Node port")
            private Integer port;

            @Option(names = {"-r", "--rest-port"}, description = "REST port")
            private Integer restPort;

            @Option(names = {"-j", "--join"}, description = "Seed nodes", split = ",", converter = NetworkAddressConverter.class)
            private NetworkAddress[] seedNodes;
        }

        /** {@inheritDoc} */
        @Override
        public Integer call() {
            IgnitePaths ignitePaths = cliPathsCfgLdr.loadIgnitePathsOrThrowError();

            PrintWriter out = spec.commandLine().getOut();
            ColorScheme cs = spec.commandLine().getColorScheme();

            NodeManager.RunningNode node = nodeMgr.start(
                    nodeName,
                    ignitePaths.nodesBaseWorkDir(),
                    ignitePaths.logDir,
                    ignitePaths.cliPidsDir(),
                    getConfigPath(),
                    getConfigStr(),
                    ignitePaths.serverJavaUtilLoggingPros(),
                    out);

            out.println(ansi(UiElements.done().represent()));

            out.println(String.format("[name: %s, pid: %d]", node.name, node.pid));

            out.println();
            out.println("Node is successfully started. To stop, type "
                    + cs.commandText("ignite node stop ") + cs.parameterText(node.name));

            return 0;
        }

        private Path getConfigPath() {
            return configOptions != null ? configOptions.configPath : null;
        }

        private String getConfigStr() {
            if (configOptions == null || configOptions.args == null) {
                return null;
            }
            Map<String, Object> configMap = new HashMap<>();
            if (configOptions.args.port != null) {
                configMap.put("network.port", configOptions.args.port);
            }
            if (configOptions.args.seedNodes != null) {
                List<String> strings = Arrays.stream(configOptions.args.seedNodes)
                        .map(NetworkAddress::toString)
                        .collect(Collectors.toList());
                configMap.put("network.nodeFinder.netClusterNodes", strings);
            }
            if (configOptions.args.restPort != null) {
                configMap.put("rest.port", configOptions.args.restPort);
            }
            Config config = ConfigFactory.parseMap(configMap);
            if (configOptions.configPath != null) {
                Config fallback = ConfigFactory.parseFile(configOptions.configPath.toFile());
                config = config.withFallback(fallback).resolve();
            }
            return config.root().render(ConfigRenderOptions.concise().setJson(false));
        }
    }

    /**
     * Command for stopping Ignite node on the current machine.
     */
    @Command(name = "stop", description = "Stops a locally running Ignite node.")
    public static class StopNodeCommandSpec extends BaseCommand implements Callable<Integer> {
        /** Node manager. */
        @Inject
        private NodeManager nodeMgr;

        /** Loader for Ignite distributive paths. */
        @Inject
        private CliPathsConfigLoader cliPathsCfgLdr;

        /** Consistent ids of nodes to stop. */
        @Parameters(
                arity = "1..*",
                paramLabel = "consistent-ids",
                description = "Consistent IDs of the nodes to stop (space separated list)"
        )
        private List<String> consistentIds;

        /** {@inheritDoc} */
        @Override
        public Integer call() {
            IgnitePaths ignitePaths = cliPathsCfgLdr.loadIgnitePathsOrThrowError();

            PrintWriter out = spec.commandLine().getOut();
            ColorScheme cs = spec.commandLine().getColorScheme();

            consistentIds.forEach(p -> {
                out.println("Stopping locally running node with consistent ID " + cs.parameterText(p) + "...");

                if (nodeMgr.stopWait(p, ignitePaths.cliPidsDir())) {
                    out.println(cs.text("@|bold,green Done|@"));
                } else {
                    out.println(cs.text("@|bold,red Failed|@"));
                }
            });
            return 0;
        }
    }

    /**
     * Command for listing the running nodes.
     */
    @Command(name = "list", description = "Shows the list of currently running local Ignite nodes.")
    public static class ListNodesCommandSpec extends BaseCommand implements Callable<Integer> {
        /** Node manager. */
        @Inject
        private NodeManager nodeMgr;

        /** Loader for Ignite distributive paths. */
        @Inject
        private CliPathsConfigLoader cliPathsCfgLdr;

        /** {@inheritDoc} */
        @Override
        public Integer call() {
            IgnitePaths paths = cliPathsCfgLdr.loadIgnitePathsOrThrowError();

            List<NodeManager.RunningNode> nodes = nodeMgr.getRunningNodes(paths.logDir, paths.cliPidsDir());

            PrintWriter out = spec.commandLine().getOut();
            ColorScheme cs = spec.commandLine().getColorScheme();

            if (nodes.isEmpty()) {
                out.println("There are no locally running nodes");
                out.println("use the " + cs.commandText("ignite node start")
                        + " command to start a new node");
            } else {
                String[] headers = {"consistent id", "pid", "log file"};
                String[][] content = nodes.stream().map(
                        node -> new String[]{
                                node.name,
                                String.valueOf(node.pid),
                                String.valueOf(node.logFile)
                        }
                ).toArray(String[][]::new);

                out.println(FlipTable.of(headers, content));

                out.println("Number of running nodes: " + cs.text("@|bold " + nodes.size() + "|@"));
            }
            return 0;
        }
    }

    /**
     * Command for reading the current classpath of Ignite nodes.
     */
    @Command(name = "classpath", description = "Shows the current classpath used by the Ignite nodes.")
    public static class NodesClasspathCommandSpec extends BaseCommand implements Callable<Integer> {
        /** Node manager. */
        @Inject
        private NodeManager nodeMgr;

        /** {@inheritDoc} */
        @Override
        public Integer call() {
            try {
                List<String> items = nodeMgr.classpathItems();

                PrintWriter out = spec.commandLine().getOut();

                out.println(Ansi.AUTO.string("@|bold Current Ignite node classpath:|@"));

                for (String item : items) {
                    out.println("    " + item);
                }
            } catch (IOException e) {
                throw new IgniteCliException("Can't get current classpath", e);
            }
            return 0;
        }
    }
}
