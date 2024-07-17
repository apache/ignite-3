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

package org.apache.ignite.internal.cli.core.repl.completer;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.commands.Options;
import org.apache.ignite.internal.cli.core.repl.completer.cli.CliConfigDynamicCompleterFactory;
import org.apache.ignite.internal.cli.core.repl.completer.cluster.ClusterUrlDynamicCompleterFactory;
import org.apache.ignite.internal.cli.core.repl.completer.filter.ExclusionsCompleterFilter;
import org.apache.ignite.internal.cli.core.repl.completer.hocon.ClusterConfigDynamicCompleterFactory;
import org.apache.ignite.internal.cli.core.repl.completer.hocon.NodeConfigDynamicCompleterFactory;
import org.apache.ignite.internal.cli.core.repl.completer.jdbc.JdbcUrlDynamicCompleterFactory;
import org.apache.ignite.internal.cli.core.repl.completer.metric.MetricSourceDynamicCompleterFactory;
import org.apache.ignite.internal.cli.core.repl.completer.node.NodeNameDynamicCompleterFactory;
import org.apache.ignite.internal.cli.core.repl.completer.path.FilePathCompleter;
import org.apache.ignite.internal.cli.core.repl.completer.unit.UnitIdDynamicCompleterFactory;
import org.apache.ignite.internal.cli.core.repl.completer.unit.UnitNodesCompleterFilter;
import org.apache.ignite.internal.cli.core.repl.completer.unit.UnitNodesDynamicCompleterFactory;
import org.apache.ignite.internal.cli.core.repl.completer.unit.UnitVersionsDynamicCompleterFactory;

/** Activation point that links commands with dynamic completers. */
@Singleton
public class DynamicCompleterActivationPoint {

    @Inject
    private NodeNameDynamicCompleterFactory nodeNameDynamicCompleterFactory;

    @Inject
    private ClusterConfigDynamicCompleterFactory clusterConfigDynamicCompleterFactory;

    @Inject
    private NodeConfigDynamicCompleterFactory nodeConfigDynamicCompleterFactory;

    @Inject
    private ClusterUrlDynamicCompleterFactory clusterUrlDynamicCompleterFactory;

    @Inject
    private JdbcUrlDynamicCompleterFactory jdbcUrlDynamicCompleterFactory;

    @Inject
    private UnitIdDynamicCompleterFactory unitIdDynamicCompleterFactory;

    @Inject
    private UnitVersionsDynamicCompleterFactory unitVersionsDynamicCompleterFactory;

    @Inject
    private UnitNodesDynamicCompleterFactory unitNodesDynamicCompleterFactory;

    @Inject
    private CliConfigDynamicCompleterFactory cliConfigDynamicCompleterFactory;

    @Inject
    private MetricSourceDynamicCompleterFactory metricSourceDynamicCompleterFactory;


    /**
     * Registers all dynamic completers in given {@link DynamicCompleterRegistry}.
     */
    public void activateDynamicCompleter(DynamicCompleterRegistry registry) {
        registry.register(
                CompleterConf.builder()
                        .command("cluster", "config", "show")
                        .command("cluster", "config", "update")
                        .singlePositionalParameter().build(),
                clusterConfigDynamicCompleterFactory
        );
        registry.register(
                CompleterConf.builder()
                        .command("node", "config", "show")
                        .singlePositionalParameter().build(),
                nodeConfigDynamicCompleterFactory
        );

        registry.register(
                CompleterConf.builder()
                        .command("node", "config", "update")
                        .singlePositionalParameter()
                        .filter(new ExclusionsCompleterFilter("compute", "raft"))
                        .build(),
                nodeConfigDynamicCompleterFactory
        );

        // exclusive option that disables other completers for node name
        registry.register(
                CompleterConf.builder()
                        .enableOptions(Options.NODE_NAME)
                        .exclusiveEnableOptions().build(),
                nodeNameDynamicCompleterFactory
        );
        registry.register(
                CompleterConf.builder()
                        .command("cluster", "init")
                        .enableOptions(Options.META_STORAGE_NODE_NAME, Options.CMG_NODE_NAME)
                        .build(),
                nodeNameDynamicCompleterFactory
        );

        registry.register(
                CompleterConf.builder()
                        .command("sql")
                        .enableOptions(Options.SCRIPT_FILE)
                        .exclusiveEnableOptions().build(),
                words -> new FilePathCompleter()
        );

        registry.register(
                CompleterConf.builder()
                        .command("sql")
                        .enableOptions(Options.JDBC_URL)
                        .exclusiveEnableOptions().build(),
                jdbcUrlDynamicCompleterFactory
        );

        registry.register(
                CompleterConf.builder()
                        .enableOptions(Options.CLUSTER_URL, Options.NODE_URL)
                        .exclusiveEnableOptions().build(),
                clusterUrlDynamicCompleterFactory
        );

        registry.register(
                CompleterConf.builder()
                        .command("cli", "config", "set")
                        .command("cli", "config", "get")
                        .command("cli", "config", "remove")
                        .singlePositionalParameter()
                        .build(),
                cliConfigDynamicCompleterFactory
        );

        registry.register(
                CompleterConf.builder()
                        .command("cluster", "unit", "deploy")
                        .enableOptions(Options.UNIT_PATH)
                        .exclusiveEnableOptions().build(),
                words -> new FilePathCompleter()
        );

        registry.register(
                CompleterConf.builder()
                        .command("cluster", "unit", "deploy")
                        .enableOptions(Options.UNIT_NODES)
                        .filter(new UnitNodesCompleterFilter())
                        .exclusiveEnableOptions().build(),
                unitNodesDynamicCompleterFactory
        );

        registry.register(
                CompleterConf.builder()
                        .command("cluster", "unit", "undeploy")
                        .command("cluster", "unit", "status")
                        .command("cluster", "unit", "list")
                        .command("node", "unit", "list")
                        .singlePositionalParameter()
                        .build(),
                unitIdDynamicCompleterFactory
        );

        registry.register(
                CompleterConf.builder()
                        .command("cluster", "unit", "undeploy")
                        .command("cluster", "unit", "list")
                        .command("node", "unit", "list")
                        .enableOptions(Options.UNIT_VERSION)
                        .exclusiveEnableOptions().build(),
                unitVersionsDynamicCompleterFactory
        );

        registry.register(
                CompleterConf.builder()
                        .command("node", "metric", "source", "enable")
                        .command("node", "metric", "source", "disable")
                        .singlePositionalParameter()
                        .build(),
                metricSourceDynamicCompleterFactory
        );
    }
}
