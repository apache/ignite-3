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

package org.apache.ignite.internal.cli.commands;

import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_CONFIG_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PASSWORD_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_CMG_NODES_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_METASTORAGE_REPLICATION_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_NODE_NAMES_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_PARTITION_GLOBAL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_PARTITION_LOCAL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.USERNAME_OPTION;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createEmptyWithCurrentProfileConfig;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createIntegrationTestsConfig;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.micronaut.configuration.picocli.MicronautFactory;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.Set;
import org.apache.ignite.internal.cli.call.connect.ConnectCallInput;
import org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerProvider;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlMixin;
import org.apache.ignite.internal.cli.commands.cluster.init.ClusterInitOptions;
import org.apache.ignite.internal.cli.commands.connect.ConnectOptions;
import org.apache.ignite.internal.cli.commands.node.NodeUrlMixin;
import org.apache.ignite.internal.cli.commands.recovery.cluster.reset.ResetClusterMixin;
import org.apache.ignite.internal.cli.commands.recovery.partitions.states.PartitionStatesMixin;
import org.apache.ignite.internal.cli.core.repl.registry.NodeNameRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@SuppressWarnings("unused")
@MicronautTest
class MixinTest {
    @Inject
    private ApplicationContext context;

    @Inject
    protected TestConfigManagerProvider configManagerProvider;

    @BeforeEach
    void resetConfigManager() {
        configManagerProvider.setConfigFile(createEmptyWithCurrentProfileConfig());
    }

    @Test
    void doubleInvocationNodeName() {
        NodeCommand command = new NodeCommand();
        CommandLine commandLine = new CommandLine(command, new MicronautFactory(context));

        String nodeName = "test";

        commandLine.parseArgs(NODE_NAME_OPTION, nodeName);
        assertThat(command.nodeUrl.getNodeUrl(), is("http://" + nodeName));

        commandLine.parseArgs();
        assertThat(command.nodeUrl.getNodeUrl(), is(nullValue()));
    }

    @Test
    void doubleInvocationNodeUrl() {
        NodeCommand command = new NodeCommand();
        CommandLine commandLine = new CommandLine(command, new MicronautFactory(context));

        String nodeUrl = "http://test";

        commandLine.parseArgs(NODE_URL_OPTION, nodeUrl);
        assertThat(command.nodeUrl.getNodeUrl(), is(nodeUrl));

        commandLine.parseArgs();
        assertThat(command.nodeUrl.getNodeUrl(), is(nullValue()));
    }

    @Test
    void doubleInvocationClusterUrl() {
        ClusterCommand command = new ClusterCommand();
        CommandLine commandLine = new CommandLine(command, new MicronautFactory(context));

        String clusterUrl = "http://test";

        commandLine.parseArgs(CLUSTER_URL_OPTION, clusterUrl);
        assertThat(command.clusterUrl.getClusterUrl(), is(clusterUrl));

        commandLine.parseArgs();
        assertThat(command.clusterUrl.getClusterUrl(), is(nullValue()));
    }

    @Test
    void clusterUrlDefaultValue() {
        ClusterCommand command = new ClusterCommand();
        CommandLine commandLine = new CommandLine(command, new MicronautFactory(context));

        configManagerProvider.setConfigFile(createIntegrationTestsConfig());

        // Default value is taken from the config
        commandLine.parseArgs();
        assertThat(command.clusterUrl.getClusterUrl(), is("http://localhost:10300"));
    }

    @Test
    void nodeUrlDefaultValue() {
        NodeCommand command = new NodeCommand();
        CommandLine commandLine = new CommandLine(command, new MicronautFactory(context));

        configManagerProvider.setConfigFile(createIntegrationTestsConfig());

        // Default value is taken from the config
        commandLine.parseArgs();
        assertThat(command.nodeUrl.getNodeUrl(), is("http://localhost:10300"));
    }

    @Test
    void doubleInvocationUnitList() {
        UnitCommand command = new UnitCommand();
        CommandLine commandLine = new CommandLine(command, new MicronautFactory(context));

        String version = "version";

        commandLine.parseArgs("--version", version, "unitId");
        assertThat(command.unitListOptions.toListUnitCallInput(null).version(), is(version));

        commandLine.parseArgs("unitId");
        assertThat(command.unitListOptions.toListUnitCallInput(null).version(), is(nullValue()));
    }

    @Test
    void doubleInvocationResetCluster() {
        ResetCommand command = new ResetCommand();
        CommandLine commandLine = new CommandLine(command, new MicronautFactory(context));

        String version = "version";

        commandLine.parseArgs(RECOVERY_METASTORAGE_REPLICATION_OPTION, "1");
        assertThat(command.resetCluster.metastorageReplicationFactor(), is(1));
        assertThat(command.resetCluster.cmgNodeNames(), is(nullValue()));

        commandLine.parseArgs(RECOVERY_CMG_NODES_OPTION, "node");
        assertThat(command.resetCluster.metastorageReplicationFactor(), is(nullValue()));
        assertThat(command.resetCluster.cmgNodeNames(), contains("node"));
    }

    @Test
    void doubleInvocationInit() {
        InitCommand command = new InitCommand();
        CommandLine commandLine = new CommandLine(command, new MicronautFactory(context));

        String config = "config";

        commandLine.parseArgs(CLUSTER_NAME_OPTION, "name", CLUSTER_CONFIG_OPTION, config);
        assertThat(command.initOptions.clusterConfiguration(), is(config));

        commandLine.parseArgs(CLUSTER_NAME_OPTION, "name");
        assertThat(command.initOptions.clusterConfiguration(), is(nullValue()));
    }

    @Test
    void doubleInvocationConnect() {
        ConnectCommand command = new ConnectCommand();
        CommandLine commandLine = new CommandLine(command, new MicronautFactory(context));

        String nodeUrl = "http://test";
        String username = "username";
        String password = "password";

        commandLine.parseArgs(nodeUrl, USERNAME_OPTION, username, PASSWORD_OPTION, password);
        ConnectCallInput callInput = command.connectOptions.buildCallInput();

        assertThat(callInput.url(), is(nodeUrl));
        assertThat(callInput.username(), is(username));
        assertThat(callInput.password(), is(password));

        commandLine.parseArgs(nodeUrl);
        callInput = command.connectOptions.buildCallInput();

        assertThat(callInput.url(), is(nodeUrl));
        assertThat(callInput.username(), is(nullValue()));
        assertThat(callInput.password(), is(nullValue()));
    }

    @Test
    void doubleInvocationPartitionStates() {
        PartitionStatesCommand command = new PartitionStatesCommand();
        CommandLine commandLine = new CommandLine(command, new MicronautFactory(context));

        String node = "node";

        commandLine.parseArgs(RECOVERY_PARTITION_LOCAL_OPTION, RECOVERY_NODE_NAMES_OPTION, "node");
        assertThat(command.partitionStates.nodeNames(), contains(node));
        assertThat(command.partitionStates.local(), is(true));

        commandLine.parseArgs(RECOVERY_PARTITION_GLOBAL_OPTION);
        assertThat(command.partitionStates.nodeNames(), is(empty()));
        assertThat(command.partitionStates.local(), is(false));
    }

    @Command
    private static class NodeCommand {
        @Mixin
        private NodeUrlMixin nodeUrl;
    }

    @Command
    private static class ClusterCommand {
        @Mixin
        private ClusterUrlMixin clusterUrl;
    }

    @Command
    private static class UnitCommand {
        @Mixin
        private UnitListOptionsMixin unitListOptions;
    }

    @Command
    private static class ResetCommand {
        @Mixin
        private ResetClusterMixin resetCluster;
    }

    @Command
    private static class InitCommand {
        @Mixin
        private ClusterInitOptions initOptions;
    }

    @Command
    private static class ConnectCommand {
        @Mixin
        private ConnectOptions connectOptions;
    }

    @Command
    private static class PartitionStatesCommand {
        @Mixin
        private PartitionStatesMixin partitionStates;
    }

    @Bean
    @Replaces(NodeNameRegistry.class)
    public static NodeNameRegistry nodeNameRegistry() {
        return new NodeNameRegistry() {
            @Override
            public Optional<String> nodeUrlByName(String nodeName) {
                return Optional.of("http://" + nodeName);
            }

            @Override
            public Set<String> names() {
                return Set.of();
            }

            @Override
            public Set<String> urls() {
                return Set.of();
            }
        };
    }
}
