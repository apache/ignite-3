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

import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.internal.cli.call.cluster.ClusterInitCall;
import org.apache.ignite.internal.cli.call.cluster.ClusterInitCallFactory;
import org.apache.ignite.internal.cli.call.cluster.ClusterInitCallInput;
import org.apache.ignite.internal.cli.call.cluster.status.ClusterStatusCall;
import org.apache.ignite.internal.cli.call.cluster.topology.LogicalTopologyCall;
import org.apache.ignite.internal.cli.call.cluster.topology.PhysicalTopologyCall;
import org.apache.ignite.internal.cli.call.cluster.unit.ClusterListUnitCall;
import org.apache.ignite.internal.cli.call.cluster.unit.DeployUnitCall;
import org.apache.ignite.internal.cli.call.cluster.unit.DeployUnitCallFactory;
import org.apache.ignite.internal.cli.call.cluster.unit.DeployUnitCallInput;
import org.apache.ignite.internal.cli.call.cluster.unit.UndeployUnitCall;
import org.apache.ignite.internal.cli.call.cluster.unit.UndeployUnitCallInput;
import org.apache.ignite.internal.cli.call.configuration.ClusterConfigShowCall;
import org.apache.ignite.internal.cli.call.configuration.ClusterConfigShowCallInput;
import org.apache.ignite.internal.cli.call.configuration.ClusterConfigUpdateCall;
import org.apache.ignite.internal.cli.call.configuration.ClusterConfigUpdateCallInput;
import org.apache.ignite.internal.cli.call.configuration.NodeConfigShowCall;
import org.apache.ignite.internal.cli.call.configuration.NodeConfigShowCallInput;
import org.apache.ignite.internal.cli.call.configuration.NodeConfigUpdateCall;
import org.apache.ignite.internal.cli.call.configuration.NodeConfigUpdateCallInput;
import org.apache.ignite.internal.cli.call.node.status.NodeStatusCall;
import org.apache.ignite.internal.cli.call.node.unit.NodeListUnitCall;
import org.apache.ignite.internal.cli.call.recovery.reset.ResetPartitionsCall;
import org.apache.ignite.internal.cli.call.recovery.reset.ResetPartitionsCallInput;
import org.apache.ignite.internal.cli.call.recovery.restart.RestartPartitionsCall;
import org.apache.ignite.internal.cli.call.recovery.restart.RestartPartitionsCallInput;
import org.apache.ignite.internal.cli.call.recovery.states.PartitionStatesCall;
import org.apache.ignite.internal.cli.call.recovery.states.PartitionStatesCallInput;
import org.apache.ignite.internal.cli.call.unit.ListUnitCallInput;
import org.apache.ignite.internal.cli.core.call.AsyncCall;
import org.apache.ignite.internal.cli.core.call.AsyncCallFactory;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallInput;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test for --profile override for --url options.
 */
@MicronautTest(resolveParameters = false)
@ExtendWith(WorkDirectoryExtension.class)
public class ProfileMixinTest extends CliCommandTestBase {
    /**
     * Cluster URL from default profile in integration_tests.ini.
     */
    private static final String DEFAULT_URL = "http://localhost:10300";

    /**
     * Cluster URL from test profile in integration_tests.ini.
     */
    private static final String URL_FROM_PROFILE = "http://localhost:10301";

    /**
     * Cluster URL override from command line.
     */
    private static final String URL_FROM_CMD = "http://localhost:10302";

    @WorkDirectory
    private static Path WORK_DIR;

    private static String TEMP_FILE_PATH;

    @BeforeAll
    public static void createTempFile() throws IOException {
        TEMP_FILE_PATH = Files.createFile(WORK_DIR.resolve("temp.txt")).toString();
    }

    @ParameterizedTest
    @DisplayName("Should take URL from default profile")
    @MethodSource("allCallsProvider")
    <IT extends CallInput, OT, T extends Call<IT, OT>> void defaultUrl(
            String command,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> urlSupplier
    ) {
        checkParameters(command, callClass, callInputClass, urlSupplier, "", DEFAULT_URL);
    }

    @ParameterizedTest
    @DisplayName("Should take URL from default profile")
    @MethodSource("allAsyncCallsProvider")
    <IT extends CallInput, OT, T extends AsyncCall<IT, OT>, FT extends AsyncCallFactory<IT, OT>> void defaultUrlAsync(
            String command,
            Class<FT> callFactoryClass,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> urlSupplier
    ) {
        checkParametersAsync(command, callFactoryClass, callClass, callInputClass, urlSupplier, "", DEFAULT_URL);
    }

    @ParameterizedTest
    @DisplayName("Should take URL from specified profile")
    @MethodSource("allCallsProvider")
    <IT extends CallInput, OT, T extends Call<IT, OT>> void profileUrl(
            String command,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> urlSupplier
    ) {
        checkParameters(command, callClass, callInputClass, urlSupplier, "--profile test", URL_FROM_PROFILE);
    }

    @ParameterizedTest
    @DisplayName("Should take URL from specified profile")
    @MethodSource("allAsyncCallsProvider")
    <IT extends CallInput, OT, T extends AsyncCall<IT, OT>, FT extends AsyncCallFactory<IT, OT>> void profileUrlAsync(
            String command,
            Class<FT> callFactoryClass,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> urlSupplier
    ) {
        checkParametersAsync(command, callFactoryClass, callClass, callInputClass, urlSupplier, "--profile test", URL_FROM_PROFILE);
    }

    @ParameterizedTest
    @DisplayName("Should take node URL from command line")
    @MethodSource("nodeCallsProvider")
    <IT extends CallInput, OT, T extends Call<IT, OT>> void commandNodeUrl(
            String command,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> urlSupplier
    ) {
        checkParameters(command, callClass, callInputClass, urlSupplier, "--url " + URL_FROM_CMD, URL_FROM_CMD);
    }

    @ParameterizedTest
    @DisplayName("Should take cluster endpoint URL from command line")
    @MethodSource("clusterCallsProvider")
    <IT extends CallInput, OT, T extends Call<IT, OT>> void commandClusterUrl(
            String command,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> urlSupplier
    ) {
        checkParameters(command, callClass, callInputClass, urlSupplier, "--url " + URL_FROM_CMD, URL_FROM_CMD);
    }

    @ParameterizedTest
    @DisplayName("Should take cluster endpoint URL from command line")
    @MethodSource("clusterAsyncCallsProvider")
    <IT extends CallInput, OT, T extends AsyncCall<IT, OT>, FT extends AsyncCallFactory<IT, OT>> void commandClusterUrlAsync(
            String command,
            Class<FT> callFactoryClass,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> urlSupplier
    ) {
        checkParametersAsync(command, callFactoryClass, callClass, callInputClass, urlSupplier, "--url " + URL_FROM_CMD, URL_FROM_CMD);
    }

    @ParameterizedTest
    @DisplayName("Node URL from command line should override specified profile")
    @MethodSource("nodeCallsProvider")
    <IT extends CallInput, OT, T extends Call<IT, OT>> void commandNodeUrlOverridesProfile(
            String command,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> urlSupplier
    ) {
        checkParameters(command, callClass, callInputClass, urlSupplier, "--profile test --url " + URL_FROM_CMD, URL_FROM_CMD);
    }

    @ParameterizedTest
    @DisplayName("Cluster endpoint URL from command line should override specified profile")
    @MethodSource("clusterCallsProvider")
    <IT extends CallInput, OT, T extends Call<IT, OT>> void commandClusterUrlOverridesProfile(
            String command,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> urlSupplier
    ) {
        checkParameters(command, callClass, callInputClass, urlSupplier, "--profile test --url " + URL_FROM_CMD, URL_FROM_CMD);
    }

    @ParameterizedTest
    @DisplayName("Cluster endpoint URL from command line should override specified profile")
    @MethodSource("clusterAsyncCallsProvider")
    <IT extends CallInput, OT, T extends AsyncCall<IT, OT>, FT extends AsyncCallFactory<IT, OT>>
            void commandClusterUrlOverridesProfileAsync(
            String command,
            Class<FT> callFactoryClass,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> urlSupplier
    ) {
        checkParametersAsync(command, callFactoryClass, callClass, callInputClass, urlSupplier, "--profile test --url " + URL_FROM_CMD,
                URL_FROM_CMD);
    }

    private static Stream<Arguments> nodeCallsProvider() {
        return Stream.of(
                arguments(
                        "node config show",
                        NodeConfigShowCall.class,
                        NodeConfigShowCallInput.class,
                        (Function<NodeConfigShowCallInput, String>) NodeConfigShowCallInput::getNodeUrl
                ),
                arguments(
                        "node config update config",
                        NodeConfigUpdateCall.class,
                        NodeConfigUpdateCallInput.class,
                        (Function<NodeConfigUpdateCallInput, String>) NodeConfigUpdateCallInput::getNodeUrl
                ),
                arguments(
                        "node status",
                        NodeStatusCall.class,
                        UrlCallInput.class,
                        (Function<UrlCallInput, String>) UrlCallInput::getUrl
                ),
                arguments(
                        "node unit list",
                        NodeListUnitCall.class,
                        ListUnitCallInput.class,
                        (Function<ListUnitCallInput, String>) ListUnitCallInput::url
                )
        );
    }

    private static Stream<Arguments> clusterCallsProvider() {
        return Stream.of(
                arguments(
                        "cluster config show",
                        ClusterConfigShowCall.class,
                        ClusterConfigShowCallInput.class,
                        (Function<ClusterConfigShowCallInput, String>) ClusterConfigShowCallInput::getClusterUrl
                ),
                arguments(
                        "cluster config update config",
                        ClusterConfigUpdateCall.class,
                        ClusterConfigUpdateCallInput.class,
                        (Function<ClusterConfigUpdateCallInput, String>) ClusterConfigUpdateCallInput::getClusterUrl
                ),
                arguments(
                        "cluster topology physical",
                        PhysicalTopologyCall.class,
                        UrlCallInput.class,
                        (Function<UrlCallInput, String>) UrlCallInput::getUrl
                ),
                arguments(
                        "cluster topology logical",
                        LogicalTopologyCall.class,
                        UrlCallInput.class,
                        (Function<UrlCallInput, String>) UrlCallInput::getUrl
                ),
                arguments(
                        "cluster status",
                        ClusterStatusCall.class,
                        UrlCallInput.class,
                        (Function<UrlCallInput, String>) UrlCallInput::getUrl
                ),
                arguments(
                        "cluster unit list",
                        ClusterListUnitCall.class,
                        ListUnitCallInput.class,
                        (Function<ListUnitCallInput, String>) ListUnitCallInput::url
                ),
                arguments(
                        "cluster unit undeploy id --version=1.0.0",
                        UndeployUnitCall.class,
                        UndeployUnitCallInput.class,
                        (Function<UndeployUnitCallInput, String>) UndeployUnitCallInput::clusterUrl
                ),
                arguments(
                        "recovery partitions states --global",
                        PartitionStatesCall.class,
                        PartitionStatesCallInput.class,
                        (Function<PartitionStatesCallInput, String>) PartitionStatesCallInput::clusterUrl
                ),
                arguments(
                        "recovery partitions reset --table test --zone test",
                        ResetPartitionsCall.class,
                        ResetPartitionsCallInput.class,
                        (Function<ResetPartitionsCallInput, String>) ResetPartitionsCallInput::clusterUrl
                ),
                arguments(
                        "recovery partitions restart --table test --zone test",
                        RestartPartitionsCall.class,
                        RestartPartitionsCallInput.class,
                        (Function<RestartPartitionsCallInput, String>) RestartPartitionsCallInput::clusterUrl
                )
        );
    }

    private static Stream<Arguments> clusterAsyncCallsProvider() {
        return Stream.of(
                arguments(
                        "cluster init --name cluster --metastorage-group node",
                        ClusterInitCallFactory.class,
                        ClusterInitCall.class,
                        ClusterInitCallInput.class,
                        (Function<ClusterInitCallInput, String>) ClusterInitCallInput::getClusterUrl
                ),
                 arguments(
                         "cluster unit deploy foo --version=1 --path=" + TEMP_FILE_PATH,
                         DeployUnitCallFactory.class,
                         DeployUnitCall.class,
                         DeployUnitCallInput.class,
                         (Function<DeployUnitCallInput, String>) DeployUnitCallInput::clusterUrl
                 )
        );
    }

    private static Stream<Arguments> allCallsProvider() {
        return Stream.concat(nodeCallsProvider(), clusterCallsProvider());
    }

    private static Stream<Arguments> allAsyncCallsProvider() {
        return clusterAsyncCallsProvider();
    }

    @Override
    protected Class<?> getCommandClass() {
        return TopLevelCliCommand.class;
    }
}
