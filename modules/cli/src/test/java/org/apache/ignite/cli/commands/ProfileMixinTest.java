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

package org.apache.ignite.cli.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micronaut.configuration.picocli.MicronautFactory;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.cli.call.cluster.ClusterInitCall;
import org.apache.ignite.cli.call.cluster.ClusterInitCallInput;
import org.apache.ignite.cli.call.cluster.topology.LogicalTopologyCall;
import org.apache.ignite.cli.call.cluster.topology.PhysicalTopologyCall;
import org.apache.ignite.cli.call.configuration.ClusterConfigShowCall;
import org.apache.ignite.cli.call.configuration.ClusterConfigShowCallInput;
import org.apache.ignite.cli.call.configuration.ClusterConfigUpdateCall;
import org.apache.ignite.cli.call.configuration.ClusterConfigUpdateCallInput;
import org.apache.ignite.cli.call.configuration.NodeConfigShowCall;
import org.apache.ignite.cli.call.configuration.NodeConfigShowCallInput;
import org.apache.ignite.cli.call.configuration.NodeConfigUpdateCall;
import org.apache.ignite.cli.call.configuration.NodeConfigUpdateCallInput;
import org.apache.ignite.cli.call.node.status.NodeStatusCall;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallInput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.core.call.UrlCallInput;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import picocli.CommandLine;

/**
 * Test for --profile override for --node-url and --cluster-endpoint-url options.
 */
@MicronautTest
public class ProfileMixinTest {
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

    @Inject
    private ApplicationContext ctx;

    @ParameterizedTest
    @DisplayName("Should take URL from default profile")
    @MethodSource("allCallsProvider")
    <IT extends CallInput, OT, T extends Call<IT, OT>> void defaultUrl(
            String command,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> urlSupplier) {
        T call = registerMockCall(callClass);
        execute(command);
        IT callInput = verifyCallInput(call, callInputClass);
        assertEquals(DEFAULT_URL, urlSupplier.apply(callInput));
    }

    @ParameterizedTest
    @DisplayName("Should take URL from specified profile")
    @MethodSource("allCallsProvider")
    <IT extends CallInput, OT, T extends Call<IT, OT>> void profileUrl(
            String command,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> urlSupplier) {
        T call = registerMockCall(callClass);
        execute(command + " --profile test");
        IT callInput = verifyCallInput(call, callInputClass);
        assertEquals(URL_FROM_PROFILE, urlSupplier.apply(callInput));
    }

    @ParameterizedTest
    @DisplayName("Should take node URL from command line")
    @MethodSource("nodeCallsProvider")
    <IT extends CallInput, OT, T extends Call<IT, OT>> void commandNodeUrl(
            String command,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> urlSupplier) {
        T call = registerMockCall(callClass);
        execute(command + " --node-url " + URL_FROM_CMD);
        IT callInput = verifyCallInput(call, callInputClass);
        assertEquals(URL_FROM_CMD, urlSupplier.apply(callInput));
    }

    @ParameterizedTest
    @DisplayName("Should take cluster endpoint URL from command line")
    @MethodSource("clusterCallsProvider")
    <IT extends CallInput, OT, T extends Call<IT, OT>> void commandClusterUrl(
            String command,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> urlSupplier) {
        T call = registerMockCall(callClass);
        execute(command + " --cluster-endpoint-url " + URL_FROM_CMD);
        IT callInput = verifyCallInput(call, callInputClass);
        assertEquals(URL_FROM_CMD, urlSupplier.apply(callInput));
    }

    @ParameterizedTest
    @DisplayName("Node URL from command line should override specified profile")
    @MethodSource("nodeCallsProvider")
    <IT extends CallInput, OT, T extends Call<IT, OT>> void commandNodeUrlOverridesProfile(
            String command,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> urlSupplier) {
        T call = registerMockCall(callClass);
        execute(command + " --profile test --node-url " + URL_FROM_CMD);
        IT callInput = verifyCallInput(call, callInputClass);
        assertEquals(URL_FROM_CMD, urlSupplier.apply(callInput));
    }

    @ParameterizedTest
    @DisplayName("Cluster endpoint URL from command line should override specified profile")
    @MethodSource("clusterCallsProvider")
    <IT extends CallInput, OT, T extends Call<IT, OT>> void commandClusterUrlOverridesProfile(
            String command,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> urlSupplier) {
        T call = registerMockCall(callClass);
        execute(command + " --profile test --cluster-endpoint-url " + URL_FROM_CMD);
        IT callInput = verifyCallInput(call, callInputClass);
        assertEquals(URL_FROM_CMD, urlSupplier.apply(callInput));
    }

    static Stream<Arguments> nodeCallsProvider() {
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
                )
        );
    }

    static Stream<Arguments> clusterCallsProvider() {
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
                        "cluster init --cluster-name cluster --meta-storage-node node",
                        ClusterInitCall.class,
                        ClusterInitCallInput.class,
                        (Function<ClusterInitCallInput, String>) ClusterInitCallInput::getClusterUrl
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
                )
        );
    }

    static Stream<Arguments> allCallsProvider() {
        return Stream.concat(nodeCallsProvider(), clusterCallsProvider());
    }

    private void execute(String cmdLine) {
        CommandLine cmd = new CommandLine(TopLevelCliCommand.class, new MicronautFactory(ctx));
        cmd.execute(cmdLine.split(" "));
    }

    private <IT extends CallInput, OT, T extends Call<IT, OT>> T registerMockCall(Class<T> callClass) {
        T mock = mock(callClass);
        ctx.registerSingleton(mock);
        when(mock.execute(any())).thenReturn(DefaultCallOutput.empty());
        return mock;
    }

    private <IT extends CallInput, OT, T extends Call<IT, OT>> IT verifyCallInput(T call, Class<IT> inputClass) {
        ArgumentCaptor<IT> captor = ArgumentCaptor.forClass(inputClass);
        verify(call).execute(captor.capture());
        return captor.getValue();
    }
}
