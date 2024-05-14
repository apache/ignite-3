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

package org.apache.ignite.internal.cli;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.copyResourceToTempFile;
import static org.apache.ignite.internal.cli.core.style.AnsiStringSupport.fg;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockserver.matchers.MatchType.ONLY_MATCHING_FIELDS;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.HttpStatusCode.INTERNAL_SERVER_ERROR_500;
import static org.mockserver.model.HttpStatusCode.OK_200;
import static org.mockserver.model.JsonBody.json;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.cli.core.style.AnsiStringSupport.Color;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.junit.jupiter.MockServerExtension;
import org.mockserver.model.MediaType;

/**
 * Smoke test for Ignite CLI features and its UI. Structure of tests should be self-documented and repeat the structure of Ignite CLI
 * subcommands.
 */
@DisplayName("ignite")
@ExtendWith(MockitoExtension.class)
@ExtendWith(MockServerExtension.class)
public class IgniteCliInterfaceTest extends AbstractCliTest {
    private final ClientAndServer clientAndServer;

    private final String mockUrl;

    public IgniteCliInterfaceTest(ClientAndServer clientAndServer) {
        this.clientAndServer = clientAndServer;
        mockUrl = "http://localhost:" + clientAndServer.getPort();
    }

    /**
     * Sets up environment before test execution.
     */
    @BeforeEach
    void setup() {
        resetStreams();

        clientAndServer.reset();
    }

    private int execute(String cmdLine) {
        return execute(cmdLine.split(" "));
    }

    /**
     * Tests "node" command.
     */
    @Nested
    @DisplayName("node")
    class Node {

        /**
         * Tests "config" command.
         */
        @Nested
        @DisplayName("config")
        class Config {
            @Test
            @DisplayName("show --url http://localhost:10300")
            void show() {
                clientAndServer
                        .when(request()
                                .withMethod("GET")
                                .withPath("/management/v1/configuration/node")
                        )
                        .respond(response("{\"autoAdjust\":{\"enabled\":true}}"));

                int exitCode = execute("node config show --url " + mockUrl);

                assertThatExitCodeMeansSuccess(exitCode);

                assertOutputEqual("{\n"
                                + "  \"autoAdjust\" : {\n"
                                + "    \"enabled\" : true\n"
                                + "  }\n"
                                + "}\n"
                );
                assertThatStderrIsEmpty();
            }

            @Test
            @DisplayName("show --url http://localhost:10300 local.baseline")
            void showSubtree() {
                clientAndServer
                        .when(request()
                                .withMethod("GET")
                                .withPath("/management/v1/configuration/node/local.baseline")
                        )
                        .respond(response("{\"autoAdjust\":{\"enabled\":true}}"));

                int exitCode = execute("node config show --url " + mockUrl + " local.baseline");

                assertThatExitCodeMeansSuccess(exitCode);

                assertOutputEqual("{\n"
                                + "  \"autoAdjust\" : {\n"
                                + "    \"enabled\" : true\n"
                                + "  }\n"
                                + "}\n"
                );
                assertThatStderrIsEmpty();
            }

            @Test
            @DisplayName("update --url http://localhost:10300 local.baseline.autoAdjust.enabled=true")
            void updateHocon() {
                clientAndServer
                        .when(request()
                                .withMethod("PATCH")
                                .withPath("/management/v1/configuration/node")
                                .withBody("local.baseline.autoAdjust.enabled=true")
                        )
                        .respond(response(null));

                int exitCode = execute("node config update --url " + mockUrl + " local.baseline.autoAdjust.enabled=true");

                assertThatExitCodeMeansSuccess(exitCode);

                assertOutputEqual("Node configuration updated. "
                        + fg(Color.YELLOW).mark("Restart the node to apply changes."));
                assertThatStderrIsEmpty();
            }
        }

        @Nested
        @DisplayName("metric")
        class Metric {
            @Test
            @DisplayName("metric source enable srcName")
            void enable() {
                clientAndServer
                        .when(request()
                                .withMethod("POST")
                                .withPath("/management/v1/metric/node/enable")
                                .withBody("srcName")
                        )
                        .respond(response(null));

                int exitCode = execute("node metric source enable --url " + mockUrl + " srcName");

                assertThatExitCodeMeansSuccess(exitCode);

                assertOutputEqual("Metric source was enabled successfully");
                assertThatStderrIsEmpty();
            }

            @Test
            @DisplayName("metric source disable srcName")
            void disable() {
                clientAndServer
                        .when(request()
                                .withMethod("POST")
                                .withPath("/management/v1/metric/node/disable")
                                .withBody("srcName")
                        )
                        .respond(response(null));

                int exitCode = execute("node metric source disable --url " + mockUrl + " srcName");

                assertThatExitCodeMeansSuccess(exitCode);

                assertOutputEqual("Metric source was disabled successfully");
                assertThatStderrIsEmpty();
            }

            @Test
            @DisplayName("metric source list")
            void listSources() {
                String responseBody = "[{\"name\":\"enabledMetric\",\"enabled\":true},{\"name\":\"disabledMetric\",\"enabled\":false}]";
                clientAndServer
                        .when(request()
                                .withMethod("GET")
                                .withPath("/management/v1/metric/node/source")
                        )
                        .respond(response(responseBody));

                int exitCode = execute("node metric source list --plain --url " + mockUrl);

                assertThatExitCodeMeansSuccess(exitCode);

                assertOutputEqual("Set name\tEnabled\nenabledMetric\tenabled\ndisabledMetric\tdisabled\n");
                assertThatStderrIsEmpty();
            }

            @Test
            @DisplayName("metric list")
            void listSets() {
                String responseBody = "[{\"name\":\"metricSet\",\"metrics\":[{\"name\":\"metric\",\"desc\":\"description\"}]}]";
                clientAndServer
                        .when(request()
                                .withMethod("GET")
                                .withPath("/management/v1/metric/node/set")
                        )
                        .respond(response(responseBody));

                int exitCode = execute("node metric list --plain --url " + mockUrl);

                assertThatExitCodeMeansSuccess(exitCode);

                assertOutputEqual("Set name\tMetric name\tDescription\nmetricSet\t\t\n\tmetric\tdescription");
                assertThatStderrIsEmpty();
            }
        }
    }

    /**
     * Tests "cluster" command.
     */
    @Nested
    @DisplayName("cluster")
    class Cluster {
        @Test
        @DisplayName("init --url http://localhost:10300 --ms-node node1ConsistentId --ms-node node2ConsistentId "
                + "--cmg-node node2ConsistentId --cmg-node node3ConsistentId --name cluster")
        void initSuccess() {
            var expectedSentContent = "{\"metaStorageNodes\":[\"node1ConsistentId\",\"node2ConsistentId\"],"
                    + "\"cmgNodes\":[\"node2ConsistentId\",\"node3ConsistentId\"],"
                    + "\"clusterName\":\"cluster\"}";

            clientAndServer
                    .when(request()
                            .withMethod("POST")
                            .withPath("/management/v1/cluster/init")
                            .withBody(json(expectedSentContent, ONLY_MATCHING_FIELDS))
                            .withContentType(MediaType.APPLICATION_JSON_UTF_8)
                    )
                    .respond(response(null));


            int exitCode = execute(
                    "cluster", "init",
                    "--url", mockUrl,
                    "--ms-node", "node1ConsistentId",
                    "--ms-node", "node2ConsistentId",
                    "--cmg-node", "node2ConsistentId",
                    "--cmg-node", "node3ConsistentId",
                    "--name", "cluster"
            );

            assertThatExitCodeMeansSuccess(exitCode);

            assertOutputEqual("Cluster was initialized successfully");
            assertThatStderrIsEmpty();
        }

        @Test
        @DisplayName(
                "init --url http://localhost:10300 --ms-node node1ConsistentId --ms-node node2ConsistentId "
                        + "--cmg-node node2ConsistentId --cmg-node node3ConsistentId --name cluster "
                        + "--auth-enabled --basic-auth-username admin --basic-auth-password password")
        void initWithAuthenticationSuccess() throws IOException {

            Path clusterConfigurationFile = copyResourceToTempFile("cluster-configuration-with-enabled-auth.conf").toPath();
            String clusterConfiguration = Files.readString(clusterConfigurationFile);

            var expectedSentContent = "{\n"
                    + "  \"metaStorageNodes\": [\n"
                    + "    \"node1ConsistentId\",\n"
                    + "    \"node2ConsistentId\"\n"
                    + "  ],\n"
                    + "  \"cmgNodes\": [\n"
                    + "    \"node2ConsistentId\",\n"
                    + "    \"node3ConsistentId\"\n"
                    + "  ],\n"
                    + "  \"clusterName\": \"cluster\",\n"
                    + "  \"clusterConfiguration\": \"" + clusterConfiguration + "\"\n"
                    + "}";

            clientAndServer
                    .when(request()
                            .withMethod("POST")
                            .withPath("/management/v1/cluster/init")
                            .withBody(json(expectedSentContent, ONLY_MATCHING_FIELDS))
                            .withContentType(MediaType.APPLICATION_JSON_UTF_8)
                    )
                    .respond(response(null));

            int exitCode = execute(
                    "cluster", "init",
                    "--url", mockUrl,
                    "--ms-node", "node1ConsistentId",
                    "--ms-node", "node2ConsistentId",
                    "--cmg-node", "node2ConsistentId",
                    "--cmg-node", "node3ConsistentId",
                    "--name", "cluster",
                    "--config-file", clusterConfigurationFile.toString()
            );

            assertThatExitCodeMeansSuccess(exitCode);

            assertOutputEqual("Cluster was initialized successfully");
            assertThatStderrIsEmpty();
        }

        @Test
        void initError() {
            clientAndServer
                    .when(request()
                            .withMethod("POST")
                            .withPath("/management/v1/cluster/init")
                    )
                    .respond(response()
                            .withStatusCode(INTERNAL_SERVER_ERROR_500.code())
                            .withBody("{\"status\":500, \"detail\":\"Oops\"}")
                    );

            int exitCode = execute(
                    "cluster", "init",
                    "--url", mockUrl,
                    "--ms-node", "node1ConsistentId",
                    "--ms-node", "node2ConsistentId",
                    "--cmg-node", "node2ConsistentId",
                    "--cmg-node", "node3ConsistentId",
                    "--name", "cluster"
            );

            assertThatExitCodeIs(1, exitCode);

            assertThatStdoutIsEmpty();
            assertErrOutputEqual("Oops");
        }

        @Test
        @DisplayName("init --url http://localhost:10300 --cmg-node node2ConsistentId --cmg-node node3ConsistentId")
        void metastorageNodesAreMandatoryForInit() {
            int exitCode = execute(
                    "cluster", "init",
                    "--url", mockUrl,
                    "--cmg-node", "node2ConsistentId",
                    "--cmg-node", "node3ConsistentId",
                    "--name", "cluster"
            );

            assertThatExitCodeIs(2, exitCode);

            assertThatStdoutIsEmpty();
            assertThat(err.toString(UTF_8), startsWith("Missing required option: '--ms-node=<metaStorageNodes>'"));
        }

        @Test
        @DisplayName("init --url http://localhost:10300 --ms-node node2ConsistentId --ms-node node3ConsistentId")
        void cmgNodesAreNotMandatoryForInit() {
            clientAndServer
                    .when(request()
                            .withMethod("POST")
                            .withPath("/management/v1/cluster/init")
                    )
                    .respond(response().withStatusCode(OK_200.code()));

            int exitCode = execute(
                    "cluster", "init",
                    "--url", mockUrl,
                    "--ms-node", "node1ConsistentId",
                    "--ms-node", "node2ConsistentId",
                    "--name", "cluster"
            );

            assertThatExitCodeMeansSuccess(exitCode);

            assertOutputEqual("Cluster was initialized successfully");
            assertThatStderrIsEmpty();
        }

        @Test
        @DisplayName("init --url http://localhost:10300 --ms-node node1ConsistentId --cmg-node node2ConsistentId")
        void clusterNameIsMandatoryForInit() {
            int exitCode = execute(
                    "cluster", "init",
                    "--url", mockUrl,
                    "--ms-node", "node1ConsistentId",
                    "--cmg-node", "node2ConsistentId"
            );

            assertThatExitCodeIs(2, exitCode);

            assertThatStdoutIsEmpty();
            assertThat(err.toString(UTF_8), startsWith("Missing required option: '--name=<clusterName>'"));
        }

        @Nested
        @DisplayName("config")
        class Config {
            @Test
            @DisplayName("show --url http://localhost:10300")
            void show() {
                clientAndServer
                        .when(request()
                                .withMethod("GET")
                                .withPath("/management/v1/configuration/cluster")
                        )
                        .respond(response("{\"autoAdjust\":{\"enabled\":true}}"));

                int exitCode = execute("cluster config show --url " + mockUrl);

                assertThatExitCodeMeansSuccess(exitCode);

                assertOutputEqual("{\n"
                        + "  \"autoAdjust\" : {\n"
                        + "    \"enabled\" : true\n"
                        + "  }\n"
                        + "}\n");
                assertThatStderrIsEmpty();
            }

            @Test
            @DisplayName("show --url http://localhost:10300 local.baseline")
            void showSubtree() {
                clientAndServer
                        .when(request()
                                .withMethod("GET")
                                .withPath("/management/v1/configuration/cluster/local.baseline")
                        )
                        .respond(response("{\"autoAdjust\":{\"enabled\":true}}"));

                int exitCode = execute("cluster config show --url " + mockUrl + " local.baseline");

                assertThatExitCodeMeansSuccess(exitCode);

                assertOutputEqual("{\n"
                        + "  \"autoAdjust\" : {\n"
                        + "    \"enabled\" : true\n"
                        + "  }\n"
                        + "}\n");
                assertThatStderrIsEmpty();
            }

            @Test
            @DisplayName("update --url http://localhost:10300 local.baseline.autoAdjust.enabled=true")
            void updateHocon() {
                clientAndServer
                        .when(request()
                                .withMethod("PATCH")
                                .withPath("/management/v1/configuration/cluster")
                                .withBody("local.baseline.autoAdjust.enabled=true")
                        )
                        .respond(response(null));

                int exitCode = execute("cluster config update --url "
                        + mockUrl + " local.baseline.autoAdjust.enabled=true");

                assertThatExitCodeMeansSuccess(exitCode);

                assertOutputEqual("Cluster configuration was updated successfully");
                assertThatStderrIsEmpty();
            }
        }
    }

    private void assertThatStdoutIsEmpty() {
        assertThat(out.toString(UTF_8), is(""));
    }

    private void assertThatStderrIsEmpty() {
        assertThat(err.toString(UTF_8), is(""));
    }

    private void assertThatExitCodeMeansSuccess(int exitCode) {
        assertThatExitCodeIs(0, exitCode);
    }

    private void assertThatExitCodeIs(int expectedCode, int exitCode) {
        assertEquals(expectedCode, exitCode, outputStreams());
    }

    private String outputStreams() {
        return "stdout:\n" + out.toString(UTF_8) + "\n" + "stderr:\n" + err.toString(UTF_8);
    }

    /**
     * <em>Assert</em> that {@code expected} and {@code actual} are equals ignoring differences in line separators.
     *
     * <p>If both are {@code null}, they are considered equal.
     *
     * @param exp Expected result.
     * @param actual Actual result.
     * @see Object#equals(Object)
     */
    private static void assertEqualsIgnoreLineSeparators(String exp, String actual) {
        assertEquals(
                exp.lines().collect(toList()),
                actual.lines().collect(toList())
        );
    }

    private void assertOutputEqual(String exp) {
        assertEqualsIgnoreLineSeparators(exp, out.toString(UTF_8));
    }

    private void assertErrOutputEqual(String exp) {
        assertEqualsIgnoreLineSeparators(exp, err.toString(UTF_8));
    }
}
