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

package org.apache.ignite.cli.deprecated;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockserver.matchers.MatchType.ONLY_MATCHING_FIELDS;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.HttpStatusCode.INTERNAL_SERVER_ERROR_500;
import static org.mockserver.model.HttpStatusCode.OK_200;
import static org.mockserver.model.JsonBody.json;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.Environment;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.cli.commands.TopLevelCliCommand;
import org.apache.ignite.cli.deprecated.builtins.init.InitIgniteCommand;
import org.apache.ignite.cli.deprecated.builtins.node.NodeManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.junit.jupiter.MockServerExtension;
import org.mockserver.model.MediaType;
import picocli.CommandLine;

/**
 * Smoke test for Ignite CLI features and its UI. Structure of tests should be self-documented and repeat the structure of Ignite CLI
 * subcommands.
 */
@DisplayName("ignite")
@ExtendWith(MockitoExtension.class)
@ExtendWith(MockServerExtension.class)
public class IgniteCliInterfaceTest extends AbstractCliTest {
    /** DI application context. */
    ApplicationContext ctx;

    /** stderr. */
    ByteArrayOutputStream err;

    /** stdout. */
    ByteArrayOutputStream out;

    /** Configuration loader. */
    @Mock
    CliPathsConfigLoader cliPathsCfgLdr;

    /** Paths to cli working directories. */
    IgnitePaths ignitePaths = new IgnitePaths(
            Path.of("bin"),
            Path.of("work"),
            Path.of("config"),
            Path.of("log"),
            "version");

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
        ctx = ApplicationContext.run(Environment.TEST);

        ctx.registerSingleton(cliPathsCfgLdr);

        err = new ByteArrayOutputStream();
        out = new ByteArrayOutputStream();

        clientAndServer.reset();
    }

    /**
     * Stops application context after a test.
     */
    @AfterEach
    void tearDown() {
        ctx.stop();
    }

    /**
     * Creates a new command line interpreter with the given application context.
     *
     * @return New {@code CommandLine} interpreter.
     */
    CommandLine cmd(ApplicationContext applicationCtx) {
        CommandLine.IFactory factory = new CommandFactory(applicationCtx);

        return new CommandLine(TopLevelCliCommand.class, factory)
                .setErr(new PrintWriter(err, true))
                .setOut(new PrintWriter(out, true));
    }

    private int execute(String cmdLine) {
        return cmd(ctx).execute(cmdLine.split(" "));
    }

    /**
     * Tests "bootstrap" command.
     */
    @DisplayName("bootstrap")
    @Nested
    class Bootstrap {
        @Test
        @DisplayName("bootstrap")
        void bootstrap() {
            var initIgniteCmd = mock(InitIgniteCommand.class);

            ctx.registerSingleton(InitIgniteCommand.class, initIgniteCmd);

            CommandLine cli = cmd(ctx);

            assertEquals(0, cli.execute("bootstrap"));
            verify(initIgniteCmd).init(any(), any(), any());
        }
    }

    /**
     * Tests "node" command.
     */
    @Nested
    @DisplayName("node")
    class Node {
        /** Manager of local Ignite nodes. */
        @Mock
        NodeManager nodeMgr;

        @BeforeEach
        void setUp() {
            ctx.registerSingleton(nodeMgr);
        }

        @Test
        @DisplayName("start node1 --config conf.json")
        void start() {
            var nodeName = "node1";

            var node =
                    new NodeManager.RunningNode(1, nodeName, Path.of("logfile"));

            when(nodeMgr.start(any(), any(), any(), any(), any(), any(), any(), any()))
                    .thenReturn(node);

            when(cliPathsCfgLdr.loadIgnitePathsOrThrowError())
                    .thenReturn(ignitePaths);

            CommandLine cli = cmd(ctx);

            int exitCode = cli.execute(("node start " + nodeName + " --config conf.json").split(" "));

            assertThatExitCodeMeansSuccess(exitCode);

            verify(nodeMgr).start(
                    nodeName,
                    ignitePaths.nodesBaseWorkDir(),
                    ignitePaths.logDir,
                    ignitePaths.cliPidsDir(),
                    Path.of("conf.json"),
                    null,
                    ignitePaths.serverJavaUtilLoggingPros(),
                    cli.getOut());

            assertOutputEqual("\nNode is successfully started. To stop, type ignite node stop " + nodeName + "\n\n"
                            + "+-----------+---------+\n"
                            + "| Node name | node1   |\n"
                            + "+-----------+---------+\n"
                            + "| PID       | 1       |\n"
                            + "+-----------+---------+\n"
                            + "| Log File  | logfile |\n"
                            + "+-----------+---------+\n"
            );
            assertThatStderrIsEmpty();
        }

        @Test
        @DisplayName("start node1 --port 12345")
        void startCustomPort() {
            var nodeName = "node1";

            var node = new NodeManager.RunningNode(1, nodeName, Path.of("logfile"));

            when(nodeMgr.start(any(), any(), any(), any(), any(), any(), any(), any()))
                    .thenReturn(node);

            when(cliPathsCfgLdr.loadIgnitePathsOrThrowError())
                    .thenReturn(ignitePaths);

            int exitCode = execute("node start " + nodeName + " --port 12345");

            assertThatExitCodeMeansSuccess(exitCode);

            ArgumentCaptor<String> configStrCaptor = ArgumentCaptor.forClass(String.class);
            verify(nodeMgr).start(any(), any(), any(), any(), any(), configStrCaptor.capture(), any(), any());

            assertEqualsIgnoreLineSeparators(
                    "network{port=12345}",
                    configStrCaptor.getValue()
            );
        }

        @Test
        @DisplayName("start node1 --config ignite-config.json --port 12345")
        void startCustomPortOverrideConfigFile() throws URISyntaxException {
            var nodeName = "node1";

            var node = new NodeManager.RunningNode(1, nodeName, Path.of("logfile"));

            when(nodeMgr.start(any(), any(), any(), any(), any(), any(), any(), any()))
                    .thenReturn(node);

            when(cliPathsCfgLdr.loadIgnitePathsOrThrowError())
                    .thenReturn(ignitePaths);

            Path configPath = Path.of(IgniteCliInterfaceTest.class.getResource("/ignite-config.json").toURI());

            int exitCode = execute("node start " + nodeName + " --config "
                    + configPath.toAbsolutePath()
                    + " --port 12345");

            assertThatExitCodeMeansSuccess(exitCode);

            ArgumentCaptor<String> configStrCaptor = ArgumentCaptor.forClass(String.class);
            verify(nodeMgr).start(any(), any(), any(), any(), any(), configStrCaptor.capture(), any(), any());

            assertEqualsIgnoreLineSeparators(
                    "network{port=12345},rest{port=10300}",
                    configStrCaptor.getValue()
            );
        }

        @Test
        @DisplayName("start node1 --port 12345 --rest-port 12346")
        void startCustomPortAndRestPort() {
            var nodeName = "node1";

            var node = new NodeManager.RunningNode(1, nodeName, Path.of("logfile"));

            when(nodeMgr.start(any(), any(), any(), any(), any(), any(), any(), any()))
                    .thenReturn(node);

            when(cliPathsCfgLdr.loadIgnitePathsOrThrowError())
                    .thenReturn(ignitePaths);

            int exitCode = execute("node start " + nodeName + " --port 12345 --rest-port 12346");

            assertThatExitCodeMeansSuccess(exitCode);

            ArgumentCaptor<String> configStrCaptor = ArgumentCaptor.forClass(String.class);
            verify(nodeMgr).start(any(), any(), any(), any(), any(), configStrCaptor.capture(), any(), any());

            assertEqualsIgnoreLineSeparators(
                    "network{port=12345},rest{port=12346}",
                    configStrCaptor.getValue()
            );
        }

        @Test
        @DisplayName("start node1 --port 12345 --rest-port 12346 --join localhost:12345")
        void startCustomPortRestPortAndSeedNodes() {
            var nodeName = "node1";

            var node = new NodeManager.RunningNode(1, nodeName, Path.of("logfile"));

            when(nodeMgr.start(any(), any(), any(), any(), any(), any(), any(), any()))
                    .thenReturn(node);

            when(cliPathsCfgLdr.loadIgnitePathsOrThrowError())
                    .thenReturn(ignitePaths);

            int exitCode = execute("node start " + nodeName + " --port 12345 --rest-port 12346 --join localhost:12345");

            assertThatExitCodeMeansSuccess(exitCode);

            ArgumentCaptor<String> configStrCaptor = ArgumentCaptor.forClass(String.class);
            verify(nodeMgr).start(any(), any(), any(), any(), any(), configStrCaptor.capture(), any(), any());

            assertEqualsIgnoreLineSeparators(
                    "network{nodeFinder{netClusterNodes=[\"localhost:12345\"]},port=12345},rest{port=12346}",
                    configStrCaptor.getValue()
            );
        }

        @Test
        @DisplayName("stop node1")
        void stopRunning() {
            var nodeName = "node1";

            when(nodeMgr.stopWait(any(), any()))
                    .thenReturn(true);

            when(cliPathsCfgLdr.loadIgnitePathsOrThrowError())
                    .thenReturn(ignitePaths);

            CommandLine cmd = cmd(ctx);
            int exitCode =
                    cmd.execute(("node stop " + nodeName).split(" "));

            assertThatExitCodeMeansSuccess(exitCode);
            verify(nodeMgr).stopWait(nodeName, ignitePaths.cliPidsDir());
            assertOutputEqual(
                    "Stopping locally running node with consistent ID "
                            + cmd.getColorScheme().parameterText(nodeName)
                            + cmd.getColorScheme().text("... @|bold,green Done!|@\n")
            );
            assertThatStderrIsEmpty();
        }

        @Test
        @DisplayName("stop unknown-node")
        void stopUnknown() {
            var nodeName = "unknown-node";

            when(nodeMgr.stopWait(any(), any()))
                    .thenReturn(false);

            when(cliPathsCfgLdr.loadIgnitePathsOrThrowError())
                    .thenReturn(ignitePaths);

            CommandLine cmd = cmd(ctx);
            int exitCode =
                    cmd.execute(("node stop " + nodeName).split(" "));

            assertThatExitCodeMeansSuccess(exitCode);
            verify(nodeMgr).stopWait(nodeName, ignitePaths.cliPidsDir());
            assertOutputEqual(
                    "Stopping locally running node with consistent ID "
                            + cmd.getColorScheme().parameterText(nodeName)
                            + cmd.getColorScheme().text("... @|bold,red Failed|@\n")
            );
            assertThatStderrIsEmpty();
        }

        @Test
        @DisplayName("list")
        void list() {
            when(nodeMgr.getRunningNodes(any(), any()))
                    .thenReturn(Arrays.asList(
                            new NodeManager.RunningNode(1, "new1", Path.of("logFile1")),
                            new NodeManager.RunningNode(2, "new2", Path.of("logFile2"))
                    ));

            when(cliPathsCfgLdr.loadIgnitePathsOrThrowError())
                    .thenReturn(ignitePaths);

            CommandLine cmd = cmd(ctx);
            int exitCode =
                    cmd.execute("node list".split(" "));

            assertThatExitCodeMeansSuccess(exitCode);
            verify(nodeMgr).getRunningNodes(ignitePaths.logDir, ignitePaths.cliPidsDir());
            assertOutputEqual(cmd.getColorScheme().text("Number of running nodes: @|bold 2|@\n\n")
                    + "+---------------+-----+----------+\n"
                    + cmd.getColorScheme().text("| @|bold Consistent ID|@ | @|bold PID|@ | @|bold Log File|@ |\n")
                    + "+---------------+-----+----------+\n"
                    + "| new1          | 1   | logFile1 |\n"
                    + "+---------------+-----+----------+\n"
                    + "| new2          | 2   | logFile2 |\n"
                    + "+---------------+-----+----------+\n"
            );
            assertThatStderrIsEmpty();
        }

        @Test
        @DisplayName("list")
        void listEmpty() {
            when(nodeMgr.getRunningNodes(any(), any()))
                    .thenReturn(Collections.emptyList());

            when(cliPathsCfgLdr.loadIgnitePathsOrThrowError())
                    .thenReturn(ignitePaths);

            CommandLine cmd = cmd(ctx);
            int exitCode =
                    cmd.execute("node list".split(" "));

            assertThatExitCodeMeansSuccess(exitCode);
            verify(nodeMgr).getRunningNodes(ignitePaths.logDir, ignitePaths.cliPidsDir());
            assertOutputEqual("Currently, there are no locally running nodes.\n\n"
                            + "Use the " + cmd.getColorScheme().commandText("ignite node start") + " command to start a new node.\n"
            );
            assertThatStderrIsEmpty();
        }

        @Test
        @DisplayName("classpath")
        void classpath() throws IOException {
            when(nodeMgr.classpathItems()).thenReturn(Arrays.asList("item1", "item2"));

            CommandLine cmd = cmd(ctx);
            int exitCode = cmd.execute("node classpath".split(" "));

            assertThatExitCodeMeansSuccess(exitCode);
            verify(nodeMgr).classpathItems();
            assertOutputEqual(
                    cmd.getColorScheme().text(
                            "@|bold Current Ignite node classpath:|@\n    item1\n    item2\n").toString()
            );
            assertThatStderrIsEmpty();
        }

        /**
         * Tests "config" command.
         */
        @Nested
        @DisplayName("config")
        class Config {
            @Test
            @DisplayName("show --node-url http://localhost:10300")
            void show() {
                clientAndServer
                        .when(request()
                                .withMethod("GET")
                                .withPath("/management/v1/configuration/node")
                        )
                        .respond(response("{\"autoAdjust\":{\"enabled\":true}}"));

                int exitCode = execute("node config show --node-url " + mockUrl);

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
            @DisplayName("show --node-url http://localhost:10300 --selector local.baseline")
            void showSubtree() {
                clientAndServer
                        .when(request()
                                .withMethod("GET")
                                .withPath("/management/v1/configuration/node/local.baseline")
                        )
                        .respond(response("{\"autoAdjust\":{\"enabled\":true}}"));

                int exitCode = execute("node config show --node-url " + mockUrl + " --selector local.baseline");

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
            @DisplayName("update --node-url http://localhost:10300 local.baseline.autoAdjust.enabled=true")
            void updateHocon() {
                clientAndServer
                        .when(request()
                                .withMethod("PATCH")
                                .withPath("/management/v1/configuration/node")
                                .withBody("local.baseline.autoAdjust.enabled=true")
                        )
                        .respond(response(null));

                int exitCode = execute("node config update --node-url " + mockUrl + " local.baseline.autoAdjust.enabled=true");

                assertThatExitCodeMeansSuccess(exitCode);

                assertOutputEqual("Node configuration was updated successfully.");
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
        @DisplayName("init --cluster-endpoint-url http://localhost:10300 --meta-storage-node node1ConsistentId --meta-storage-node node2ConsistentId "
                + "--cmg-node node2ConsistentId --cmg-node node3ConsistentId --cluster-name cluster")
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


            int exitCode = cmd(ctx).execute(
                    "cluster", "init",
                    "--cluster-endpoint-url", mockUrl,
                    "--meta-storage-node", "node1ConsistentId",
                    "--meta-storage-node", "node2ConsistentId",
                    "--cmg-node", "node2ConsistentId",
                    "--cmg-node", "node3ConsistentId",
                    "--cluster-name", "cluster"
            );

            assertThatExitCodeMeansSuccess(exitCode);

            assertOutputEqual("Cluster was initialized successfully.");
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
                            .withBody("Oops")
                    );

            int exitCode = cmd(ctx).execute(
                    "cluster", "init",
                    "--cluster-endpoint-url", mockUrl,
                    "--meta-storage-node", "node1ConsistentId",
                    "--meta-storage-node", "node2ConsistentId",
                    "--cmg-node", "node2ConsistentId",
                    "--cmg-node", "node3ConsistentId",
                    "--cluster-name", "cluster"
            );

            assertThatExitCodeIs(1, exitCode);

            assertThatStdoutIsEmpty();
            assertErrOutputEqual("An error occurred [errorCode=500, response=Oops]");
        }

        @Test
        @DisplayName("init --cluster-endpoint-url http://localhost:10300 --cmg-node node2ConsistentId --cmg-node node3ConsistentId")
        void metastorageNodesAreMandatoryForInit() {
            int exitCode = cmd(ctx).execute(
                    "cluster", "init",
                    "--cluster-endpoint-url", mockUrl,
                    "--cmg-node", "node2ConsistentId",
                    "--cmg-node", "node3ConsistentId",
                    "--cluster-name", "cluster"
            );

            assertThatExitCodeIs(2, exitCode);

            assertThatStdoutIsEmpty();
            assertThat(err.toString(UTF_8), startsWith("Missing required option: '--meta-storage-node=<metaStorageNodes>'"));
        }

        @Test
        @DisplayName("init --cluster-endpoint-url http://localhost:10300 --meta-storage-node node2ConsistentId --meta-storage-node node3ConsistentId")
        void cmgNodesAreNotMandatoryForInit() {
            clientAndServer
                    .when(request()
                            .withMethod("POST")
                            .withPath("/management/v1/cluster/init")
                    )
                    .respond(response().withStatusCode(OK_200.code()));

            int exitCode = cmd(ctx).execute(
                    "cluster", "init",
                    "--cluster-endpoint-url", mockUrl,
                    "--meta-storage-node", "node1ConsistentId",
                    "--meta-storage-node", "node2ConsistentId",
                    "--cluster-name", "cluster"
            );

            assertThatExitCodeMeansSuccess(exitCode);

            assertOutputEqual("Cluster was initialized successfully.");
            assertThatStderrIsEmpty();
        }

        @Test
        @DisplayName("init --cluster-endpoint-url http://localhost:10300 --meta-storage-node node1ConsistentId --cmg-node node2ConsistentId")
        void clusterNameIsMandatoryForInit() {
            int exitCode = cmd(ctx).execute(
                    "cluster", "init",
                    "--cluster-endpoint-url", mockUrl,
                    "--meta-storage-node", "node1ConsistentId",
                    "--cmg-node", "node2ConsistentId"
            );

            assertThatExitCodeIs(2, exitCode);

            assertThatStdoutIsEmpty();
            assertThat(err.toString(UTF_8), startsWith("Missing required option: '--cluster-name=<clusterName>'"));
        }

        @Nested
        @DisplayName("config")
        class Config {
            @Test
            @DisplayName("show --cluster-endpoint-url http://localhost:10300")
            void show() {
                clientAndServer
                        .when(request()
                                .withMethod("GET")
                                .withPath("/management/v1/configuration/cluster")
                        )
                        .respond(response("{\"autoAdjust\":{\"enabled\":true}}"));

                int exitCode = execute("cluster config show --cluster-endpoint-url " + mockUrl);

                assertThatExitCodeMeansSuccess(exitCode);

                assertOutputEqual("{\n"
                        + "  \"autoAdjust\" : {\n"
                        + "    \"enabled\" : true\n"
                        + "  }\n"
                        + "}\n");
                assertThatStderrIsEmpty();
            }

            @Test
            @DisplayName("show --cluster-endpoint-url http://localhost:10300 --selector local.baseline")
            void showSubtree() {
                clientAndServer
                        .when(request()
                                .withMethod("GET")
                                .withPath("/management/v1/configuration/cluster/local.baseline")
                        )
                        .respond(response("{\"autoAdjust\":{\"enabled\":true}}"));

                int exitCode = execute("cluster config show --cluster-endpoint-url " + mockUrl + " --selector local.baseline");

                assertThatExitCodeMeansSuccess(exitCode);

                assertOutputEqual("{\n"
                        + "  \"autoAdjust\" : {\n"
                        + "    \"enabled\" : true\n"
                        + "  }\n"
                        + "}\n");
                assertThatStderrIsEmpty();
            }

            @Test
            @DisplayName("update --cluster-endpoint-url http://localhost:10300 local.baseline.autoAdjust.enabled=true")
            void updateHocon() {
                clientAndServer
                        .when(request()
                                .withMethod("PATCH")
                                .withPath("/management/v1/configuration/cluster")
                                .withBody("local.baseline.autoAdjust.enabled=true")
                        )
                        .respond(response(null));

                int exitCode = execute("cluster config update --cluster-endpoint-url "
                        + mockUrl + " local.baseline.autoAdjust.enabled=true");

                assertThatExitCodeMeansSuccess(exitCode);

                assertOutputEqual("Cluster configuration was updated successfully.");
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
