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

package org.apache.ignite.cli;

import static com.github.npathai.hamcrestopt.OptionalMatchers.isPresentAnd;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.Environment;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.cli.builtins.init.InitIgniteCommand;
import org.apache.ignite.cli.builtins.module.ModuleManager;
import org.apache.ignite.cli.builtins.module.ModuleRegistry;
import org.apache.ignite.cli.builtins.module.StandardModuleDefinition;
import org.apache.ignite.cli.builtins.node.NodeManager;
import org.apache.ignite.cli.spec.IgniteCliSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import picocli.CommandLine;
import reactor.adapter.JdkFlowAdapter;

/**
 * Smoke test for Ignite CLI features and its UI. Structure of tests should be self-documented and repeat the structure of Ignite CLI
 * subcommands.
 */
@DisplayName("ignite")
@ExtendWith(MockitoExtension.class)
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

    @Captor
    private ArgumentCaptor<HttpRequest> requestCaptor;

    /**
     * Sets up environment before test execution.
     */
    @BeforeEach
    void setup() {
        ctx = ApplicationContext.run(Environment.TEST);

        ctx.registerSingleton(cliPathsCfgLdr);

        err = new ByteArrayOutputStream();
        out = new ByteArrayOutputStream();
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

        return new CommandLine(IgniteCliSpec.class, factory)
                .setErr(new PrintWriter(err, true))
                .setOut(new PrintWriter(out, true));
    }

    /**
     * Tests "init" command.
     */
    @DisplayName("init")
    @Nested
    class Init {
        @Test
        @DisplayName("init")
        void init() {
            var initIgniteCmd = mock(InitIgniteCommand.class);

            ctx.registerSingleton(InitIgniteCommand.class, initIgniteCmd);

            CommandLine cli = cmd(ctx);

            assertEquals(0, cli.execute("init"));
            verify(initIgniteCmd).init(any(), any(), any());
        }
    }

    /**
     * Tests "module" command.
     */
    @DisplayName("module")
    @Nested
    class Module {
        /** Distributive module manager. */
        @Mock
        ModuleManager moduleMgr;

        /** registry of installed CLI or Ignite server modules. */
        @Mock
        ModuleRegistry moduleRegistry;

        @BeforeEach
        void setUp() {
            ctx.registerSingleton(moduleMgr);
            ctx.registerSingleton(moduleRegistry);
        }

        @Test
        @DisplayName("add mvn:groupId:artifact:version")
        void add() {
            when(cliPathsCfgLdr.loadIgnitePathsOrThrowError()).thenReturn(ignitePaths);

            int exitCode =
                    cmd(ctx).execute("module add mvn:groupId:artifactId:version".split(" "));

            verify(moduleMgr).addModule("mvn:groupId:artifactId:version", ignitePaths, Collections.emptyList());
            assertThatExitCodeMeansSuccess(exitCode);
        }

        @Test
        @DisplayName("add mvn:groupId:artifact:version --repo http://mvnrepo.com/repostiory --repo http://anotherrepo.com/repostiory")
        void addWithCustomRepo() throws MalformedURLException {
            doNothing().when(moduleMgr).addModule(any(), any(), any());

            when(cliPathsCfgLdr.loadIgnitePathsOrThrowError()).thenReturn(ignitePaths);

            int exitCode = cmd(ctx)
                    .execute(
                            ("module add mvn:groupId:artifactId:version --repo http://mvnrepo.com/repostiory "
                                    + "--repo http://anotherrepo.com/repostiory").split(" "));

            verify(moduleMgr).addModule(
                    "mvn:groupId:artifactId:version",
                    ignitePaths,
                    List.of(new URL("http://mvnrepo.com/repostiory"), new URL("http://anotherrepo.com/repostiory")));
            assertThatExitCodeMeansSuccess(exitCode);
        }

        @Test
        @DisplayName("add test-module")
        void addBuiltinModule() {
            doNothing().when(moduleMgr).addModule(any(), any(), any());

            when(cliPathsCfgLdr.loadIgnitePathsOrThrowError()).thenReturn(ignitePaths);

            int exitCode =
                    cmd(ctx).execute("module add test-module".split(" "));

            verify(moduleMgr).addModule("test-module", ignitePaths, Collections.emptyList());
            assertThatExitCodeMeansSuccess(exitCode);
        }

        @Test
        @DisplayName("remove builtin-module")
        void remove() throws UnsupportedEncodingException {
            var moduleName = "builtin-module";

            when(moduleMgr.removeModule(moduleName)).thenReturn(true);

            CommandLine cmd = cmd(ctx);
            int exitCode =
                    cmd.execute("module remove builtin-module".split(" "));

            verify(moduleMgr).removeModule(moduleName);
            assertThatExitCodeMeansSuccess(exitCode);
            assertOutputEqual("Module " + cmd.getColorScheme().parameterText(moduleName)
                    + " was removed successfully.\n", out.toString(UTF_8));
            assertThatStderrIsEmpty();
        }

        @Test
        @DisplayName("remove unknown-module")
        void removeUnknownModule() {
            var moduleName = "unknown-module";

            when(moduleMgr.removeModule(moduleName)).thenReturn(false);

            CommandLine cmd = cmd(ctx);
            int exitCode =
                    cmd(ctx).execute("module remove unknown-module".split(" "));

            verify(moduleMgr).removeModule(moduleName);
            assertThatExitCodeMeansSuccess(exitCode);
            assertOutputEqual("Nothing to do: module " + cmd.getColorScheme().parameterText(moduleName)
                    + " is not yet added.\n", out.toString(UTF_8));
            assertThatStderrIsEmpty();
        }

        @Test
        @DisplayName("list")
        void list() {
            var module1 = new StandardModuleDefinition(
                    "module1",
                    "description1",
                    Collections.singletonList("artifact1"),
                    Collections.singletonList("cli-artifact1"));
            var module2 = new StandardModuleDefinition(
                    "module2",
                    "description2",
                    Collections.singletonList("artifact2"),
                    Collections.singletonList("cli-artifact2"));

            when(moduleMgr.builtinModules()).thenReturn(Arrays.asList(module1, module2));

            var externalModule = new ModuleRegistry.ModuleDefinition(
                    "org.apache.ignite:snapshot:2.9.0",
                    Collections.emptyList(),
                    Collections.emptyList(),
                    ModuleRegistry.SourceType.Maven,
                    "mvn:org.apache.ignite:snapshot:2.9.0");

            when(moduleRegistry.listInstalled()).thenReturn(
                    new ModuleRegistry.ModuleDefinitionsList(
                            Arrays.asList(
                                    new ModuleRegistry.ModuleDefinition(
                                            module1.name,
                                            Collections.emptyList(),
                                            Collections.emptyList(),
                                            ModuleRegistry.SourceType.Standard, ""), externalModule)));

            CommandLine cmd = cmd(ctx);
            int exitCode =
                    cmd.execute("module list".split(" "));

            verify(moduleMgr).builtinModules();
            assertThatExitCodeMeansSuccess(exitCode);

            String expOutput = cmd.getColorScheme().text("@|bold Optional Ignite Modules|@\n"
                    + "+---------+--------------+------------+\n"
                    + "| @|bold Name|@    | @|bold Description|@  | @|bold Installed?|@ |\n"
                    + "+---------+--------------+------------+\n"
                    + "| module1 | description1 | Yes        |\n"
                    + "+---------+--------------+------------+\n"
                    + "| module2 | description2 | No         |\n"
                    + "+---------+--------------+------------+\n"
                    + "\n" + "@|bold Additional Maven Dependencies|@\n"
                    + "+-------------------+-------------+---------+\n"
                    + "| @|bold Group ID|@          | @|bold Artifact ID|@ | @|bold Version|@ |\n"
                    + "+-------------------+-------------+---------+\n"
                    + "| org.apache.ignite | snapshot    | 2.9.0   |\n"
                    + "+-------------------+-------------+---------+\n"
                    + "Type " + cmd.getColorScheme().commandText("ignite module remove") + " "
                    + cmd.getColorScheme().parameterText("<groupId>:<artifactId>:<version>")
                    + " to remove a dependency.\n"
            ).toString();
            assertOutputEqual(expOutput, out.toString(UTF_8));
            assertThatStderrIsEmpty();
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

            when(nodeMgr.start(any(), any(), any(), any(), any(), any(), any()))
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
                    ignitePaths.serverJavaUtilLoggingPros(),
                    cli.getOut());

            assertOutputEqual("\nNode is successfully started. To stop, type ignite node stop " + nodeName + "\n\n"
                            + "+-----------+---------+\n"
                            + "| Node name | node1   |\n"
                            + "+-----------+---------+\n"
                            + "| PID       | 1       |\n"
                            + "+-----------+---------+\n"
                            + "| Log File  | logfile |\n"
                            + "+-----------+---------+\n",
                    out.toString(UTF_8));
            assertThatStderrIsEmpty();
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
                            + cmd.getColorScheme().text("... @|bold,green Done!|@\n"),
                    out.toString(UTF_8));
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
                            + cmd.getColorScheme().text("... @|bold,red Failed|@\n"),
                    out.toString(UTF_8));
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
                            + "+---------------+-----+----------+\n",
                    out.toString(UTF_8));
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
                            + "Use the " + cmd.getColorScheme().commandText("ignite node start") + " command to start a new node.\n",
                    out.toString(UTF_8));
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
                            "@|bold Current Ignite node classpath:|@\n    item1\n    item2\n").toString(),
                    out.toString(UTF_8));
            assertThatStderrIsEmpty();
        }
    }

    /**
     * Tests "config" command.
     */
    @Nested
    @DisplayName("config")
    class Config {
        /** Http client that is used for communication purposes. */
        @Mock
        private HttpClient httpClient;

        /** HTTP response. */
        @Mock
        private HttpResponse<String> res;

        @BeforeEach
        void setUp() {
            ctx.registerSingleton(httpClient);
        }

        //TODO: Fix in https://issues.apache.org/jira/browse/IGNITE-15306
        @Test
        @DisplayName("get --node-endpoint localhost:8081 --type node")
        void get() throws IOException, InterruptedException {
            when(res.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
            when(res.body()).thenReturn("{\"baseline\":{\"autoAdjust\":{\"enabled\":true}}}");
            when(httpClient.<String>send(any(), any())).thenReturn(res);

            int exitCode =
                    cmd(ctx).execute("config get --node-endpoint localhost:8081 --type node".split(" "));

            assertThatExitCodeMeansSuccess(exitCode);

            verify(httpClient).send(requestCaptor.capture(), any());
            HttpRequest capturedRequest = requestCaptor.getValue();

            assertThat(capturedRequest.uri().toString(), is("http://localhost:8081/management/v1/configuration/node/"));
            assertThat(capturedRequest.method(), is("GET"));
            assertThat(capturedRequest.headers().firstValue("Content-Type"), isPresentAnd(is("application/json")));

            assertOutputEqual("{\n"
                    + "  \"baseline\" : {\n"
                    + "    \"autoAdjust\" : {\n"
                    + "      \"enabled\" : true\n"
                    + "    }\n"
                    + "  }\n"
                    + "}\n", out.toString(UTF_8));
            assertThatStderrIsEmpty();
        }

        //TODO: Fix in https://issues.apache.org/jira/browse/IGNITE-15306
        @Test
        @DisplayName("get --node-endpoint localhost:8081 --selector local.baseline --type node")
        void getSubtree() throws IOException, InterruptedException {
            when(res.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
            when(res.body()).thenReturn("{\"autoAdjust\":{\"enabled\":true}}");
            when(httpClient.<String>send(any(), any())).thenReturn(res);

            int exitCode =
                    cmd(ctx).execute(("config get --node-endpoint localhost:8081 "
                            + "--selector local.baseline --type node").split(" "));

            assertThatExitCodeMeansSuccess(exitCode);

            verify(httpClient).send(requestCaptor.capture(), any());
            HttpRequest capturedRequest = requestCaptor.getValue();

            assertThat(capturedRequest.uri().toString(), is("http://localhost:8081/management/v1/configuration/node/local.baseline"));
            assertThat(capturedRequest.method(), is("GET"));
            assertThat(capturedRequest.headers().firstValue("Content-Type"), isPresentAnd(is("application/json")));

            assertOutputEqual("{\n"
                    + "  \"autoAdjust\" : {\n"
                    + "    \"enabled\" : true\n"
                    + "  }\n"
                    + "}\n", out.toString(UTF_8));
            assertThatStderrIsEmpty();
        }

        //TODO: Fix in https://issues.apache.org/jira/browse/IGNITE-15306
        @Test
        @DisplayName("set --node-endpoint localhost:8081 local.baseline.autoAdjust.enabled=true --type node")
        void setHocon() throws IOException, InterruptedException {
            when(res.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
            when(httpClient.<String>send(any(), any())).thenReturn(res);

            var expSentContent = "{\"local\":{\"baseline\":{\"autoAdjust\":{\"enabled\":true}}}}";

            CommandLine cmd = cmd(ctx);
            int exitCode =
                    cmd.execute(("config set --node-endpoint localhost:8081 "
                            + "local.baseline.autoAdjust.enabled=true --type node"
                    ).split(" "));

            assertThatExitCodeMeansSuccess(exitCode);

            verify(httpClient).send(requestCaptor.capture(), any());
            HttpRequest capturedRequest = requestCaptor.getValue();

            assertThat(capturedRequest.uri().toString(), is("http://localhost:8081/management/v1/configuration/node/"));
            assertThat(capturedRequest.method(), is("PATCH"));
            assertThat(requestBodyBytes(capturedRequest), is(expSentContent.getBytes(UTF_8)));
            assertThat(capturedRequest.headers().firstValue("Content-Type"), isPresentAnd(is("application/json")));

            assertOutputEqual("Configuration was updated successfully.\n\n"
                    + "Use the " + cmd.getColorScheme().commandText("ignite config get")
                    + " command to view the updated configuration.\n", out.toString(UTF_8));
            assertThatStderrIsEmpty();
        }

        //TODO: Fix in https://issues.apache.org/jira/browse/IGNITE-15306
        @Test
        @DisplayName(
                "set --node-endpoint localhost:8081 {\"local\":{\"baseline\":{\"autoAdjust\":{\"enabled\":true}}}} "
                        + "--type node"
        )
        void setJson() throws IOException, InterruptedException {
            when(res.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
            when(httpClient.<String>send(any(), any())).thenReturn(res);

            var expSentContent = "{\"local\":{\"baseline\":{\"autoAdjust\":{\"enabled\":true}}}}";

            CommandLine cmd = cmd(ctx);
            int exitCode =
                    cmd.execute(("config set --node-endpoint localhost:8081 "
                            + "local.baseline.autoAdjust.enabled=true --type node"
                    ).split(" "));

            assertThatExitCodeMeansSuccess(exitCode);

            verify(httpClient).send(requestCaptor.capture(), any());
            HttpRequest capturedRequest = requestCaptor.getValue();

            assertThat(capturedRequest.uri().toString(), is("http://localhost:8081/management/v1/configuration/node/"));
            assertThat(capturedRequest.method(), is("PATCH"));
            assertThat(requestBodyBytes(capturedRequest), is(expSentContent.getBytes(UTF_8)));
            assertThat(capturedRequest.headers().firstValue("Content-Type"), isPresentAnd(is("application/json")));

            assertOutputEqual("Configuration was updated successfully.\n\n"
                    + "Use the " + cmd.getColorScheme().commandText("ignite config get")
                    + " command to view the updated configuration.\n", out.toString(UTF_8));
            assertThatStderrIsEmpty();
        }
    }

    /**
     * Tests "cluster" command.
     */
    @Nested
    @DisplayName("cluster")
    class Cluster {
        @Mock
        private HttpClient httpClient;

        @Mock
        private HttpResponse<String> response;

        @BeforeEach
        void setUp() {
            ctx.registerSingleton(httpClient);
        }

        @Test
        @DisplayName("init --node-endpoint=127.0.0.1:17300 --meta-storage-node node1ConsistentId --meta-storage-node node2ConsistentId "
                + "--cmg-node node2ConsistentId --cmg-node node3ConsistentId")
        void initSuccess() throws Exception {
            when(response.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
            when(httpClient.<String>send(any(), any())).thenReturn(response);

            var expSentContent = "{\"metaStorageNodes\":[\"node1ConsistentId\",\"node2ConsistentId\"],"
                    + "\"cmgNodes\":[\"node2ConsistentId\",\"node3ConsistentId\"]}";

            CommandLine cmd = cmd(ctx);
            int exitCode =
                    cmd.execute("cluster", "init", "--node-endpoint=127.0.0.1:8081",
                            "--meta-storage-node", "node1ConsistentId", "--meta-storage-node", "node2ConsistentId",
                            "--cmg-node", "node2ConsistentId", "--cmg-node", "node3ConsistentId"
                    );

            assertThatExitCodeMeansSuccess(exitCode);

            verify(httpClient).send(requestCaptor.capture(), any());
            HttpRequest capturedRequest = requestCaptor.getValue();

            assertThat(capturedRequest.uri().toString(), is("http://127.0.0.1:8081/management/v1/cluster/init/"));
            assertThat(capturedRequest.method(), is("POST"));
            assertThat(requestBodyBytes(capturedRequest), is(expSentContent.getBytes(UTF_8)));
            assertThat(capturedRequest.headers().firstValue("Content-Type"), isPresentAnd(is("application/json")));

            assertThat(out.toString(UTF_8), is(platformizeNewLines("Cluster was initialized successfully.\n")));
            assertThatStderrIsEmpty();
        }

        @Test
        void initErrorWithWellFormedJsonResponseDisplaysPrettifiedJson() throws Exception {
            when(response.statusCode()).thenReturn(HttpURLConnection.HTTP_INTERNAL_ERROR);
            when(response.body()).thenReturn("{\"error\":{\"type\":\"INTERNAL_ERROR\",\"message\":\"Cannot elect leaders\"}}");
            when(httpClient.<String>send(any(), any())).thenReturn(response);

            CommandLine cmd = cmd(ctx);
            int exitCode =
                    cmd.execute("cluster", "init", "--node-endpoint=127.0.0.1:8081",
                            "--meta-storage-node", "node1ConsistentId", "--meta-storage-node", "node2ConsistentId",
                            "--cmg-node", "node2ConsistentId", "--cmg-node", "node3ConsistentId"
                    );

            assertThatExitCodeIs(1, exitCode);

            assertThatStdoutIsEmpty();
            assertThat(err.toString(UTF_8), startsWith(platformizeNewLines(
                    "org.apache.ignite.cli.IgniteCliException: Failed to initialize cluster\n"
                            + "\n"
                            + "{\n"
                            + "  \"error\" : {\n"
                            + "    \"type\" : \"INTERNAL_ERROR\",\n"
                            + "    \"message\" : \"Cannot elect leaders\"\n"
                            + "  }\n"
                            + "}")));
        }

        @Test
        void initErrorWithNonJsonResponse() throws Exception {
            when(response.statusCode()).thenReturn(HttpURLConnection.HTTP_INTERNAL_ERROR);
            when(response.body()).thenReturn("Oops");
            when(httpClient.<String>send(any(), any())).thenReturn(response);

            CommandLine cmd = cmd(ctx);
            int exitCode =
                    cmd.execute("cluster", "init", "--node-endpoint=127.0.0.1:8081",
                            "--meta-storage-node", "node1ConsistentId", "--meta-storage-node", "node2ConsistentId",
                            "--cmg-node", "node2ConsistentId", "--cmg-node", "node3ConsistentId"
                    );

            assertThatExitCodeIs(1, exitCode);

            assertThatStdoutIsEmpty();
            assertThat(err.toString(UTF_8), startsWith(platformizeNewLines(
                    "org.apache.ignite.cli.IgniteCliException: Failed to initialize cluster\n"
                            + "\n"
                            + "Oops")));
        }

        @Test
        @DisplayName("init --meta-storage-node node1ConsistentId --meta-storage-node node2ConsistentId "
                + "--cmg-node node2ConsistentId --cmg-node node3ConsistentId")
        void nodeEndpointIsMandatoryForInit() {
            CommandLine cmd = cmd(ctx);
            int exitCode =
                    cmd.execute("cluster", "init",
                            "--meta-storage-node", "node1ConsistentId", "--meta-storage-node", "node2ConsistentId",
                            "--cmg-node", "node2ConsistentId", "--cmg-node", "node3ConsistentId"
                    );

            assertThatExitCodeIs(2, exitCode);

            assertThatStdoutIsEmpty();
            assertThat(err.toString(UTF_8), startsWith("Missing required option: '--node-endpoint=<nodeEndpoint>'"));
        }

        @Test
        @DisplayName("init --node-endpoint=127.0.0.1:17300 --cmg-node node2ConsistentId --cmg-node node3ConsistentId")
        void metastorageNodesAreMandatoryForInit() {
            CommandLine cmd = cmd(ctx);
            int exitCode =
                    cmd.execute("cluster", "init", "--node-endpoint=127.0.0.1:8081",
                            "--cmg-node", "node2ConsistentId", "--cmg-node", "node3ConsistentId"
                    );

            assertThatExitCodeIs(2, exitCode);

            assertThatStdoutIsEmpty();
            assertThat(err.toString(UTF_8), startsWith("Missing required option: '--meta-storage-node=<metastorageNodes>'"));
        }

        @Test
        @DisplayName("init --node-endpoint=127.0.0.1:17300 --meta-storage-node node2ConsistentId --meta-storage-node node3ConsistentId")
        void cmgNodesAreNotMandatoryForInit() throws Exception {
            when(response.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
            when(httpClient.<String>send(any(), any())).thenReturn(response);

            CommandLine cmd = cmd(ctx);
            int exitCode =
                    cmd.execute("cluster", "init", "--node-endpoint=127.0.0.1:8081",
                            "--meta-storage-node", "node1ConsistentId", "--meta-storage-node", "node2ConsistentId"
                    );

            assertThatExitCodeMeansSuccess(exitCode);

            assertThat(out.toString(UTF_8), is(platformizeNewLines("Cluster was initialized successfully.\n")));
            assertThatStderrIsEmpty();
        }
    }

    private String platformizeNewLines(String str) {
        return str.replace("\n", System.lineSeparator());
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

    private byte[] requestBodyBytes(HttpRequest capturedRequest) {
        assertTrue(capturedRequest.bodyPublisher().isPresent());

        BodyPublisher jdkBodyPublisher = capturedRequest.bodyPublisher().get();
        ByteBuffer requestBodyBuffer = JdkFlowAdapter.flowPublisherToFlux(jdkBodyPublisher).blockFirst();
        assertThat(requestBodyBuffer, is(notNullValue()));

        byte[] bytes = new byte[requestBodyBuffer.remaining()];
        requestBodyBuffer.get(bytes);

        return bytes;
    }

    /**
     * <em>Assert</em> that {@code expected} and {@code actual} are equal.
     *
     * <p>If both are {@code null}, they are considered equal.
     *
     * @param exp Expected result.
     * @param actual Actual result.
     * @see Object#equals(Object)
     */
    private static void assertOutputEqual(String exp, String actual) {
        assertEquals(
                exp.lines().collect(toList()),
                actual.lines().collect(toList())
        );
    }
}
