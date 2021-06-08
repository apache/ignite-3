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

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.Environment;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import org.apache.ignite.cli.builtins.init.InitIgniteCommand;
import org.apache.ignite.cli.builtins.module.ModuleManager;
import org.apache.ignite.cli.builtins.module.ModuleRegistry;
import org.apache.ignite.cli.builtins.module.StandardModuleDefinition;
import org.apache.ignite.cli.builtins.node.NodeManager;
import org.apache.ignite.cli.spec.IgniteCliSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import picocli.CommandLine;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Smoke test for Ignite CLI features and its UI.
 * Structure of tests should be self-documented
 * and repeat the structure of Ignite CLI subcommands.
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

    /** */
    @Mock
    CliPathsConfigLoader cliPathsCfgLdr;

    /** */
    @BeforeEach
    void setup() {
        ctx = ApplicationContext.run(Environment.TEST);

        ctx.registerSingleton(cliPathsCfgLdr);

        err = new ByteArrayOutputStream();
        out = new ByteArrayOutputStream();
    }

    @AfterEach
    private void tearDown() {
        ctx.stop();
    }

    /** */
    CommandLine cmd(ApplicationContext applicationCtx) {
        CommandLine.IFactory factory = new CommandFactory(applicationCtx);

        return new CommandLine(IgniteCliSpec.class, factory)
            .setErr(new PrintWriter(err, true))
            .setOut(new PrintWriter(out, true));
    }

    IgnitePaths ignitePaths = new IgnitePaths(
            Path.of("bin"),
            Path.of("work"),
            Path.of("config"),
            Path.of("log"),
            "version");

    /** */
    @DisplayName("init")
    @Nested
    class Init {
        /** */
        @Test
        @DisplayName("init")
        void init() {
            var initIgniteCmd = mock(InitIgniteCommand.class);

            ctx.registerSingleton(InitIgniteCommand.class, initIgniteCmd);

            CommandLine cli = cmd(ctx);

            Assertions.assertEquals(0, cli.execute("init"));
            verify(initIgniteCmd).init(any(), any(), any());
        }
    }

    /** */
    @DisplayName("module")
    @Nested
    class Module {
        /** */
        @Mock
        ModuleManager moduleMgr;

        /** */
        @Mock
        ModuleRegistry moduleRegistry;

        /** */
        @BeforeEach
        void setUp() {
            ctx.registerSingleton(moduleMgr);
            ctx.registerSingleton(moduleRegistry);
        }

        /** */
        @Test
        @DisplayName("add mvn:groupId:artifact:version")
        void add() {
            when(cliPathsCfgLdr.loadIgnitePathsOrThrowError()).thenReturn(ignitePaths);

            var exitCode =
                cmd(ctx).execute("module add mvn:groupId:artifactId:version".split(" "));

            verify(moduleMgr).addModule("mvn:groupId:artifactId:version", ignitePaths, Collections.emptyList());
            Assertions.assertEquals(0, exitCode);
        }

        /** */
        @Test
        @DisplayName("add mvn:groupId:artifact:version --repo http://mvnrepo.com/repostiory")
        void addWithCustomRepo() throws MalformedURLException {
            doNothing().when(moduleMgr).addModule(any(), any(), any());

            when(cliPathsCfgLdr.loadIgnitePathsOrThrowError()).thenReturn(ignitePaths);

            var exitCode =
                cmd(ctx)
                    .execute(
                        "module add mvn:groupId:artifactId:version --repo http://mvnrepo.com/repostiory".split(" "));

            verify(moduleMgr).addModule(
                "mvn:groupId:artifactId:version",
                ignitePaths,
                Collections.singletonList(new URL("http://mvnrepo.com/repostiory")));
            Assertions.assertEquals(0, exitCode);
        }

        /** */
        @Test
        @DisplayName("add test-module")
        void addBuiltinModule() {
            doNothing().when(moduleMgr).addModule(any(), any(), any());

            when(cliPathsCfgLdr.loadIgnitePathsOrThrowError()).thenReturn(ignitePaths);

            var exitCode =
                cmd(ctx).execute("module add test-module".split(" "));

            verify(moduleMgr).addModule("test-module", ignitePaths, Collections.emptyList());
            Assertions.assertEquals(0, exitCode);
        }

        /** */
        @Test
        @DisplayName("remove builtin-module")
        void remove() {
            var moduleName = "builtin-module";

            when(moduleMgr.removeModule(moduleName)).thenReturn(true);

            var cmd = cmd(ctx);
            var exitCode =
                cmd.execute("module remove builtin-module".split(" "));

            verify(moduleMgr).removeModule(moduleName);
            Assertions.assertEquals(0, exitCode);
            assertEquals("Module " + cmd.getColorScheme().parameterText(moduleName) +
                " was removed successfully.\n", out.toString());
        }

        /** */
        @Test
        @DisplayName("remove unknown-module")
        void removeUnknownModule() {
            var moduleName = "unknown-module";

            when(moduleMgr.removeModule(moduleName)).thenReturn(false);

            var cmd = cmd(ctx);
            var exitCode =
                cmd(ctx).execute("module remove unknown-module".split(" "));

            verify(moduleMgr).removeModule(moduleName);
            Assertions.assertEquals(0, exitCode);
            assertEquals("Nothing to do: module " + cmd.getColorScheme().parameterText(moduleName) +
                " is not yet added.\n", out.toString());
        }

        /** */
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

            var cmd = cmd(ctx);
            var exitCode =
                cmd.execute("module list".split(" "));

            verify(moduleMgr).builtinModules();
            Assertions.assertEquals(0, exitCode);

            var expOutput = cmd.getColorScheme().text("@|bold Optional Ignite Modules|@\n" +
                "+---------+--------------+------------+\n" +
                "| @|bold Name|@    | @|bold Description|@  | @|bold Installed?|@ |\n" +
                "+---------+--------------+------------+\n" +
                "| module1 | description1 | Yes        |\n" +
                "+---------+--------------+------------+\n" +
                "| module2 | description2 | No         |\n" +
                "+---------+--------------+------------+\n" +
                "\n" +
                "@|bold Additional Maven Dependencies|@\n" +
                "+-------------------+-------------+---------+\n" +
                "| @|bold Group ID|@          | @|bold Artifact ID|@ | @|bold Version|@ |\n" +
                "+-------------------+-------------+---------+\n" +
                "| org.apache.ignite | snapshot    | 2.9.0   |\n" +
                "+-------------------+-------------+---------+\n" +
                "Type " + cmd.getColorScheme().commandText("ignite module remove") + " " +
                cmd.getColorScheme().parameterText("<groupId>:<artifactId>:<version>") +
                " to remove a dependency.\n"
            ).toString();
            assertEquals(expOutput, out.toString());
        }
    }

    /** */
    @Nested
    @DisplayName("node")
    class Node {
        /** */
        @Mock
        NodeManager nodeMgr;

        /** */
        @BeforeEach
        void setUp() {
            ctx.registerSingleton(nodeMgr);
        }

        /** */
        @Test
        @DisplayName("start node1 --config conf.json")
        void start() {
            var nodeName = "node1";

            var node =
                new NodeManager.RunningNode(1, nodeName, Path.of("logfile"));

            when(nodeMgr.start(any(), any(), any(), any(), any()))
                .thenReturn(node);

            when(cliPathsCfgLdr.loadIgnitePathsOrThrowError())
                .thenReturn(ignitePaths);

            CommandLine cli = cmd(ctx);

            var exitCode = cli.execute(("node start " + nodeName + " --config conf.json").split(" "));

            Assertions.assertEquals(0, exitCode);

            verify(nodeMgr).start(
                nodeName,
                ignitePaths.logDir,
                ignitePaths.cliPidsDir(),
                Path.of("conf.json"),
                cli.getOut());

            assertEquals("\nNode is successfully started. To stop, type ignite node stop " + nodeName + "\n\n" +
                "+-----------+---------+\n" +
                "| Node name | node1   |\n" +
                "+-----------+---------+\n" +
                "| PID       | 1       |\n" +
                "+-----------+---------+\n" +
                "| Log File  | logfile |\n" +
                "+-----------+---------+\n",
                out.toString());
        }

        /** */
        @Test
        @DisplayName("stop node1")
        void stopRunning() {
            var nodeName = "node1";

            when(nodeMgr.stopWait(any(), any()))
                .thenReturn(true);

            when(cliPathsCfgLdr.loadIgnitePathsOrThrowError())
                .thenReturn(ignitePaths);

            var cmd = cmd(ctx);
            var exitCode =
                cmd.execute(("node stop " + nodeName).split(" "));

            Assertions.assertEquals(0, exitCode);
            verify(nodeMgr).stopWait(nodeName, ignitePaths.cliPidsDir());
            assertEquals(
                "Stopping locally running node with consistent ID " +
                    cmd.getColorScheme().parameterText(nodeName) +
                    cmd.getColorScheme().text("... @|bold,green Done!|@\n"),
                out.toString());
        }

        /** */
        @Test
        @DisplayName("stop unknown-node")
        void stopUnknown() {
            var nodeName = "unknown-node";

            when(nodeMgr.stopWait(any(), any()))
                .thenReturn(false);

            when(cliPathsCfgLdr.loadIgnitePathsOrThrowError())
                .thenReturn(ignitePaths);

            var cmd = cmd(ctx);
            var exitCode =
                cmd.execute(("node stop " + nodeName).split(" "));

            Assertions.assertEquals(0, exitCode);
            verify(nodeMgr).stopWait(nodeName, ignitePaths.cliPidsDir());
            assertEquals(
                "Stopping locally running node with consistent ID " +
                    cmd.getColorScheme().parameterText(nodeName) +
                    cmd.getColorScheme().text("... @|bold,red Failed|@\n"),
                out.toString());
        }

        /** */
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

            var cmd = cmd(ctx);
            var exitCode =
                cmd.execute("node list".split(" "));

            Assertions.assertEquals(0, exitCode);
            verify(nodeMgr).getRunningNodes(ignitePaths.logDir, ignitePaths.cliPidsDir());
            assertEquals(cmd.getColorScheme().text("Currently, there are @|bold 2|@ locally running nodes.\n\n") +
                "+---------------+-----+----------+\n" +
                cmd.getColorScheme().text("| @|bold Consistent ID|@ | @|bold PID|@ | @|bold Log File|@ |\n") +
                "+---------------+-----+----------+\n" +
                "| new1          | 1   | logFile1 |\n" +
                "+---------------+-----+----------+\n" +
                "| new2          | 2   | logFile2 |\n" +
                "+---------------+-----+----------+\n",
                out.toString());
        }

        /** */
        @Test
        @DisplayName("list")
        void listEmpty() {
            when(nodeMgr.getRunningNodes(any(), any()))
                .thenReturn(Collections.emptyList());

            when(cliPathsCfgLdr.loadIgnitePathsOrThrowError())
                .thenReturn(ignitePaths);

            var cmd = cmd(ctx);
            var exitCode =
                cmd.execute("node list".split(" "));

            Assertions.assertEquals(0, exitCode);
            verify(nodeMgr).getRunningNodes(ignitePaths.logDir, ignitePaths.cliPidsDir());
            assertEquals("Currently, there are no locally running nodes.\n\n" +
                "Use the " + cmd.getColorScheme().commandText("ignite node start") + " command to start a new node.\n",
                out.toString());
        }

        /** */
        @Test
        @DisplayName("classpath")
        void classpath() throws IOException {
            when(nodeMgr.classpathItems()).thenReturn(Arrays.asList("item1", "item2"));

            var cmd = cmd(ctx);
            var exitCode = cmd.execute("node classpath".split(" "));

            Assertions.assertEquals(0, exitCode);
            verify(nodeMgr).classpathItems();
            assertEquals(
                cmd.getColorScheme().text(
                    "@|bold Current Ignite node classpath:|@\n    item1\n    item2\n").toString(),
                out.toString());
        }
    }

    /** */
    @Nested
    @DisplayName("config")
    class Config {
        /** */
        @Mock
        private HttpClient httpClient;

        /** */
        @Mock
        private HttpResponse<String> res;

        /** */
        @BeforeEach
        void setUp() {
            ctx.registerSingleton(httpClient);
        }

        /** */
        @Test
        @DisplayName("get --node-endpoint localhost:8081")
        void get() throws IOException, InterruptedException {
            when(res.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
            when(res.body()).thenReturn("{\"baseline\":{\"autoAdjust\":{\"enabled\":true}}}");
            when(httpClient.<String>send(any(), any())).thenReturn(res);

            var exitCode =
                cmd(ctx).execute("config get --node-endpoint localhost:8081".split(" "));

            Assertions.assertEquals(0, exitCode);
            verify(httpClient).send(
                argThat(r -> "http://localhost:8081/management/v1/configuration/".equals(r.uri().toString()) &&
                    "application/json".equals(r.headers().firstValue("Content-Type").get())),
                any());
            assertEquals("{\n" +
                "  \"baseline\" : {\n" +
                "    \"autoAdjust\" : {\n" +
                "      \"enabled\" : true\n" +
                "    }\n" +
                "  }\n" +
                "}\n", out.toString());
        }

        /** */
        @Test
        @DisplayName("get --node-endpoint localhost:8081 --selector local.baseline")
        void getSubtree() throws IOException, InterruptedException {
            when(res.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
            when(res.body()).thenReturn("{\"autoAdjust\":{\"enabled\":true}}");
            when(httpClient.<String>send(any(), any())).thenReturn(res);

            var exitCode =
                cmd(ctx).execute(("config get --node-endpoint localhost:8081 " +
                    "--selector local.baseline").split(" "));

            Assertions.assertEquals(0, exitCode);
            verify(httpClient).send(
                argThat(r ->
                    "http://localhost:8081/management/v1/configuration/local.baseline".equals(r.uri().toString()) &&
                    "application/json".equals(r.headers().firstValue("Content-Type").get())),
                any());
            assertEquals("{\n" +
                "  \"autoAdjust\" : {\n" +
                "    \"enabled\" : true\n" +
                "  }\n" +
                "}\n", out.toString());
        }

        /** */
        @Test
        @DisplayName("set --node-endpoint localhost:8081 local.baseline.autoAdjust.enabled=true")
        void setHocon() throws IOException, InterruptedException {
            when(res.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
            when(httpClient.<String>send(any(), any())).thenReturn(res);

            var expSentContent = "{\"local\":{\"baseline\":{\"autoAdjust\":{\"enabled\":true}}}}";

            var cmd = cmd(ctx);
            var exitCode =
                cmd.execute(("config set --node-endpoint localhost:8081 " +
                    "local.baseline.autoAdjust.enabled=true"
                    ).split(" "));

            Assertions.assertEquals(0, exitCode);
            verify(httpClient).send(
                argThat(r -> "http://localhost:8081/management/v1/configuration/".equals(r.uri().toString()) &&
                    "POST".equals(r.method()) &&
                    r.bodyPublisher().get().contentLength() == expSentContent.getBytes().length &&
                    "application/json".equals(r.headers().firstValue("Content-Type").get())),
                any());
            assertEquals("Configuration was updated successfully.\n\n" +
                "Use the " + cmd.getColorScheme().commandText("ignite config get") +
                " command to view the updated configuration.\n", out.toString());
        }

        /** */
        @Test
        @DisplayName("set --node-endpoint localhost:8081 {\"local\":{\"baseline\":{\"autoAdjust\":{\"enabled\":true}}}}")
        void setJson() throws IOException, InterruptedException {
            when(res.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
            when(httpClient.<String>send(any(), any())).thenReturn(res);

            var expSentContent = "{\"local\":{\"baseline\":{\"autoAdjust\":{\"enabled\":true}}}}";

            var cmd = cmd(ctx);
            var exitCode =
                cmd.execute(("config set --node-endpoint localhost:8081 " +
                    "local.baseline.autoAdjust.enabled=true"
                ).split(" "));

            Assertions.assertEquals(0, exitCode);
            verify(httpClient).send(
                argThat(r -> "http://localhost:8081/management/v1/configuration/".equals(r.uri().toString()) &&
                    "POST".equals(r.method()) &&
                    r.bodyPublisher().get().contentLength() == expSentContent.getBytes().length &&
                    "application/json".equals(r.headers().firstValue("Content-Type").get())),
                any());
            assertEquals("Configuration was updated successfully.\n\n" +
                "Use the " + cmd.getColorScheme().commandText("ignite config get") +
                " command to view the updated configuration.\n", out.toString());
        }
    }

    /** */
    private static void assertEquals(String exp, String actual) {
        Assertions.assertEquals(
            exp.lines().collect(Collectors.toList()),
            actual.lines().collect(Collectors.toList())
        );
    }
}
