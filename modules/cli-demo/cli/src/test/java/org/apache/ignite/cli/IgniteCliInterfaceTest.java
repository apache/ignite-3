package org.apache.ignite.cli;

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
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.Environment;
import org.apache.ignite.cli.builtins.init.InitIgniteCommand;
import org.apache.ignite.cli.builtins.module.ModuleManager;
import org.apache.ignite.cli.builtins.module.StandardModuleDefinition;
import org.apache.ignite.cli.builtins.node.NodeManager;
import org.apache.ignite.cli.spec.IgniteCliSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("ignite")
@ExtendWith(MockitoExtension.class)
public class IgniteCliInterfaceTest {

    ApplicationContext applicationContext;
    ByteArrayOutputStream err;
    ByteArrayOutputStream out;

    @Mock CliPathsConfigLoader cliPathsConfigLoader;

    @BeforeEach
    void setup() {
        applicationContext = ApplicationContext.run(Environment.TEST);
        applicationContext.registerSingleton(cliPathsConfigLoader);
        err = new ByteArrayOutputStream();
        out = new ByteArrayOutputStream();
    }

    CommandLine commandLine(ApplicationContext applicationContext) {
        CommandLine.IFactory factory = new CommandFactory(applicationContext);
        return new CommandLine(IgniteCliSpec.class, factory)
            .setErr(new PrintWriter(err, true))
            .setOut(new PrintWriter(out, true));
    }

    @DisplayName("init")
    @Nested
    class Init {

        @Test
        @DisplayName("init")
        void init() {
            var initIgniteCommand = mock(InitIgniteCommand.class);
            applicationContext.registerSingleton(InitIgniteCommand.class, initIgniteCommand);
            assertEquals(0, commandLine(applicationContext).execute("init"));
            verify(initIgniteCommand).init(any());
        }
    }

    @DisplayName("module")
    @Nested
    class Module {

        @Mock ModuleManager moduleManager;

        @BeforeEach
        void setUp() {
            applicationContext.registerSingleton(moduleManager);
        }

        @Test
        @DisplayName("add mvn:groupId:artifact:version")
        void add() {
            IgnitePaths paths = new IgnitePaths(Path.of("binDir"),
                Path.of("worksDir"), "version");
            when(cliPathsConfigLoader.loadIgnitePathsOrThrowError()).thenReturn(paths);

            var exitCode =
                commandLine(applicationContext).execute("module add mvn:groupId:artifactId:version".split(" "));
            verify(moduleManager).addModule("mvn:groupId:artifactId:version", paths, Arrays.asList());
            assertEquals(0, exitCode);
        }

        @Test
        @DisplayName("add mvn:groupId:artifact:version --repo http://mvnrepo.com/repostiory")
        void addWithCustomRepo() throws MalformedURLException {
            doNothing().when(moduleManager).addModule(any(), any(), any());

            IgnitePaths paths = new IgnitePaths(Path.of("binDir"),
                Path.of("worksDir"), "version");
            when(cliPathsConfigLoader.loadIgnitePathsOrThrowError()).thenReturn(paths);

            var exitCode =
                commandLine(applicationContext)
                    .execute("module add mvn:groupId:artifactId:version --repo http://mvnrepo.com/repostiory".split(" "));
            verify(moduleManager).addModule("mvn:groupId:artifactId:version", paths,
                Arrays.asList(new URL("http://mvnrepo.com/repostiory")));
            assertEquals(0, exitCode);
        }

        @Test
        @DisplayName("add demo-module")
        void addBuiltinModule() {
            doNothing().when(moduleManager).addModule(any(), any(), any());

            IgnitePaths paths = new IgnitePaths(Path.of("binDir"),
                Path.of("worksDir"), "version");
            when(cliPathsConfigLoader.loadIgnitePathsOrThrowError()).thenReturn(paths);

            var exitCode =
                commandLine(applicationContext).execute("module add demo-module".split(" "));
            verify(moduleManager).addModule("demo-module", paths, Collections.emptyList());
            assertEquals(0, exitCode);
        }

        @Test
        @DisplayName("remove builtin-module")
        void remove() {
            var moduleName = "builtin-module";
            when(moduleManager.removeModule(moduleName)).thenReturn(true);

            var exitCode =
                commandLine(applicationContext).execute("module remove builtin-module".split(" "));
            verify(moduleManager).removeModule(moduleName);
            assertEquals(0, exitCode);
            assertEquals("Module " + moduleName + " was uninstalled successfully.\n", out.toString());
        }

        @Test
        @DisplayName("remove unknown-module")
        void removeUnknownModule() {
            var moduleName = "unknown-module";
            when(moduleManager.removeModule(moduleName)).thenReturn(false);

            var exitCode =
                commandLine(applicationContext).execute("module remove unknown-module".split(" "));
            verify(moduleManager).removeModule(moduleName);
            assertEquals(0, exitCode);
            assertEquals("Nothing to do: module " + moduleName + " is not yet installed.\n", out.toString());
        }

        @Test
        @DisplayName("list")
        void list() {
           when(moduleManager.builtinModules()).thenReturn(Arrays.asList(
               new StandardModuleDefinition("module1", "description1", Arrays.asList("artifact1"), Arrays.asList("cli-artifact1") ),
               new StandardModuleDefinition("module2", "description2", Arrays.asList("artifact2"), Arrays.asList("cli-artifact2") )
           ));

           var exitCode =
               commandLine(applicationContext).execute("module list".split(" "));
           verify(moduleManager).builtinModules();
           assertEquals(0, exitCode);

           var expectedOutput =
               "+---------+--------------+\n" +
               "| Name    | Description  |\n" +
               "+---------+--------------+\n" +
               "| module1 | description1 |\n" +
               "+---------+--------------+\n" +
               "| module2 | description2 |\n" +
               "+---------+--------------+\n";
           assertEquals(expectedOutput, out.toString());
        }
    }

    @Nested
    @DisplayName("node")
    class Node {

        @Mock NodeManager nodeManager;

        @BeforeEach
        void setUp() {
            applicationContext.registerSingleton(nodeManager);
        }

        @Test
        @DisplayName("start node1 --config conf.json")
        void start() {
           var ignitePaths = new IgnitePaths(Path.of(""), Path.of(""), "version");
           var nodeName = "node1";
           var node =
               new NodeManager.RunningNode(1, nodeName, Path.of("logfile"));
           when(nodeManager.start(any(), any(), any(), any()))
               .thenReturn(node);

            when(cliPathsConfigLoader.loadIgnitePathsOrThrowError())
                .thenReturn(ignitePaths);

            var exitCode =
                commandLine(applicationContext).execute(("node start " + nodeName + " --config conf.json").split(" "));

            assertEquals(0, exitCode);
            verify(nodeManager).start(nodeName, ignitePaths.workDir, ignitePaths.cliPidsDir(), Path.of("conf.json"));
            assertEquals("Started ignite node.\n" +
                "PID: 1\n" +
                "Consistent Id: node1\n" +
                "Log file: logfile\n", out.toString());
        }

        @Test
        @DisplayName("stop node1")
        void stopRunning() {
            var ignitePaths = new IgnitePaths(Path.of(""), Path.of(""), "version");
            var nodeName = "node1";
            when(nodeManager.stopWait(any(), any()))
                .thenReturn(true);

            when(cliPathsConfigLoader.loadIgnitePathsOrThrowError())
                .thenReturn(ignitePaths);

            var exitCode =
                commandLine(applicationContext).execute(("node stop " + nodeName).split(" "));

            assertEquals(0, exitCode);
            verify(nodeManager).stopWait(nodeName, ignitePaths.cliPidsDir());
            assertEquals("Node with consistent id " + nodeName + " was stopped\n", out.toString());
        }

        @Test
        @DisplayName("stop unknown-node")
        void stopUnknown() {
            var ignitePaths = new IgnitePaths(Path.of(""), Path.of(""), "version");
            var nodeName = "unknown-node";
            when(nodeManager.stopWait(any(), any()))
                .thenReturn(false);

            when(cliPathsConfigLoader.loadIgnitePathsOrThrowError())
                .thenReturn(ignitePaths);

            var exitCode =
                commandLine(applicationContext).execute(("node stop " + nodeName).split(" "));

            assertEquals(0, exitCode);
            verify(nodeManager).stopWait(nodeName, ignitePaths.cliPidsDir());
            assertEquals("Stop of node " + nodeName + " was failed\n", out.toString());
        }

        @Test
        @DisplayName("list")
        void list() {
            var ignitePaths = new IgnitePaths(Path.of(""), Path.of(""), "version");
            when(nodeManager.getRunningNodes(any(), any()))
                .thenReturn(Arrays.asList(
                    new NodeManager.RunningNode(1, "new1", Path.of("logFile1")),
                    new NodeManager.RunningNode(2, "new2", Path.of("logFile2"))
                ));

            when(cliPathsConfigLoader.loadIgnitePathsOrThrowError())
                .thenReturn(ignitePaths);

            var exitCode =
                commandLine(applicationContext).execute("node list".split(" "));

            assertEquals(0, exitCode);
            verify(nodeManager).getRunningNodes(ignitePaths.workDir, ignitePaths.cliPidsDir());
            assertEquals("+-----+---------------+----------+\n" +
                "| PID | Consistent ID | Log      |\n" +
                "+-----+---------------+----------+\n" +
                "| 1   | new1          | logFile1 |\n" +
                "+-----+---------------+----------+\n" +
                "| 2   | new2          | logFile2 |\n" +
                "+-----+---------------+----------+\n\n", out.toString());
        }

        @Test
        @DisplayName("list")
        void listEmpty() {
            var ignitePaths = new IgnitePaths(Path.of(""), Path.of(""), "version");
            when(nodeManager.getRunningNodes(any(), any()))
                .thenReturn(Arrays.asList());

            when(cliPathsConfigLoader.loadIgnitePathsOrThrowError())
                .thenReturn(ignitePaths);

            var exitCode =
                commandLine(applicationContext).execute("node list".split(" "));

            assertEquals(0, exitCode);
            verify(nodeManager).getRunningNodes(ignitePaths.workDir, ignitePaths.cliPidsDir());
            assertEquals("Currently, there are no locally running nodes.\n\n" +
                "Use the ignite node start command to start a new node.\n", out.toString());
        }

        @Test
        @DisplayName("classpath")
        void classpath() throws IOException {
            when(nodeManager.classpathItems()).thenReturn(Arrays.asList("item1", "item2"));

            var exitCode = commandLine(applicationContext).execute("node classpath".split(" "));

            assertEquals(0, exitCode);
            verify(nodeManager).classpathItems();
            assertEquals("Current Ignite node classpath\n  item1\n  item2\n", out.toString());
        }
    }

    @Nested
    @DisplayName("config")
    class Config {

        @Mock private HttpClient httpClient;
        @Mock private HttpResponse<String> response;

        @BeforeEach
        void setUp() {
            applicationContext.registerSingleton(httpClient);
        }

        @Test
        @DisplayName("get --node-endpoint localhost:8081")
        void get() throws IOException, InterruptedException {
            when(response.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
            when(response.body()).thenReturn("{\"baseline\":{\"autoAdjust\":{\"enabled\":true}}}");
            when(httpClient.<String>send(any(), any())).thenReturn(response);

            var exitCode =
                commandLine(applicationContext).execute("config get --node-endpoint localhost:8081".split(" "));

            assertEquals(0, exitCode);
            verify(httpClient).send(
                argThat(r -> r.uri().toString().equals("http://localhost:8081/management/v1/configuration/") &&
                    r.headers().firstValue("Content-Type").get().equals("application/json")),
                any());
            assertEquals("{\n" +
                "  \"baseline\" : {\n" +
                "    \"autoAdjust\" : {\n" +
                "      \"enabled\" : true\n" +
                "    }\n" +
                "  }\n" +
                "}\n", out.toString());
        }

        @Test
        @DisplayName("get --node-endpoint localhost:8081 --subtree local.baseline")
        void getSubtree() throws IOException, InterruptedException {
            when(response.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
            when(response.body()).thenReturn("{\"autoAdjust\":{\"enabled\":true}}");
            when(httpClient.<String>send(any(), any())).thenReturn(response);

            var exitCode =
                commandLine(applicationContext).execute(("config get --node-endpoint localhost:8081 " +
                    "--selector local.baseline").split(" "));

            assertEquals(0, exitCode);
            verify(httpClient).send(
                argThat(r -> r.uri().toString().equals("http://localhost:8081/management/v1/configuration/local.baseline") &&
                    r.headers().firstValue("Content-Type").get().equals("application/json")),
                any());
            assertEquals("{\n" +
                "  \"autoAdjust\" : {\n" +
                "    \"enabled\" : true\n" +
                "  }\n" +
                "}\n", out.toString());
        }

        @Test
        @DisplayName("set --node-endpoint localhost:8081 local.baseline.autoAdjust.enabled=true")
        void setHocon() throws IOException, InterruptedException {
            when(response.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
            when(httpClient.<String>send(any(), any())).thenReturn(response);

            var expectedSentContent = "{\"local\":{\"baseline\":{\"autoAdjust\":{\"enabled\":true}}}}";

            var exitCode =
                commandLine(applicationContext).execute(("config set --node-endpoint localhost:8081 " +
                    "local.baseline.autoAdjust.enabled=true"
                    ).split(" "));

            assertEquals(0, exitCode);
            verify(httpClient).send(
                argThat(r -> r.uri().toString().equals("http://localhost:8081/management/v1/configuration/") &&
                    r.method().equals("POST") &&
                    // TODO: body matcher should be fixed to more appropriate
                    r.bodyPublisher().get().contentLength() == expectedSentContent.getBytes().length &&
                    r.headers().firstValue("Content-Type").get().equals("application/json")),
                any());
            assertEquals("Configuration successfully updated.\n", out.toString());
        }

        @Test
        @DisplayName("set --node-endpoint localhost:8081 {\"local\":{\"baseline\":{\"autoAdjust\":{\"enabled\":true}}}}")
        void setJson() throws IOException, InterruptedException {
            when(response.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
            when(httpClient.<String>send(any(), any())).thenReturn(response);

            var expectedSentContent = "{\"local\":{\"baseline\":{\"autoAdjust\":{\"enabled\":true}}}}";

            var exitCode =
                commandLine(applicationContext).execute(("config set --node-endpoint localhost:8081 " +
                    "local.baseline.autoAdjust.enabled=true"
                ).split(" "));

            assertEquals(0, exitCode);
            verify(httpClient).send(
                argThat(r -> r.uri().toString().equals("http://localhost:8081/management/v1/configuration/") &&
                    r.method().equals("POST") &&
                    // TODO: body matcher should be fixed to more appropriate
                    r.bodyPublisher().get().contentLength() == expectedSentContent.getBytes().length &&
                    r.headers().firstValue("Content-Type").get().equals("application/json")),
                any());
            assertEquals("Configuration successfully updated.\n", out.toString());

        }
    }
}
