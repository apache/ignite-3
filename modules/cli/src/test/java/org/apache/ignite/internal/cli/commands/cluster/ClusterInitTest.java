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

package org.apache.ignite.internal.cli.commands.cluster;

import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.serverError;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.copyResourceToTempFile;
import static org.junit.jupiter.api.Assertions.assertAll;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.ignite.internal.cli.commands.IgniteCliInterfaceTestBase;
import org.apache.ignite.internal.cli.commands.cluster.init.ClusterInitCommand;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Tests "cluster init" command. */
@DisplayName("cluster init")
@ExtendWith(WorkDirectoryExtension.class)
class ClusterInitTest extends IgniteCliInterfaceTestBase {
    private static final Pattern QUOTE_PATTERN = Pattern.compile("\"");
    private static final Pattern CR_PATTERN = Pattern.compile("\n");

    @Override
    protected Class<?> getCommandClass() {
        return ClusterInitCommand.class;
    }

    @Test
    void duplicatedOption() {
        execute(
                "--url", mockUrl,
                "--metastorage-group", "node1ConsistentId",
                "--metastorage-group", "node2ConsistentId", // we do not allow repeating options
                "--cluster-management-group", "node2ConsistentId",
                "--cluster-management-group", "node3ConsistentId", // and here
                "--name", "cluster"
        );

        assertErrOutputContains("Unmatched arguments");
    }

    @Test
    void wrongConfigFilePath() {
        stubFor(post("/management/v1/cluster/init")
                .willReturn(ok()));

        execute(
                "--url", mockUrl,
                "--metastorage-group", "node1ConsistentId",
                "--name", "cluster",
                "--config-files", "wrong-path"
        );

        assertErrOutputIs("Couldn't read cluster configuration file wrong-path");

        execute(
                "--url", mockUrl,
                "--metastorage-group", "node1ConsistentId",
                "--name", "cluster"
        );

        assertSuccessfulOutputContains("Cluster was initialized successfully.");
    }

    @Test
    void wrongConfigFile(@WorkDirectory Path workDir) throws IOException {
        Path configFile = Files.createTempFile(workDir, "config", "");
        Files.write(configFile, List.of("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));

        execute(
                "--url", mockUrl,
                "--metastorage-group", "node1ConsistentId",
                "--name", "cluster",
                "--config-files", configFile.toString()
        );

        assertErrOutputIs("Couldn't parse cluster configuration file " + configFile + "\n"
                + "String: 1: Key '<' may not be followed by token: '?' (Reserved character '?' is not allowed outside quotes)"
                + " (if you intended '?' (Reserved character '?' is not allowed outside quotes)"
                + " to be part of a key or string value, try enclosing the key or value in double quotes)"
        );
    }

    @Test
    @DisplayName("--url http://localhost:10300 --metastorage-group node1ConsistentId, node2ConsistentId "
            + "--cluster-management-group node2ConsistentId, node3ConsistentId --cluster-name cluster")
    void initSuccess() {
        var expectedSentContent = "{\"metaStorageNodes\":[\"node1ConsistentId\",\"node2ConsistentId\"],"
                + "\"cmgNodes\":[\"node2ConsistentId\",\"node3ConsistentId\"],"
                + "\"clusterName\":\"cluster\"}";

        returnOkForPostWithJson("/management/v1/cluster/init", expectedSentContent, true);

        execute(
                "--url", mockUrl,
                "--metastorage-group", "node1ConsistentId, node2ConsistentId",
                "--cluster-management-group", "node2ConsistentId,node3ConsistentId",
                "--name", "cluster"
        );

        assertSuccessfulOutputContains("Cluster was initialized successfully.");
    }

    @Test
    @DisplayName("--url http://localhost:10300 --cluster-name cluster")
    void initSuccessNoMsCmg() {
        var expectedSentContent = "{\"metaStorageNodes\":[],"
                + "\"cmgNodes\":[],"
                + "\"clusterName\":\"cluster\"}";

        returnOkForPostWithJson("/management/v1/cluster/init", expectedSentContent, true);

        execute(
                "--url", mockUrl,
                "--name", "cluster"
        );

        assertSuccessfulOutputContains("Cluster was initialized successfully.");
    }

    @Test
    @DisplayName("--url http://localhost:10300 --metastorage-group node1ConsistentId, node2ConsistentId"
            + " --cluster-management-group node2ConsistentId, node3ConsistentId --name cluster"
            + " --auth-enabled --basic-auth-username admin --basic-auth-password password")
    void initWithAuthenticationSuccess() throws IOException {

        Path clusterConfigurationFile = copyResourceToTempFile("cluster-configuration-with-enabled-auth.conf").toPath();
        String clusterConfiguration = Files.readString(clusterConfigurationFile);

        String expectedSentContent = "{"
                + "  \"metaStorageNodes\": ["
                + "    \"node1ConsistentId\","
                + "    \"node2ConsistentId\""
                + "  ],"
                + "  \"cmgNodes\": ["
                + "    \"node2ConsistentId\","
                + "    \"node3ConsistentId\""
                + "  ],"
                + "  \"clusterName\": \"cluster\","
                + "  \"clusterConfiguration\": \"" + escapedJson(clusterConfiguration) + "\""
                + "}";

        returnOkForPostWithJson("/management/v1/cluster/init", expectedSentContent, true);

        execute(
                "--url", mockUrl,
                "--metastorage-group", "node1ConsistentId,node2ConsistentId",
                "--cluster-management-group", " node2ConsistentId , node3ConsistentId",
                "--name", "cluster",
                "--config-files", clusterConfigurationFile.toString()
        );

        assertSuccessfulOutputContains("Cluster was initialized successfully.");
    }

    @Test
    void initError() {
        stubFor(post("/management/v1/cluster/init")
                .willReturn(serverError().withBody("{\"status\":500, \"detail\":\"Oops\"}")));

        execute(
                "--url", mockUrl,
                "--metastorage-group", "node1ConsistentId, node2ConsistentId",
                "--cluster-management-group", "node2ConsistentId, node3ConsistentId",
                "--name", "cluster"
        );

        assertAll(
                this::assertExitCodeIsError,
                () -> assertOutputContains("Initializing"), // Spinner output
                () -> assertErrOutputIs("Oops")
        );
    }

    @Test
    @DisplayName("--url http://localhost:10300 --cluster-management-group node2ConsistentId, node3ConsistentId")
    void metastorageNodesAreNotMandatoryForInit() {
        var expectedSentContent = "{"
                + "\"metaStorageNodes\":[],"
                + "\"cmgNodes\":[\"node2ConsistentId\",\"node3ConsistentId\"],"
                + "\"clusterName\":\"cluster\"}";

        returnOkForPostWithJson("/management/v1/cluster/init", expectedSentContent, true);

        execute(
                "--url", mockUrl,
                "--cluster-management-group", "node2ConsistentId, node3ConsistentId",
                "--name", "cluster"
        );

        assertSuccessfulOutputContains("Cluster was initialized successfully.");
    }

    @Test
    @DisplayName("--url http://localhost:10300 --metastorage-group node2ConsistentId, node3ConsistentId")
    void cmgNodesAreNotMandatoryForInit() {
        stubFor(post("/management/v1/cluster/init").willReturn(ok()));

        execute(
                "--url", mockUrl,
                "--metastorage-group", "node1ConsistentId, node2ConsistentId",
                "--name", "cluster"
        );

        assertSuccessfulOutputContains("Cluster was initialized successfully.");
    }

    @Test
    @DisplayName("--url http://localhost:10300 --metastorage-group node1ConsistentId --cluster-management-group node2ConsistentId")
    void clusterNameIsMandatoryForInit() {
        execute(
                "--url", mockUrl,
                "--metastorage-group", "node1ConsistentId",
                "--cluster-management-group", "node2ConsistentId"
        );

        assertAll(
                () -> assertExitCodeIs(2),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Missing required option: '--name=<clusterName>'")
        );
    }

    @Test
    void clusterInitFromMultipleConfigFiles() {
        String clusterConfigurationFile1 = copyResourceToTempFile("cluster-configuration-with-enabled-auth.conf").getAbsolutePath();
        String clusterConfigurationFile2 = copyResourceToTempFile("cluster-configuration-with-default.conf").getAbsolutePath();

        var expectedClusterConfiguration = "ignite.security: {\n"
                + "  enabled: true,\n"
                + "  authentication: {\n"
                + "    providers.default: {\n"
                + "      type: basic,\n"
                + "      users: [\n"
                + "        {\n"
                + "          username: admin,\n"
                + "          password: password\n"
                + "        },\n"
                + "        {\n"
                + "          username: admin1,\n"
                + "          password: password\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  }\n"
                + "}\n"
                + "ignite.schemaSync.delayDurationMillis: 100,\n"
                + "ignite.schemaSync.maxClockSkewMillis: 7,\n"
                + "ignite.system.idleSafeTimeSyncIntervalMillis: 10,\n"
                + "ignite.replication.idleSafeTimePropagationDurationMillis: 100";

        String expectedSentContent = "{"
                + "  \"metaStorageNodes\": ["
                + "    \"node1ConsistentId\""
                + "  ],"
                + "  \"cmgNodes\": ["
                + "    \"node2ConsistentId\""
                + "  ],"
                + "  \"clusterName\": \"cluster\","
                + "  \"clusterConfiguration\": \"" + escapedJson(expectedClusterConfiguration) + "\""
                + "}";

        returnOkForPostWithJson("/management/v1/cluster/init", expectedSentContent, true);

        execute(
                "--url", mockUrl,
                "--metastorage-group", "node1ConsistentId",
                "--cluster-management-group", "node2ConsistentId",
                "--name", "cluster",
                "--config-files", String.join(",", clusterConfigurationFile1, clusterConfigurationFile2)
        );

        assertSuccessfulOutputContains("Cluster was initialized successfully.");
    }

    private static String escapedJson(String configuration) {
        String json = ConfigFactory.parseString(configuration)
                .root().render(ConfigRenderOptions.concise().setFormatted(true).setJson(true));

        String quoted = QUOTE_PATTERN.matcher(json).replaceAll("\\\\\"");

        return CR_PATTERN.matcher(quoted).replaceAll("\\\\n");
    }
}
