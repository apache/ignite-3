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

import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.copyResourceToTempFile;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockserver.matchers.MatchType.ONLY_MATCHING_FIELDS;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.HttpStatusCode.INTERNAL_SERVER_ERROR_500;
import static org.mockserver.model.HttpStatusCode.OK_200;
import static org.mockserver.model.JsonBody.json;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import org.apache.ignite.internal.cli.commands.IgniteCliInterfaceTestBase;
import org.apache.ignite.internal.cli.commands.cluster.init.ClusterInitCommand;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockserver.model.MediaType;

/** Tests "cluster init" command. */
@DisplayName("cluster init")
class ClusterInitTest extends IgniteCliInterfaceTestBase {
    private static final Pattern PATTERN = Pattern.compile("\"");

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
        execute(
                "--url", mockUrl,
                "--metastorage-group", "node1ConsistentId",
                "--name", "cluster",
                "--config-files", "wrong-path"
        );

        assertErrOutputIs("Couldn't read cluster configuration file: [wrong-path]");
    }

    @Test
    @DisplayName("--url http://localhost:10300 --metastorage-group node1ConsistentId, node2ConsistentId "
            + "--cluster-management-group node2ConsistentId, node3ConsistentId --cluster-name cluster")
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

        execute(
                "--url", mockUrl,
                "--metastorage-group", "node1ConsistentId, node2ConsistentId",
                "--cluster-management-group", "node2ConsistentId,node3ConsistentId",
                "--name", "cluster"
        );

        assertSuccessfulOutputIs("Cluster was initialized successfully");
    }

    @Test
    @DisplayName("--url http://localhost:10300 --metastorage-group node1ConsistentId, node2ConsistentId"
            + " --cluster-management-group node2ConsistentId, node3ConsistentId --name cluster"
            + " --auth-enabled --basic-auth-username admin --basic-auth-password password")
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
                + "  \"clusterConfiguration\": \"" + escapedJson(clusterConfiguration) + "\"\n"
                + "}";

        clientAndServer
                .when(request()
                        .withMethod("POST")
                        .withPath("/management/v1/cluster/init")
                        .withBody(json(expectedSentContent, ONLY_MATCHING_FIELDS))
                        .withContentType(MediaType.APPLICATION_JSON_UTF_8)
                )
                .respond(response(null));

        execute(
                "--url", mockUrl,
                "--metastorage-group", "node1ConsistentId,node2ConsistentId",
                "--cluster-management-group", " node2ConsistentId , node3ConsistentId",
                "--name", "cluster",
                "--config-files", clusterConfigurationFile.toString()
        );

        assertSuccessfulOutputIs("Cluster was initialized successfully");
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

        execute(
                "--url", mockUrl,
                "--metastorage-group", "node1ConsistentId, node2ConsistentId",
                "--cluster-management-group", "node2ConsistentId, node3ConsistentId",
                "--name", "cluster"
        );

        assertAll(
                this::assertExitCodeIsError,
                this::assertOutputIsEmpty,
                () -> assertErrOutputIs("Oops")
        );
    }

    @Test
    @DisplayName("--url http://localhost:10300 --cluster-management-group node2ConsistentId, node3ConsistentId")
    void metastorageNodesAreMandatoryForInit() {
        execute(
                "--url", mockUrl,
                "--cluster-management-group", "node2ConsistentId, node3ConsistentId",
                "--name", "cluster"
        );

        assertAll(
                () -> assertExitCodeIs(2),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Missing required option: '--metastorage-group=<node name>'")
        );
    }

    @Test
    @DisplayName("--url http://localhost:10300 --metastorage-group node2ConsistentId, node3ConsistentId")
    void cmgNodesAreNotMandatoryForInit() {
        clientAndServer
                .when(request()
                        .withMethod("POST")
                        .withPath("/management/v1/cluster/init")
                )
                .respond(response().withStatusCode(OK_200.code()));

        execute(
                "--url", mockUrl,
                "--metastorage-group", "node1ConsistentId, node2ConsistentId",
                "--name", "cluster"
        );

        assertSuccessfulOutputIs("Cluster was initialized successfully");
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
                + "ignite.schemaSync.delayDuration: 100,\n"
                + "ignite.schemaSync.maxClockSkew: 7,\n"
                + "ignite.metaStorage.idleSyncTimeInterval: 10,\n"
                + "ignite.replication.idleSafeTimePropagationDuration: 100";

        var expectedSentContent = "{\n"
                + "  \"metaStorageNodes\": [\n"
                + "    \"node1ConsistentId\"\n"
                + "  ],\n"
                + "  \"cmgNodes\": [\n"
                + "    \"node2ConsistentId\"\n"
                + "  ],\n"
                + "  \"clusterName\": \"cluster\",\n"
                + "  \"clusterConfiguration\": \"" + escapedJson(expectedClusterConfiguration) + "\"\n"
                + "}";

        clientAndServer
                .when(request()
                        .withMethod("POST")
                        .withPath("/management/v1/cluster/init")
                        .withBody(json(expectedSentContent, ONLY_MATCHING_FIELDS))
                        .withContentType(MediaType.APPLICATION_JSON_UTF_8)
                )
                .respond(response(null));

        execute(
                "--url", mockUrl,
                "--metastorage-group", "node1ConsistentId",
                "--cluster-management-group", "node2ConsistentId",
                "--name", "cluster",
                "--config-files", String.join(",", clusterConfigurationFile1, clusterConfigurationFile2)
        );

        assertSuccessfulOutputIs("Cluster was initialized successfully");
    }

    private static String escapedJson(String configuration) {
        String json = ConfigFactory.parseString(configuration)
                .root().render(ConfigRenderOptions.concise().setFormatted(true).setJson(true));

        return PATTERN.matcher(json).replaceAll("\\\\\"");
    }
}
