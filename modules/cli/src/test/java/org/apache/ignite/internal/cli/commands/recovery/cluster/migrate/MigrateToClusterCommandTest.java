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

package org.apache.ignite.internal.cli.commands.recovery.cluster.migrate;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.JsonBody.json;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.cli.commands.IgniteCliInterfaceTestBase;
import org.apache.ignite.rest.client.model.ClusterState;
import org.apache.ignite.rest.client.model.ClusterTag;
import org.apache.ignite.rest.client.model.MigrateRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockserver.model.MediaType;

/** Tests "recovery cluster migrate" commands. */
@DisplayName("recovery cluster migrate")
class MigrateToClusterCommandTest extends IgniteCliInterfaceTestBase {
    @Test
    @DisplayName("--old-cluster-url http://localhost:10300 --new-cluster-url http://localhost:10300")
    void migrate() {
        respondWithClusterState();
        expectMigrateRequest();

        execute("recovery cluster migrate --old-cluster-url " + mockUrl + " --new-cluster-url " + mockUrl);

        assertSuccessfulOutputIs("Successfully initiated migration.");
    }

    @Test
    @DisplayName("--old-cluster-url http://localhost:10300/ --new-cluster-url http://localhost:10300/")
    void trailingSlash() {
        respondWithClusterState();
        expectMigrateRequest();

        execute("recovery cluster migrate --old-cluster-url " + mockUrl + "/ --new-cluster-url " + mockUrl + "/");

        assertSuccessfulOutputIs("Successfully initiated migration.");
    }

    private static void respondWithClusterState() {
        ClusterState clusterState = new ClusterState()
                .cmgNodes(List.of("node"))
                .msNodes(List.of("node"))
                .igniteVersion("version")
                .clusterTag(
                        new ClusterTag()
                                .clusterName("clusterName")
                                .clusterId(UUID.fromString("e0655494-eb02-4757-89db-ae562a170280"))
                );

        clientAndServer
                .when(request()
                        .withMethod("GET")
                        .withPath("/management/v1/cluster/state")
                )
                .respond(response(clusterState.toJson()));
    }

    private static void expectMigrateRequest() {
        MigrateRequest migrateRequest = new MigrateRequest()
                .cmgNodes(List.of("node"))
                .metaStorageNodes(List.of("node"))
                .version("version")
                .clusterName("clusterName")
                .clusterId(UUID.fromString("e0655494-eb02-4757-89db-ae562a170280"));

        clientAndServer
                .when(request()
                        .withMethod("POST")
                        .withPath("/management/v1/recovery/cluster/migrate")
                        .withBody(json(migrateRequest.toJson()))
                        .withContentType(MediaType.APPLICATION_JSON_UTF_8)
                )
                .respond(response());
    }
}
