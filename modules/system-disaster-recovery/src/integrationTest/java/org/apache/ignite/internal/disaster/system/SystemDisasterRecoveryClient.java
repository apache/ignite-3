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

package org.apache.ignite.internal.disaster.system;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.rest.api.cluster.ClusterState;
import org.apache.ignite.internal.rest.api.recovery.system.MigrateRequest;
import org.apache.ignite.internal.rest.api.recovery.system.ResetClusterRequest;
import org.jetbrains.annotations.Nullable;

/**
 * Used to run system disaster recovery CLI commands.
 */
class SystemDisasterRecoveryClient {
    private static final IgniteLogger LOG = Loggers.forClass(SystemDisasterRecoveryClient.class);

    static void initiateClusterReset(
            String httpHost,
            int httpPort,
            @Nullable Integer metastorageReplicationFactor,
            String... newCmgNodeNames
    ) {
        LOG.info(
                "Initiating cluster reset via {}:{}, new CMG {}, metastorage replication Factor {}",
                httpHost,
                httpPort,
                List.of(newCmgNodeNames),
                metastorageReplicationFactor
        );

        ResetClusterRequest resetClusterRequest = new ResetClusterRequest(List.of(newCmgNodeNames), metastorageReplicationFactor);

        try {
            sendPostRequest(httpHost, httpPort, "/management/v1/recovery/cluster/reset", resetClusterRequest);
        } catch (IOException e) {
            LOG.info("Node has gone, this most probably means that cluster repair is initiated and the node restarts.");
        }
    }

    static void initiateMigration(String oldHttpHost, int oldHttpPort, String newHttpHost, int newHttpPort) {
        LOG.info("Initiating migration, old {}:{}, new {}:{}", oldHttpHost, oldHttpPort, newHttpHost, newHttpPort);

        try {
            HttpResponse<String> response = sendGetRequest(newHttpHost, newHttpPort, "/management/v1/cluster/state");
            ObjectMapper objectMapper = new ObjectMapper();
            ClusterState clusterState = objectMapper.readValue(response.body(), ClusterState.class);

            MigrateRequest migrateRequest = new MigrateRequest(
                    List.copyOf(clusterState.cmgNodes()),
                    List.copyOf(clusterState.msNodes()),
                    clusterState.igniteVersion(),
                    clusterState.clusterTag().clusterId(),
                    clusterState.clusterTag().clusterName(),
                    clusterState.formerClusterIds()
            );

            sendPostRequest(oldHttpHost, oldHttpPort, "/management/v1/recovery/cluster/migrate", migrateRequest);
        } catch (IOException e) {
            LOG.info("Node has gone, this most probably means that migration is initiated and the node restarts.");
        }
    }

    private static HttpResponse<String> sendGetRequest(String httpHost, int httpPort, String path) throws IOException {
        return sendHttpRequest(httpHost, httpPort, path, Builder::GET);
    }

    private static void sendPostRequest(String httpHost, int httpPort, String path, Object arg) throws IOException {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            BodyPublisher bodyPublisher = BodyPublishers.ofString(objectMapper.writeValueAsString(arg));
            sendHttpRequest(httpHost, httpPort, path, builder -> builder.POST(bodyPublisher));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static HttpResponse<String> sendHttpRequest(
            String httpHost,
            int httpPort,
            String path,
            Consumer<Builder> builderConsumer
    ) throws IOException {
        Builder builder = HttpRequest.newBuilder(URI.create("http://" + httpHost + ":" + httpPort + path))
                .header("content-type", "application/json");
        builderConsumer.accept(builder);
        HttpRequest request = builder.build();

        try {
            HttpClient client = HttpClient.newBuilder().build();
            HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new RuntimeException(response.body());
            }
            return response;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
